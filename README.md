# reconcile-engine-go

一个**本地运行**的期望状态协调引擎（reconciliation engine）参考实现。它演示在
依赖关系、失败重试与并发变更同时存在时，如何让对象的最终状态**可复现、可断言、
可解释**。

- 语言：Go（无任何外部依赖，仅标准库）
- 运行演示：`go run ./cmd/reconcile-demo`
- 运行测试：`go test -race -count=10 ./...`

## 1. 对象模型

| 对象 | 角色 | 关键字段 |
| --- | --- | --- |
| `Service` | 属主（Owner） | `spec.replicas` 期望派生副本数；`spec.invalid` 标记不可恢复配置错误 |
| `Endpoint` | 派生对象（Dependent） | `spec.ownerRef` 指向属主；对应一个必须幂等创建/回收的外部资源 |

每个 `Object` 带两个版本号：

- `Generation`：**期望状态**（Spec / finalizer / 删除标记）每变化一次递增；
- `ResourceVersion`（rv）：对象内容（含 Status）每变化一次递增，用于
  **乐观并发控制（CAS）**。

`Status` 中记录：`Phase`（Pending / Applied / Terminal / Deleting）、
`ObservedGen`（状态所反映的期望代数）、`ReadyReplicas` / `Provisioned`
以及 Terminal 的可区分失败原因 `Message`。

## 2. 协调协议（水平触发 + 幂等）

协调器是 **level-triggered** 的：输入只有“Store 中的最新快照 + 外部世界的实际
状态”，不依赖上一次协调发生了什么。对同一输入，判定永远相同，因此：

- **重复协调不产生重复副作用**：外部系统的 `Provision` / `Delete` 都是幂等的，
  命中已存在状态时返回 `created=false`，外部副作用计数器 `Mutations` 不增长；
- **已收敛对象不被反复改写**：`MemoryStore.UpdateSpec/UpdateStatus` 在内容
  无差异时返回空操作（不递增 rv、不计入 `WriteCount`）；协调器在状态已等于
  期望值时直接返回，不发起写入；
- **单轮判定完整可解释**：每次协调都通过 `Logger` 打印
  “输入（kind/name、gen、rv、deleting、finalizers）→ 观测 → 判定依据 → 结果 rv”。
  `go test -v` 与 demo 输出中可直接看到。

### Service 协调

1. 非删除对象确保注册 finalizer；
2. 校验配置：`invalid=true` 或 `replicas<0` → 不可恢复失败（见 §3）；
3. 按稳定命名 `<service>-<index>` 多退少补：
   缺则创建（Store 做孤儿准入），漂移则纠正，多余则 `MarkDeleting`；
4. 统计已确认外部副作用就位的副本数，全部就绪后回写
   `Applied / ObservedGen=当前 Generation`。

### Endpoint 协调

1. 非删除对象确保注册 finalizer；
2. 属主不存在或正在删除 → 不重建，直接进入级联删除（孤儿防护的第二道）；
3. 幂等 `Provision` 外部资源，成功后回写 `Applied`；
4. 删除中：幂等 `Delete` 外部资源 → 移除 finalizer → 物理删除。

## 3. 失败分类与退避重试

所有协调错误都携带 `ErrorClass`，控制器按分类处理，单个对象的失败**绝不阻塞
其他对象**（worker 池跨 key 并行，见 §5）：

| 分类 | 触发 | 调度行为 | 状态处理 |
| --- | --- | --- | --- |
| `Transient`（可恢复） | 外部后端暂时不可用（注入点：创建 / 幂等确认 / 删除） | 指数退避 `base·2^(n-1)`（封顶 `max`，无抖动，**可复现**），成功后计数清零 | **保留上次成功写入的状态**：失败路径不调用任何 Status 写入，Applied 不掉落 |
| `Terminal`（不可恢复） | `spec.invalid=true`、`replicas<0`、`capacity<=0` | 写入带**可区分原因**的 `Phase=Terminal, Message=...` 后**终止重试**，不再入队 | 配置被修复（产生新事件）后自动重新参与协调，Terminal 不是物理终态 |
| `Conflict`（版本冲突） | 写入时 rv 与存储不一致 | **立即**（Delay=0）重新入队，以最新版本重新判断 | 过期快照的内容绝不落地 |

退避时长由 `ExponentialBackoff` 确定性计算；测试用
`5ms → 10ms → 20ms …`，并断言“3 次失败的实际耗时”证明退避真实生效。

## 4. 冲突规则与并发安全

三条硬性规则：

1. **一切写入走 CAS**。`UpdateSpec / UpdateStatus / RemoveFinalizer` 都要求
   携带读取时的 rv；rv 不匹配返回 `ClassConflict`，协调器立即以最新快照重来，
   从不用过期快照覆盖较新状态。
2. **删除标记单调不可逆**。`DeletionMarked` 一旦置位，任何 Spec 更新都不能
   清除（等价于 Kubernetes 的 deletionTimestamp 不可变）。删除与更新竞争时，
   删除意图必胜——旧快照里携带的 `deleting=false` 不能复活对象。
3. **创建准入（孤儿防护）**。Store 在 `Create` 派生对象时校验属主存在且未在
   删除；属主删除中，新建请求直接以冲突拒绝，杜绝“删除进行中又长出新副本”。

此外，属主删除前会双重确认：Store 中无派生对象 **且** 外部世界中无任何
属主命名前缀的资源，防止记录丢失但外部资源泄漏。

## 5. 调度与级联删除语义

### 调度（`Controller` + `dedupQueue`）

- 启动时全量入队，之后订阅 Store 事件（派生对象 Upsert 会同时唤醒其属主）；
- 队列保证：
  - **同 key 去重**：等待中的重复事件合并；处理进行中到达的事件在 `Done`
    后**自动补一轮**，既不重复交叉也不漏事件；
  - **同 key 串行**：一个 key 被 `Get` 取走后、`Done` 之前，任何 worker
    都无法再次取到它——同一对象不被交叉协调；
  - **跨 key 并行**：不同 key 可被不同 worker 同时处理；
- 未完全收敛的对象会按 `resyncDelay` 安全重扫（level-triggered 兜底，
  事件丢失也能最终收敛）；已收敛对象完全安静、零空转（有专门测试断言）。

### 级联删除（属主确认后才回收）

```
用户 MarkDeleting(Service)
        │  （finalizer 阻止物理删除）
        ▼
Service 协调：给所有现存派生对象补打删除标记（删除/更新竞争时删除胜出）
        │
        ▼
Endpoint 协调（逐个，可并行）：
   幂等回收外部资源 ── 可恢复失败则保留 finalizer 退避重试，绝不留垃圾
        │ 回收成功
        ▼
   移除 finalizer（CAS）→ 物理删除
        │
        ▼
Service 协调：Store 无派生对象 且 外部世界无前缀残留
        │
        ▼
   移除自身 finalizer → 物理删除，回收完成
```

顺序不可颠倒：派生对象/外部资源未确认清理前，属主一直 `Deleting` 且物理存在
（测试 `TestCascadeDelete_OwnerWaitsForDependents` 显式断言）。

## 6. 代码结构

```
reconcile/
  types.go        对象/事件/状态类型与深拷贝
  errors.go       Transient / Terminal / Conflict 分类错误
  store.go        带 CAS、finalizer、删除标记、事件订阅、写入计数的内存存储
  external.go     幂等外部副作用模拟（可注入可恢复故障、统计副作用次数）
  backoff.go      确定性指数退避
  workqueue.go    去重 + 延迟 + 按 key 互斥的工作队列
  reconciler.go   Service / Endpoint 的期望→实际协调逻辑
  controller.go   事件订阅、worker 池、退避/终止/冲突调度、指标
cmd/reconcile-demo/main.go  六个端到端场景的可观察演示
```

## 7. 验证方法

### 运行全部单测（建议加 -race 与重复次数）

```bash
go vet ./...
go test -race -count=10 ./...
```

### 测试场景与需求对照

| 测试 | 覆盖的需求 |
| --- | --- |
| `TestReconcile_IdempotentRepeat` | 重复协调：外部副作用次数、各对象 `WriteCount` 收敛后恒定 |
| `TestReconcile_ScaleUpDown` | 多退少补；扩缩容副作用精确（4 创建 + 3 删除 = 7） |
| `TestReconcile_StaleSnapshotConflict` | 过期快照写入被 CAS 拒绝，过期内容（ReadyReplicas=99）不污染新状态 |
| `TestReconcile_ConflictTriggersImmediateRequeue` | 协调器遇冲突返回 `Requeue+Delay0`，以最新版本立即重判 |
| `TestReconcile_TerminalInvalidConfig` | Terminal 原因可区分、不重试、零副作用；配置修复后恢复 Applied |
| `TestController_TransientRetry_PreservesLastGoodStatus` | 指数退避真实生效；故障期间 Applied 状态保留；故障耗尽自动恢复且无新副作用 |
| `TestController_TerminalFailureIsolated` | 坏对象 Terminal 不阻塞好对象收敛；Terminal 不再被反复协调 |
| `TestController_NoChurnAfterConverged` | 已收敛对象在超过 resync 周期后仍零协调、零写入 |
| `TestController_ParallelAcrossKeys` | 观测到跨 key 并行度 > 1 |
| `TestWorkQueue_Dedup` / `...SameKeySerial...` / `...AddAfter` / `...ConcurrentAddNoLoss` | 去重、同 key 串行/处理期事件补轮、延迟取最早、高并发不丢事件 |
| `TestCascadeDelete_OwnerWaitsForDependents` | 属主必须等派生对象与外部资源全部确认清理后才删除 |
| `TestCascadeDelete_RetryThenReap` | 删除外部资源遇可恢复失败：finalizer 保留，退避后回收，无泄漏 |
| `TestCascadeDelete_ConcurrentUpdateDoesNotLeak` | 删除与更新**真实并发**：旧快照不能复活对象，CAS/not-found 安全，无 Store/外部残留，副作用恰好 3 创 + 3 删 |
| `TestCascadeDelete_OrphanAdmission` | 属主删除中新建派生对象被准入拒绝 |
| `TestCascadeDelete_ScaleDownConcurrent` | 存活属主缩容与更新并发，多余副本回收且保留副本完好 |

### 可观察的判定日志

每个测试与 demo 都打印三类信息，满足“输入、观测版本、判定依据”可追溯：

- 输入：`input=Kind/name gen=.. rv=.. deleting=.. finalizers=[..]`
- 依据：如 `判定=状态更新 phase=Pending ready=1/2 gen=1 -> rv=7`、
  `判定=外部资源 created=true`、`判定=删除未完成 store残留=2 外部残留=true`
- 冲突/失败：`判定=... 时遇到版本冲突（持有rv=..）-> 立即以最新版本重新判断`、
  `Transient: ... -> 退避重试`、`Terminal: ... 终止重试`

### 端到端演示

```bash
go run ./cmd/reconcile-demo
```

依次展示：正常收敛与非法配置隔离 → 可恢复故障退避重试且状态保留 →
扩容（只新增必要副作用）→ 缩容级联回收 → 级联删除属主 →
删除与上千笔过期快照更新竞争（全部被 CAS 拒绝、零新增副作用、零残留）。
