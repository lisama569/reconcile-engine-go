# 期望状态协调引擎（Go）

一套本地运行、零外部依赖（仅 Go 标准库）的期望状态协调引擎，演示如何在
**依赖关系、失败重试与并发变更**下让对象的最终状态可复现、可解释。

- 期望状态 ↔ 实际状态收敛，重复协调无重复副作用，已收敛不反复改写；
- 协调中版本变化以最新版本重新判定，过期快照不能覆盖较新状态；
- 可恢复失败指数退避重试并保留上次成功状态；不可恢复失败终止重试并给出
  可区分的失败原因；单个对象失败不阻塞其他对象；
- 终结器（finalizer）级联删除：先回收全部派生对象（children），再摘除
  所有者的清理终结器；删除/更新并发下不留残留；
- 不同对象并行协调，同一对象任意时刻最多一个协调在执行（不交叉）。

## 包结构

```
pkg/
  object/    领域模型：Object / Spec / Status、版本、软删除与终结器方法
  store/     版本化内存存储：Get/List/Create/CAS/Delete + 变更通知
  reconcile/ 协调器（判定逻辑）、工作队列（去重/延迟）、引擎（并行调度）
```

## 快速开始

```go
st := store.New(&object.Object{
    Key:  "svc",
    Spec: object.Spec{Generation: 1, DesiredItems: map[string]string{"bucket": "b1"}},
})
applier := reconcile.ApplierFunc(func(ctx context.Context, req reconcile.ApplyRequest) error {
    // 对接真实外部系统；按 req.Token 做幂等。
    return nil
})
eng := reconcile.New(reconcile.Config{Store: st, Applier: applier, Workers: 4})
eng.Start(context.Background())
defer eng.Stop()

// 外部变更可主动触发；Store 的每次写入也会通过 Notify() 自动触发。
eng.Trigger("svc")
```

删除一个 owner：对其设置 `DeletionTimestamp`（软删除）并 Trigger，
引擎会自动走完级联回收并摘除 `FinalizerOwnerCleanup`。

## 协调协议（ReconcileOnce）

每轮协调从**存储中重新读取最新版本**开始，不信任任何历史快照：

1. **读最新**：`Store.Get(key)`；对象已不存在则直接结束（无可作为）。
2. **确保终结器**：存活对象若缺少 `owner-cleanup` finalizer，先 CAS 写入。
3. **终结失败粘滞**：若当前 `Spec.Generation` 已有不可恢复失败结论
   （`Status.Phase=Failed && Retriable=false && ObservedGeneration==Spec.Generation`），
   不再尝试、不再改写；只有外部提交**新一代 Spec**才解除粘滞重新判定。
4. **配置校验**：`Spec.FailConfigured` 或副作用返回 `InvalidConfig`
   → 写 `Failed/Retriable=false` 与原因，**终止该对象重试**（其他对象不受影响）。
5. **幂等收敛**：对每个期望项，仅当它不在 `Status.ReadyItems` 中时才下发
   `Provision`；成功的项累积进 ReadyItems。
6. **失败保留状态**：某项返回可恢复错误时，**不清空**已成功的 ReadyItems，
   写 `Failed/Retriable=true` 与原因，并按指数退避重新入队。
7. **已收敛不写**：`Phase=Ready` 且 ReadyItems 与期望一致且无原因时，
   本轮零写入零调用直接结束，版本不再递增。
8. **删除路径**：见下节。

### 日志可解释性

每轮协调用结构化日志（`log/slog`）记录：**输入**（key、desired/ready、
是否删除、finalizer）、**观测到的版本**（`observedRV/newRV/staleRV`）、
以及**判定依据**（`basis=`，如 "desired but not ready"、
"status==ready and readyItems unchanged"、"all children reclaimed"）。
测试中日志桥接到 `t.Log`，`go test -v` 即可逐条核对。

## 重试规则

| 错误来源                       | 分类          | 行为                                   |
| ------------------------------ | ------------- | -------------------------------------- |
| 无错误                         | None          | 结束/写 Ready                          |
| `store.ErrConflict`            | **Conflict**  | 延迟 0，立即以最新版本重新判定         |
| `reconcile.Transient(err)`     | **Transient** | 指数退避 `base·2^attempt`（封顶 max）  |
| `reconcile.InvalidConfig(...)` | **InvalidConfig** | 写原因后终止，不再重试/改写        |
| 其他未知错误                   | Transient     | 保守地按可恢复处理                     |

- 退避默认 `10ms` 起步、`200ms` 封顶（无随机抖动，测试结果可复现）；
- 重试时只重放**尚未 Ready** 的项；已成功项绝不重放（重复协调无重复副作用）；
- 若协调处理期间又收到新变更，新变更优先（立即重判），旧的退避调度作废。

## 冲突规则（乐观并发控制）

- 每个 `Object` 带 `ResourceVersion`；存储的每次写入自动 +1。
- 所有状态写入与 child 删除都走 `CAS(obj, observedRV)`：
  版本不符返回 `ErrConflict`，存储内容不变 —— **过期快照永远写不进去**。
- 冲突后协调器本轮放弃写入，以 Conflict 原因**立即重排队**，下一轮读取
  最新版本重新判断，而不是重试旧写入。
- 外部修复配置后需递增 `Spec.Generation`；引擎据此区分"同一配置的失败
  结论仍有效"与"配置已更新，需要重新判定"。

## 级联删除语义

owner 对象持有 finalizer `owner-cleanup`。当其 `DeletionTimestamp` 非空：

1. 列出全部 child（`Owner == owner key`）；
2. 对每个 child 下发幂等 `Cleanup` 副作用；成功后以 **CAS** 物理删除 child；
3. 只要还有 child（含因删除/更新并发导致 CAS 失利、Cleanup 可恢复失败），
   **保留 finalizer**，按退避重新回收；
4. 确认全部 child 消失后，才摘除 owner 的 finalizer —— 这是"所有者已确认
   清理完成"的唯一标志，杜绝 child 成为无法回收的残留对象。

并发要点：删除与更新竞争时，child 删除若撞版本则本轮跳过，下一轮拿新版本
继续，所以持续的更新竞争只会推迟、不会阻止最终回收。

## 调度语义

- `WorkQueue` 对每个 Key 去重：排队/处理中的重复触发折叠为一次，
  保证同一对象不交叉协调；`Done` 时若处理期间有新触发则补排一轮；
- 引擎多 worker 并行处理**不同**对象；单对象失败（尤其终止失败）
  只影响自身队列，不影响其他 worker 上的对象；
- `Store.Notify()` 让外部写入（含并发改写）自动触发对应对象重判。

## 验证方法

```bash
go test ./...                 # 全部单测
go test ./... -race           # 竞态检测
go test ./... -race -count=10 # 稳定性/可复现性
go test ./pkg/reconcile/ -v   # 查看每轮输入/观测版本/判定依据日志
```

### 测试覆盖矩阵

| 需求                         | 测试函数                                                       |
| ---------------------------- | -------------------------------------------------------------- |
| 重复协调幂等、无重复副作用   | `TestReconcile_IdempotentNoDuplicateSideEffects`               |
| 已收敛不反复改写（版本稳定） | 同上（重复 Trigger 后断言 RV 与调用次数不变）                  |
| 可恢复失败退避重试           | `TestReconcile_RetryableFailureBackoffKeepsLastGoodState`      |
| 失败时保留上次成功状态       | 同上（中间态断言 bucket 仍在 ReadyItems）                      |
| 不可恢复失败终止、原因可区分 | `TestReconcile_InvalidConfigTerminalAndIsolated`、`..._SpecFlagInvalidConfig` |
| 单对象失败不阻塞他人         | `TestReconcile_InvalidConfigTerminalAndIsolated`（good-svc）   |
| 协调中版本变化→最新重判      | `TestConflict_RejudgeWithLatestVersion`                        |
| 过期快照不能覆盖新状态       | `pkg/store` 的 `TestCASConflictOnStaleVersion`、级联竞争测试   |
| 级联先回收 child 再摘 finalizer | `TestCascadeDelete_ChildrenReclaimedBeforeFinalizerRemoved` |
| 清理失败退避、finalizer 保留 | `TestCascadeDelete_CleanupRetriableKeepsFinalizer`             |
| 删除/更新并发不留残留        | `TestCascadeDelete_RacesWithConcurrentUpdates_NoOrphans`       |
| 同键串行、异键并行           | `TestWorkQueueNoCrossConcurrency`、`TestParallel_DifferentKeysConcurrentSameKeySerial` |
| 新一代 Spec 解除终结粘滞     | `TestTerminalFailure_RecoversAfterNewGeneration`               |

所有断言均针对**可观测事实**：副作用调用次数（`CountingApplier`）、
最终 Status/Phase、`ResourceVersion` 是否变化、child 是否全部消失。
