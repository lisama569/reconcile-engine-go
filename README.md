# 期望状态协调引擎（Go）

一套本地运行、零外部依赖（仅 Go 标准库）的期望状态协调引擎，演示如何在
**依赖关系、失败重试与并发变更**下让对象的最终状态可复现、可解释。

- 期望状态 ↔ 实际状态收敛，重复协调无重复副作用，已收敛不反复改写；
- 协调中版本变化以最新版本重新判定，过期快照不能覆盖较新状态；
- 可恢复失败指数退避重试并保留上次成功状态；不可恢复失败终止重试并给出
  可区分的失败原因；单个对象失败不阻塞其他对象；
- 每对象重试预算：连续失败逐次变长直到封顶，预算耗尽进入"已放弃、需人工
  介入"状态，新配置或显式重置才解除；
- 运行时收敛视图：按对象只读查询阶段、已完成项、失败分类、连续失败次数、
  下一次重试时间与依赖清理等待，查询无副作用、与最新状态一致；
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
| 连续失败超预算                 | **BudgetExhausted** | 放弃自动重试，等待人工介入       |
| 其他未知错误                   | Transient     | 保守地按可恢复处理                     |

- 退避默认 `10ms` 起步、`200ms` 封顶（无随机抖动，测试结果可复现）；
- 重试时只重放**尚未 Ready** 的项；已成功项绝不重放（重复协调无重复副作用）；
- 若协调处理期间又收到新变更，新变更优先（立即重判），旧的退避调度作废。

### 退避档位与连续失败计数

- 退避档位由**每对象连续失败计数器**（`retryTracker`）驱动，与队列出队次数
  无关：第 n 次连续失败的等待为 `Backoff(base, max, n-1)`，即
  `base, 2·base, 4·base, …` 直到封顶 `max`；
- 中途被 `Trigger`/外部写入**重新触发或重新排队不会回到最短等待**——
  计数器不清零，触发只会立即补一次重判，之后的等待仍按当前档位继续；
- 计数器在以下情况清零：协调成功收敛、外部提交**新一代 Spec**
  （`Spec.Generation` 变化）、运维显式 `Engine.Reset`；
- 引擎自己写状态产生的变更通知会被自写日志（`selfWriteLog`）过滤，
  不会把刚排好的退避等待折叠成立即重判（否则退避会被自写抖动破坏）。

### 重试预算与"已放弃"状态

- 每个对象可通过 `Spec.RetryBudget` 配置预算（允许的最大连续可恢复失败
  次数）；为 0 时使用引擎级 `Config.DefaultRetryBudget`；两者都 <=0 表示
  不限；`Spec.RetryBudget` 为负数表示该对象显式不限；
- 连续失败次数**超过**预算时（如预算 2 → 第 3 次连续失败），对象进入
  **已放弃、需人工介入**状态：`Phase=Failed`、`Retriable=false`、
  `FailureKind=BudgetExhausted`，不再自动重试、不再改写版本；
  删除路径上清理反复失败同样会耗尽预算——此时**保留 finalizer**、
  child 原地保留，等待人工处理；
- 放弃状态是粘滞的：外部系统恢复也不会自动解除。只有两种解除方式：
  1. 外部提交**新一代 Spec**（`Generation` 递增）；
  2. 运维调用 **`Engine.Reset(key)`**：清除失败标记、清零连续失败计数、
     立即重新入队，从最小等待重新开始；
- 放弃只影响该对象自身，其他对象照常收敛/重试；
- 配置性失败（`InvalidConfig`）不消耗预算：第一次发生即终止，
  `FailureKind=InvalidConfig`，与预算耗尽在视图中明确可区分。

## 运行时收敛视图

`Engine.View(key)` 返回某对象当前时刻的 `ConvergenceView` 快照：

| 字段                    | 含义                                                   |
| ----------------------- | ------------------------------------------------------ |
| `Exists`                | 对象是否仍存在（可能已被物理回收）                     |
| `Phase` / `ReadyItems`  | 当前阶段 / 已确认完成的目标项                          |
| `LastFailureKind`       | 最近失败分类：Transient / InvalidConfig / BudgetExhausted |
| `LastFailureRetriable`  | 最近失败是否可恢复                                     |
| `ConsecutiveFailures`   | 同一原因连续失败次数（当前代配置）                     |
| `NextRetryAt` / `RetryScheduled` | 下一次重试的计划时间 / 是否有重试在排队        |
| `GaveUp`                | 预算耗尽、已放弃、需人工介入                           |
| `Deleting` / `WaitingOnChildren` | 是否删除中 / 是否在等依赖 child 清理完         |

一致性与无副作用保证：

- 查询为**纯只读**：不阻塞协调、不产生副作用、不递增任何版本；
- `NextRetryAt` 直接读工作队列的排队项（唯一事实来源）：被新写入取消或
  提前的重试**不会**残留在视图里；
- 状态字段来自 Store 当前最新版本，与协调器判定所用的是同一份数据。

`Engine.Reset(key)` 是配套的运维操作：解除"已放弃/终结失败"粘滞、清零
退避档位并立即重新入队；对其他对象无影响。

## 批量混合负载下的最终态保证

`TestBatch_MixedLoadDeterministic` 演练 280 个对象的混合负载
（普通收敛 / 持续被更新 / 反复重试后恢复 / 永久失败超预算 / 配置非法 /
级联删除，worker 仅 3 个），保证：

- **每个对象都收敛到确定结果**：Ready、BudgetExhausted（放弃）、
  InvalidConfig（终止）、Deleting（finalizer 已摘除）之一，无对象卡死
  或永远排不上队（队列按 readyAt 公平取用）；
- **该回收的依赖对象一个不剩**：所有 child 物理删除，每个 child 的
  Cleanup 副作用恰好下发一次；
- **更新与删除交错不死锁**：持续心跳更新只推迟、不阻止最终回收；
- **收敛后系统静止**：不再有无谓的版本递增或副作用调用；
- **结果可复现**：同一场景连跑两遍，逐对象终态与每次副作用调用的
  指纹完全一致（配合 `-count=N` 可反复验证）。

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

## 兼容范围

本次扩展对现有调用方完全向后兼容：

- 不设置 `Config.DefaultRetryBudget` 且对象不带 `Spec.RetryBudget` 时，
  重试**不限次数**（预算 <=0 的默认语义），原有失败分类、终结粘滞、
  级联删除行为不变；
- `Spec.RetryBudget` / `Status.FailureKind` 为新增字段，零值语义与旧行为
  一致（不限预算 / 无失败分类）；
- `Engine.View` / `Engine.Reset` 为新增只读/运维 API，不影响既有
  `Trigger`/`Start`/`Stop`/`WaitIdle` 调用路径；
- 唯一的行为收紧：引擎自写不再触发额外重判（消除了自写抖动引起的
  多余协调轮次），对外部写入的自动重判逻辑不变。

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
| 收敛视图：已收敛/失败中/删除中 | `TestView_ConvergedObject`、`TestView_FailingObjectShowsRetryState`、`TestView_DeletingWaitsOnChildren` |
| 视图查询无副作用             | `TestView_QueryHasNoSideEffects`（并发查询不改版本、零副作用） |
| 视图重试计划与最新状态一致   | `TestView_RetryPlanConsistentAfterNewWrite`（新写入取消旧重试） |
| 退避逐次变长直到封顶         | `TestRetry_DelayEscalatesUntilCapped`（相邻重试间隔严格变长、触发不重置） |
| 预算耗尽放弃、人工重置解除   | `TestRetryBudget_GiveUpNeedsManualIntervention`                |
| 新配置解除放弃态             | `TestRetryBudget_NewGenerationReleases`                        |
| 配置失败不消耗预算、仍即停   | `TestRetryBudget_ConfigFailureStillTerminalImmediately`        |
| 删除路径预算耗尽与重置       | `TestRetryBudget_DeletePathGiveUpAndReset`                     |
| 批量混合负载终态确定、可复现 | `TestBatch_MixedLoadDeterministic`（280 对象、两次运行指纹一致） |

所有断言均针对**可观测事实**：副作用调用次数（`CountingApplier`）、
相邻两次重试的真实间隔、最终 Status/Phase、`ResourceVersion` 是否变化、
child 是否全部消失、两次批量运行的指纹是否一致。
