# 期望状态协调引擎（Go）

一套本地运行、零外部依赖（仅 Go 标准库）的期望状态协调引擎，演示如何在
**依赖关系、失败重试与并发变更**下让对象的最终状态可复现、可解释。

- 期望状态 ↔ 实际状态收敛，重复协调无重复副作用，已收敛不反复改写；
- 协调中版本变化以最新版本重新判定，过期快照不能覆盖较新状态；
- 可恢复失败指数退避重试并保留上次成功状态；不可恢复失败终止重试并给出
  可区分的失败原因；单个对象失败不阻塞其他对象；
- **收敛视图**：按对象实时查询阶段、已确认项、失败类别、连续失败次数、
  退避档位与下一次重试时间、是否在等依赖清理 —— 只读、无副作用；
- **重试预算**：每对象可配置的自动重试上限，耗尽后进入可区分的
  `Abandoned`（放弃、待人工介入）状态，只影响自身；
- 终结器（finalizer）级联删除：先回收全部派生对象（children），再摘除
  所有者的清理终结器；删除/更新并发下不留残留；
- 不同对象并行协调，同一对象任意时刻最多一个协调在执行（不交叉）；
- 批量混合负载下终态确定：成百对象混合更新/删除/重试，结果可复现。

## 包结构

```
pkg/
  object/    领域模型：Object / Spec / Status、版本、软删除与终结器方法
  store/     版本化内存存储：Get/List/Create/CAS/CASQuiet/Delete + 变更通知
  reconcile/ 协调器（判定逻辑）、工作队列（去重/延迟）、引擎（并行调度）、
             退避追踪（tracker.go）与收敛视图（inspect.go）
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
| `reconcile.Transient(err)`     | **Transient** | 指数退避 `base·2^(N-1)`（封顶 max），N 为连续失败次数 |
| `reconcile.InvalidConfig(...)` | **InvalidConfig** | 写原因后终止，不再重试/改写        |
| 其他未知错误                   | Transient     | 保守地按可恢复处理                     |

- 退避默认 `10ms` 起步、`200ms` 封顶（无随机抖动，测试结果可复现）；
- 重试时只重放**尚未 Ready** 的项；已成功项绝不重放（重复协调无重复副作用）；
- 若协调处理期间又收到新变更，新变更优先（立即重判），旧的退避调度作废。

### 退避档位与连续失败计数（引擎侧追踪）

引擎为每个对象维护一份内存态退避追踪（`RetryTracker`），规则：

- 第 N 次连续可恢复失败后，等待 `Backoff(base, max, N-1)` ——
  **连续失败次数与退避档位严格一一对应**，逐次变长直到封顶；
- 被重新触发 / 重新排队 / 冲突重判**不会**把等待回退到最小值：
  这些事件只取消"挂起的重试时间"，不清零失败计数与档位；
- 只有三种情况让计数归零、从最小等待重新开始：
  成功收敛、外部提交**新一代 Spec**（`Spec.Generation` 变化）、
  运维显式重置（`Engine.ResetRetry`）；
- 冲突重判与"等待依赖清理"的延迟重排**不计入**失败次数。

### 重试预算与放弃状态

- 每个对象可用 `Spec.RetryBudget` 配置自动重试预算（>0 生效；
  <0 不限；0 表示用引擎默认 `Config.DefaultRetryBudget`，默认亦不限）；
- 预算语义：最多自动重试 `budget` 次；第 `budget+1` 次连续可恢复失败时，
  对象被写入 **`Abandoned`** 状态（`Reason=RetryBudgetExhausted`），
  不再自动重试、不再被改写 —— 这是与配置错误（`Failed/Retriable=false`）
  可明确区分的第三种终态；
- `Abandoned` 针对当前 `Spec.Generation` 粘滞：只影响该对象自身，
  其他对象照常收敛；删除路径不受预算粘滞影响（级联清理仍会执行）；
- 解除方式只有两种：外部提交新一代 Spec，或调用 `Engine.ResetRetry(key)`
  （清除放弃状态与退避计数，从最小等待重新开始）；
- 配置性失败不消耗预算、不经退避，仍然立即终止（行为与之前一致）。

## 收敛视图（Inspect）

`Engine.Inspect(key)` / `Engine.InspectAll()` 返回 `ConvergenceView` 快照：

| 字段                   | 含义                                                   |
| ---------------------- | ------------------------------------------------------ |
| `Phase`                | 当前阶段（Pending/Ready/Failed/Deleting/Abandoned）    |
| `ReadyItems`           | 已确认完成的目标项                                     |
| `FailureClass`         | 最近失败类别：Transient / InvalidConfig / RetryBudgetExhausted |
| `Retriable`/`LastFailure` | 最近失败是否可恢复及原因文本                        |
| `ConsecutiveFailures`  | 同一代配置下连续可恢复失败次数                         |
| `CurrentBackoff`       | 当前退避档位（与失败次数对应）                         |
| `NextRetryAt`          | 下一次重试排定时间（零值=无挂起重试）                  |
| `Abandoned`            | 预算耗尽、等待人工介入                                 |
| `WaitingForDependents` | 删除中且在等依赖对象（children）清理完                 |
| `ResourceVersion`/`Generation`/`ObservedGeneration` | 版本与代际水位              |

视图保证：

- **只读**：不阻塞协调（仅取存储读锁与追踪器快照锁）、不写存储、
  不改变对象版本、不产生副作用；
- **与最新调度一致**：挂起的重试被新写入/新触发取代时，
  `NextRetryAt` 同步清除 —— 视图里不会挂着一条已被取消的重试。

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
- `Store.Notify()` 让外部写入（含并发改写）自动触发对应对象重判；
- 引擎写自身状态走 `Store.CASQuiet`（不发通知）—— 引擎对每次写入
  已有后续调度决定，自我通知只会制造冗余重判；外部写入仍走 `CAS`
  （发通知）。`Store.Delete` 保留通知（用于级联回收后的状态清理）。

## 批量混合负载：终态保证与边界

`TestBatch_MixedLoadDrillDeterministic` 一次灌入 190 个对象（3 个 worker）：

- 50 个普通对象：收敛 Ready，每项副作用恰好 1 次；
- 30 个重试对象：前 N 次可恢复失败后成功，副作用次数 = 失败次数 + 1；
- 25 个 owner × 2 children：删除请求后级联回收 —— 每个 child 的
  Cleanup 恰好下发 1 次、child 零残留、finalizer 全部摘除；
- 25 个对象被并发推进 4 代 Spec：最终 ReadyItems 与最终代期望完全一致；
- 10 个永久失败对象（预算 2）：恰好 3 次尝试后进入 Abandoned。

保证与边界：

- 每个对象都收敛到确定终态（Ready / Abandoned / 删除完成），
  不会因更新与删除交错而卡死或饿死（队列按就绪时间公平取用）；
- 清理动作幂等且不重复下发；同一对象不交叉协调；
- 同一场景连续多轮的**可观测终态摘要**（副作用总次数、各终态对象数、
  残留数）完全一致；对象内部版本号因并发重判时序不同可不保证逐轮相同，
  但已收敛对象的版本不再无谓递增；
- 边界：预算是"连续失败"预算，成功一次即清零；`Abandoned` 对象若被
  请求删除，级联清理仍会执行（删除路径不受预算粘滞阻塞）。

## 对现有调用方的兼容性

- 既有 API（`New/Start/Stop/Trigger/WaitIdle/Inflight`、
  `Reconciler.ReconcileOnce`、`WorkQueue`、`Store` 读写语义）行为不变；
- 未配置预算时（`Spec.RetryBudget=0` 且 `Config.DefaultRetryBudget<=0`）
  重试不限次数，与上一版行为一致；
- 新增均为可选：`Spec.RetryBudget`、`Config.DefaultRetryBudget`、
  `Engine.Inspect/InspectAll/ResetRetry`、`Store.CASQuiet`、
  `object.PhaseAbandoned`、`ReasonBudgetExhausted`；
- `Store.CAS` 语义不变（仍发通知）；引擎内部写入改走 `CASQuiet`，
  只减少冗余自我触发，不影响外部写入路径。

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
| 收敛视图内容与只读性         | `TestInspect_ViewDuringTransientFailure`、`TestInspect_QueryHasNoSideEffects` |
| 重试计划与最新状态一致       | `TestInspect_RetryPlanConsistentWithLatestWrite`               |
| 等待依赖清理可见             | `TestInspect_WaitingForDependents`                             |
| 退避档位随失败次数变长       | `TestRetryTracker_BackoffGrowsWithFailureCount`、`TestBackoff_ViewShowsGrowingBackoffMatchingFailureCount` |
| 重新触发不回退到最短等待     | `TestRetryTracker_RetriggerDoesNotResetBackoff`                |
| 预算耗尽进入 Abandoned       | `TestRetryTracker_BudgetExhaustion`、`TestBackoff_BudgetExhaustedAbandonsAndStops` |
| 新代 Spec / 显式重置解除放弃 | `TestBackoff_AbandonedReleasedByNewGenerationAndReset`、`TestRetryTracker_ExplicitReset` |
| 配置失败不消耗预算、仍终止   | `TestBackoff_ConfigFailureBypassesBudget`                      |
| 批量混合负载终态确定可复现   | `TestBatch_MixedLoadDrillDeterministic`（同场景连跑 3 轮比对摘要） |

所有断言均针对**可观测事实**：副作用调用次数（`CountingApplier`）、
最终 Status/Phase、`ResourceVersion` 是否变化、child 是否全部消失。
