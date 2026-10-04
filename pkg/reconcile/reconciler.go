// Package reconcile 实现期望状态协调引擎：
// 版本化判定、失败分类退避、终结器级联删除与每键串行调度。
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// Clock 抽象时间，便于测试中加速退避。
type Clock interface {
	Now() time.Time
}

// SystemClock 是生产用时钟。
type SystemClock struct{}

// Now 返回当前时间。
func (SystemClock) Now() time.Time { return time.Now() }

// 默认退避参数。
const (
	DefaultBaseDelay = 10 * time.Millisecond
	DefaultMaxDelay  = 200 * time.Millisecond
)

// Reconciler 封装单个对象一次协调所需的依赖与纯判定逻辑。
type Reconciler struct {
	Store     *store.Store
	Apply     Applier
	Clock     Clock
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Log       *slog.Logger
}

// Decision 是一次协调的判定结果。
type Decision struct {
	Key      object.Key
	Requeue  bool          // 是否需要再次入队
	Delay    time.Duration // 再次入队的延迟（退避；0 表示立即）
	Terminal bool          // 是否为不可恢复失败（终止重试）
	Reason   FailureReason
	Message  string
	// Failure 表示本轮发生了真实的可恢复失败（计入连续失败与重试预算）；
	// 冲突重判、等待依赖清理等"非失败"重排队不计入。
	Failure bool
	// Converged 表示本轮已收敛到确定结果（成功/清理完成），
	// 引擎据此清零连续失败计数与退避档位。
	Converged bool
	// Gone 表示对象已不存在。
	Gone bool
	// Generation 是本轮判定所依据的 Spec.Generation（引擎据此检测
	// "外部提交了新一代配置"，从而重置退避状态）。
	Generation int64
}

// 状态机三个终向：重排队（延迟）、终止、本轮结束（无需再排队）。
func retryDecision(key object.Key, r FailureReason, msg string, attempt int, base, max time.Duration) Decision {
	return Decision{
		Key:     key,
		Requeue: true,
		Delay:   Backoff(base, max, attempt),
		Reason:  r,
		Message: msg,
		Failure: r == ReasonTransient,
	}
}

func terminalDecision(key object.Key, msg string) Decision {
	return Decision{Key: key, Terminal: true, Reason: ReasonConfig, Message: msg}
}

func doneDecision(key object.Key) Decision {
	return Decision{Key: key, Converged: true}
}

func goneDecision(key object.Key) Decision {
	return Decision{Key: key, Gone: true}
}

// ReconcileOnce 读取最新版本并执行一轮协调。
func (r *Reconciler) ReconcileOnce(ctx context.Context, key object.Key, attempt int) Decision {
	// 1) 永远以 Store 中的最新版本为输入，不使用调用方可能持有的过期快照。
	obj, err := r.Store.Get(key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// 对象已不存在（可能已被物理回收）：无需任何动作。
			r.Log.Info("reconcile: object gone, nothing to do", "key", key, "attempt", attempt)
			return goneDecision(key)
		}
		// 存储读失败视为可恢复，立即重试。
		return retryDecision(key, ReasonTransient, err.Error(), 0, r.BaseDelay, r.MaxDelay)
	}

	r.Log.Info("reconcile: input loaded",
		"key", key,
		"observedRV", obj.ResourceVersion,
		"deleting", obj.Deleting(),
		"finalizers", obj.Finalizers,
		"desired", sortedKeys(obj.Spec.DesiredItems),
		"ready", sortedKeys(obj.Status.ReadyItems),
		"attempt", attempt,
	)

	var d Decision
	if obj.Deleting() {
		d = r.reconcileDelete(ctx, obj, attempt)
	} else {
		d = r.reconcileUpsert(ctx, obj, attempt)
	}
	// 标注本轮判定所依据的期望配置代际，供引擎检测"新一代 Spec"。
	d.Generation = obj.Spec.Generation
	return d
}

// reconcileUpsert 处理存活路径：
//
//	a. 确保 finalizer 在位（删除被请求时才会走清理路径）；
//	b. 校验配置（非法 => 不可恢复，终止重试）；
//	c. 对每个期望项做幂等 Apply：已 Ready 的跳过，保证重复协调
//	   不产生重复副作用；
//	d. 所有期望项 Ready 且无待写变更 => 不再写状态（已收敛不反复改写）；
//	e. 任一 Apply 可恢复失败 => 保留上次成功的 ReadyItems，
//	   写入 Failed/Retriable 状态并退避重试。
func (r *Reconciler) reconcileUpsert(ctx context.Context, obj *object.Object, attempt int) Decision {
	key := obj.Key
	rv := obj.ResourceVersion
	now := r.Clock.Now().UTC().Format(time.RFC3339Nano)

	// --- a. finalizer 保障（只在缺失时写一次，避免无谓版本递增）---
	if !obj.HasFinalizer(object.FinalizerOwnerCleanup) {
		next := obj.DeepCopy()
		next.AddFinalizer(object.FinalizerOwnerCleanup)
		stored, err := r.Store.CASQuiet(next, rv)
		if err != nil {
			return r.onCASConflict(key, rv, err, attempt, "add finalizer")
		}
		r.Log.Info("reconcile: finalizer ensured",
			"key", key, "observedRV", rv, "newRV", stored.ResourceVersion, "basis", "finalizer missing")
		// 版本已变（自己刚写的），本轮剩余判定基于新版本继续。
		obj = stored
		rv = stored.ResourceVersion
	}

	// --- b. 配置校验：不可恢复失败 ---
	// 预算耗尽粘滞：已放弃的对象不再自动重试、不再改写状态，
	// 直到外部提交新一代 Spec 或运维显式重置（ClearAbandoned）。
	if obj.Status.Phase == object.PhaseAbandoned &&
		obj.Status.ObservedGeneration == obj.Spec.Generation {
		r.Log.Info("reconcile: abandoned, waiting for manual intervention",
			"key", key, "observedRV", rv, "generation", obj.Spec.Generation,
			"basis", "retry budget exhausted for current generation")
		return Decision{Key: key, Reason: ReasonBudgetExhausted, Message: obj.Status.Reason}
	}
	// 终结失败粘滞：针对同一代 Spec 已下过不可恢复结论时，
	// 不再重新下发副作用、不再改写状态；直到外部提交新一代 Spec。
	if obj.Status.Phase == object.PhaseFailed && !obj.Status.Retriable &&
		obj.Status.ObservedGeneration == obj.Spec.Generation && obj.Status.Reason != "" {
		r.Log.Info("reconcile: terminal failure sticky, skip until spec changes",
			"key", key, "observedRV", rv, "generation", obj.Spec.Generation,
			"basis", "non-retriable failure for current generation")
		return terminalDecision(key, obj.Status.Reason)
	}
	if obj.Spec.FailConfigured {
		msg := "desired spec is invalid: FailConfigured=true"
		// 失败状态同样走 CAS；冲突则下一轮以最新版本重判（也许配置已被修正）。
		if d := r.writeFailure(obj, rv, ReasonConfig, msg, false, attempt, now); d != nil {
			return *d
		}
		r.Log.Info("reconcile: terminal failure recorded",
			"key", key, "observedRV", rv, "basis", msg)
		return terminalDecision(key, msg)
	}

	// --- c. 幂等收敛：只 Apply 尚未 Ready 的期望项 ---
	ready := object.CloneMap(obj.Status.ReadyItems)
	if ready == nil {
		ready = map[string]string{}
	}
	var firstErr error
	var firstReason FailureReason
	for _, token := range sortedKeys(obj.Spec.DesiredItems) {
		if _, ok := ready[token]; ok {
			// 已在上次成功协调中确认存在：跳过，不重复下发副作用。
			continue
		}
		req := ApplyRequest{
			Object: key,
			Kind:   object.EffectProvision,
			Token:  token,
			Detail: obj.Spec.DesiredItems[token],
		}
		r.Log.Info("reconcile: applying side-effect",
			"key", key, "observedRV", rv, "token", token, "basis", "desired but not ready")
		if err := r.Apply.Apply(ctx, req); err != nil {
			reason, _ := Classify(err)
			r.Log.Info("reconcile: side-effect failed",
				"key", key, "observedRV", rv, "token", token,
				"reason", reason, "err", err.Error())
			if reason == ReasonConfig {
				// 单个配置错误立即终止该对象；保留此前已成功项。
				msg := fmt.Sprintf("item %q invalid config: %v", token, err)
				if d := r.writeFailure(obj, rv, ReasonConfig, msg, false, attempt, now); d != nil {
					return *d
				}
				return terminalDecision(key, msg)
			}
			// 可恢复：记录第一个失败即可，保留已成功项，退避重试。
			if firstErr == nil {
				firstErr = err
				firstReason = reason
			}
			continue
		}
		ready[token] = obj.Spec.DesiredItems[token]
	}

	// --- d/e. 写回观测状态 ---
	if firstErr != nil {
		msg := firstErr.Error()
		d := r.writeProgress(obj, rv, ready, object.PhaseFailed, true, firstReason, msg, attempt, now,
			"retryable failure; keeping last-ready items")
		if d != nil {
			return *d
		}
		return retryDecision(key, firstReason, msg, attempt, r.BaseDelay, r.MaxDelay)
	}

	// 全部成功：仅当"内容确实变化"或"阶段不是 Ready"时才写入，
	// 杜绝已收敛对象被反复改写、版本被反复递增。
	if obj.Status.Phase == object.PhaseReady && equalMaps(obj.Status.ReadyItems, ready) && obj.Status.Reason == "" {
		r.Log.Info("reconcile: already converged, no write",
			"key", key, "observedRV", rv, "basis", "status==ready and readyItems unchanged")
		return doneDecision(key)
	}

	d := r.writeProgress(obj, rv, ready, object.PhaseReady, false, ReasonNone, "", attempt, now,
		"all desired items ready")
	if d != nil {
		return *d
	}
	return doneDecision(key)
}

// reconcileDelete 处理删除路径（级联语义）：
//  1. 列出所有 child（Store 中 Owner==本对象）；
//  2. 对每个 child：下发 Cleanup 副作用（幂等），随后物理删除 child；
//     child 删除走 CAS，若删除/更新并发造成版本不符，则下轮重判，
//     绝不基于过期版本把 child 留在系统里成为无法回收的残留；
//  3. 仍有 child 存在 => 保留 finalizer，退避/立即重排队继续回收；
//  4. child 全部回收 => 摘除 finalizer（owner 确认清理完成的标志），
//     此后对象可被外部物理删除。
func (r *Reconciler) reconcileDelete(ctx context.Context, obj *object.Object, attempt int) Decision {
	key := obj.Key
	rv := obj.ResourceVersion

	children := r.Store.ListChildren(key)
	r.Log.Info("reconcile: delete path",
		"key", key, "observedRV", rv, "children", len(children),
		"basis", "deletionTimestamp set")

	blocked := false
	cleanupFailed := false
	for _, child := range children {
		crv := child.ResourceVersion
		// child 若也处于删除中，仍需确保其外部资源被回收。
		req := ApplyRequest{
			Object: child.Key,
			Kind:   object.EffectCleanup,
			Token:  "owned-by-" + string(key),
			Detail: "cascade cleanup",
		}
		r.Log.Info("reconcile: cascade cleanup child",
			"key", key, "child", child.Key, "childRV", crv, "basis", "owner deleting")
		if err := r.Apply.Apply(ctx, req); err != nil {
			reason, _ := Classify(err)
			if reason == ReasonConfig {
				// 清理动作的"配置错误"在此不可发生（参数由引擎构造）；
				// 保守地按可恢复处理，避免永久残留。
				reason = ReasonTransient
			}
			r.Log.Info("reconcile: child cleanup failed, retry",
				"key", key, "child", child.Key, "reason", reason, "err", err.Error())
			blocked = true
			cleanupFailed = true
			continue
		}
		if err := r.Store.Delete(child.Key, crv); err != nil {
			// 与更新并发：child 刚被改过（新版本），本轮不强行删除，
			// 下一轮拿到最新版本重新回收，避免漏删/误删。
			if errors.Is(err, store.ErrConflict) {
				r.Log.Info("reconcile: child delete conflict, will rejudge",
					"key", key, "child", child.Key, "staleRV", crv)
				blocked = true
				continue
			}
			blocked = true
			continue
		}
		r.Log.Info("reconcile: child reclaimed",
			"key", key, "child", child.Key, "childRV", crv)
	}

	if blocked || len(r.Store.ListChildren(key)) > 0 {
		// 标记删除中阶段（CAS），失败则立即重判。
		next := obj.DeepCopy()
		next.Status.Phase = object.PhaseDeleting
		next.Status.Reason = "waiting for children cleanup"
		next.Status.Retriable = true
		if _, err := r.Store.CASQuiet(next, rv); err != nil {
			return r.onCASConflict(key, rv, err, attempt, "mark deleting")
		}
		d := retryDecision(key, ReasonTransient, "children remain", attempt, r.BaseDelay, r.MaxDelay)
		// 仅"清理副作用失败"计入连续失败与重试预算；
		// 单纯等待依赖清理属于非失败性质的延迟重排。
		d.Failure = cleanupFailed
		return d
	}

	// 所有 child 已回收：摘除 finalizer。
	if !obj.HasFinalizer(object.FinalizerOwnerCleanup) {
		r.Log.Info("reconcile: children empty and finalizer already gone",
			"key", key, "observedRV", rv, "basis", "cleanup confirmed")
		return doneDecision(key)
	}
	next := obj.DeepCopy()
	next.RemoveFinalizer(object.FinalizerOwnerCleanup)
	next.Status.Phase = object.PhaseDeleting
	next.Status.Reason = ""
	next.Status.Retriable = false
	stored, err := r.Store.CASQuiet(next, rv)
	if err != nil {
		return r.onCASConflict(key, rv, err, attempt, "remove finalizer")
	}
	r.Log.Info("reconcile: finalizer removed after cleanup confirmed",
		"key", key, "observedRV", rv, "newRV", stored.ResourceVersion,
		"basis", "all children reclaimed")
	return doneDecision(key)
}

// writeFailure 写入失败状态；返回非 nil Decision 表示调用方应直接返回
// （发生 CAS 冲突等情况）。
func (r *Reconciler) writeFailure(obj *object.Object, rv int64, reason FailureReason,
	msg string, retriable bool, attempt int, now string) *Decision {
	next := obj.DeepCopy()
	next.Status.Phase = object.PhaseFailed
	next.Status.Reason = msg
	next.Status.Retriable = retriable
	next.Status.Attempts = attempt + 1
	next.Status.UpdatedAt = now
	next.Status.ObservedGeneration = obj.Spec.Generation
	// 关键：不清空 ReadyItems，保留上次成功写入的状态。
	stored, err := r.Store.CASQuiet(next, rv)
	if err != nil {
		d := r.onCASConflict(obj.Key, rv, err, attempt, "write failure status")
		return &d
	}
	r.Log.Info("reconcile: status written",
		"key", obj.Key, "observedRV", rv, "newRV", stored.ResourceVersion,
		"phase", next.Status.Phase, "reason", reason, "retainedReady", sortedKeys(next.Status.ReadyItems))
	return nil
}

// writeProgress 写回进行中/成功状态。
func (r *Reconciler) writeProgress(obj *object.Object, rv int64, ready map[string]string,
	phase object.Phase, retriable bool, reason FailureReason, msg string, attempt int,
	now, basis string) *Decision {
	next := obj.DeepCopy()
	next.Status.Phase = phase
	next.Status.ReadyItems = ready
	next.Status.Reason = msg
	next.Status.Retriable = retriable
	next.Status.Attempts = attempt + 1
	next.Status.UpdatedAt = now
	next.Status.ObservedGeneration = obj.Spec.Generation
	stored, err := r.Store.CASQuiet(next, rv)
	if err != nil {
		d := r.onCASConflict(obj.Key, rv, err, attempt, "write progress")
		return &d
	}
	r.Log.Info("reconcile: status written",
		"key", obj.Key, "observedRV", rv, "newRV", stored.ResourceVersion,
		"phase", phase, "ready", sortedKeys(ready), "basis", basis)
	return nil
}

// onCASConflict 统一处理乐观锁冲突：本轮绝不覆盖，立即以最新版本重判。
func (r *Reconciler) onCASConflict(key object.Key, staleRV int64, err error, attempt int, op string) Decision {
	if errors.Is(err, store.ErrConflict) {
		r.Log.Info("reconcile: CAS conflict, requeue immediately with latest version",
			"key", key, "staleRV", staleRV, "op", op,
			"basis", "resourceVersion changed during reconcile")
		return Decision{Key: key, Requeue: true, Delay: 0, Reason: ReasonConflict,
			Message: "conflict while " + op}
	}
	r.Log.Info("reconcile: store write error, retry",
		"key", key, "op", op, "err", err.Error())
	return retryDecision(key, ReasonTransient, err.Error(), attempt, r.BaseDelay, r.MaxDelay)
}

// MarkAbandoned 把对象写入"已放弃、需要人工介入"状态（重试预算耗尽）。
// 幂等；CAS 冲突时放弃本次写入 —— 冲突产生的新版本会触发重判，
// 届时预算仍然耗尽，会再次尝试写入。
func (r *Reconciler) MarkAbandoned(key object.Key, msg string) {
	obj, err := r.Store.Get(key)
	if err != nil {
		return
	}
	if obj.Status.Phase == object.PhaseAbandoned &&
		obj.Status.ObservedGeneration == obj.Spec.Generation {
		return // 已是放弃状态
	}
	next := obj.DeepCopy()
	next.Status.Phase = object.PhaseAbandoned
	next.Status.Reason = msg
	next.Status.Retriable = false
	next.Status.UpdatedAt = r.Clock.Now().UTC().Format(time.RFC3339Nano)
	next.Status.ObservedGeneration = obj.Spec.Generation
	stored, err := r.Store.CASQuiet(next, obj.ResourceVersion)
	if err != nil {
		r.Log.Info("reconcile: mark abandoned conflict, next judgement will retry",
			"key", key, "staleRV", obj.ResourceVersion)
		return
	}
	r.Log.Info("reconcile: retry budget exhausted, abandoned",
		"key", key, "observedRV", obj.ResourceVersion, "newRV", stored.ResourceVersion,
		"basis", msg)
}

// ClearAbandoned 运维显式重置时清除 Abandoned 状态（若仍处于该状态），
// 使对象可以重新从最小退避等待开始自动收敛。
func (r *Reconciler) ClearAbandoned(key object.Key) {
	obj, err := r.Store.Get(key)
	if err != nil {
		return
	}
	if obj.Status.Phase != object.PhaseAbandoned {
		return
	}
	next := obj.DeepCopy()
	next.Status.Phase = object.PhasePending
	next.Status.Reason = ""
	next.Status.Retriable = false
	next.Status.UpdatedAt = r.Clock.Now().UTC().Format(time.RFC3339Nano)
	stored, err := r.Store.CASQuiet(next, obj.ResourceVersion)
	if err != nil {
		r.Log.Info("reconcile: clear abandoned conflict, caller should retry reset",
			"key", key, "staleRV", obj.ResourceVersion)
		return
	}
	r.Log.Info("reconcile: abandoned state cleared by operator reset",
		"key", key, "observedRV", obj.ResourceVersion, "newRV", stored.ResourceVersion,
		"basis", "explicit ResetRetry")
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
