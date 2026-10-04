package reconcile

import (
	"errors"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// ConvergenceView 是某对象在查询时刻的收敛视图快照。
// 查询为纯只读：不阻塞协调、不产生副作用、不改变对象版本。
type ConvergenceView struct {
	Key    object.Key
	Exists bool // 对象是否仍存在（可能已被物理回收）

	// 以下来自 Store 最新版本。
	Phase              object.Phase
	ReadyItems         map[string]string // 已确认完成的目标项
	ResourceVersion    int64
	ObservedGeneration int64
	Deleting           bool

	// 失败分类与重试状态。
	LastFailureKind      FailureReason // 最近一次失败的分类（None 表示无失败）
	LastFailureRetriable bool          // 最近一次失败是否可恢复
	ConsecutiveFailures  int           // 同一原因连续失败次数（当前代配置）
	NextRetryAt          time.Time     // 下一次重试的计划时间（RetryScheduled 为 false 时为零值）
	RetryScheduled       bool          // 是否有一条待执行的重试排在队列里
	GaveUp               bool          // 重试预算耗尽，已放弃、等待人工介入

	// 依赖清理。
	WaitingOnChildren bool // 是否在等依赖（child）对象清理完
}

// View 返回指定对象的收敛视图快照。
//
// 一致性保证：
//   - 状态类字段（Phase/ReadyItems/失败分类）来自 Store 当前最新版本；
//   - NextRetryAt 直接读工作队列的排队项（唯一事实来源），
//     被新写入取消/提前的重试不会残留在视图里；
//   - 连续失败次数来自协调器内部的失败计数器，随成功/换代/重置清零。
//
// 本方法只读：不加写锁、不触发协调、不递增任何版本。
func (e *Engine) View(k object.Key) ConvergenceView {
	v := ConvergenceView{Key: k}
	obj, err := e.store.Get(k)
	if err != nil {
		return v // 对象不存在：Exists=false，其余零值
	}
	v.Exists = true
	v.Phase = obj.Status.Phase
	v.ReadyItems = object.CloneMap(obj.Status.ReadyItems)
	v.ResourceVersion = obj.ResourceVersion
	v.ObservedGeneration = obj.Status.ObservedGeneration
	v.Deleting = obj.Deleting()

	v.LastFailureKind = FailureReason(obj.Status.FailureKind)
	v.LastFailureRetriable = obj.Status.Retriable
	if v.LastFailureKind == ReasonNone {
		// 状态里还没有失败分类时，以计数器里最近一次失败原因为准。
		_, v.LastFailureKind = e.tracker.snapshot(k, obj.Spec.Generation)
	}
	v.ConsecutiveFailures, _ = e.tracker.snapshot(k, obj.Spec.Generation)

	if t, ok := e.queue.ScheduledAt(k); ok {
		v.NextRetryAt = t
		v.RetryScheduled = true
	}
	v.GaveUp = obj.Status.FailureKind == string(ReasonBudgetExhausted) && !obj.Status.Retriable
	if obj.Deleting() {
		v.WaitingOnChildren = len(e.store.ListChildren(k)) > 0
	}
	return v
}

// Reset 是运维显式重置：
//  1. 解除该对象的"已放弃/终结失败"粘滞状态（清掉失败标记，CAS 写回）；
//  2. 清零连续失败计数（退避回到最小等待）；
//  3. 立即重新入队，从最新版本重新判定。
//
// 对其他对象无任何影响。对象不存在时返回 store.ErrNotFound。
func (e *Engine) Reset(k object.Key) error {
	// 解除终结粘滞（配置失败或预算耗尽都可能粘滞，统一解除；
	// 若配置仍然非法，下一轮会重新得出终结结论，不会死循环）。
	for i := 0; i < 3; i++ {
		obj, err := e.store.Get(k)
		if err != nil {
			return err
		}
		terminal := obj.Status.Phase == object.PhaseFailed && !obj.Status.Retriable
		gaveUpDeleting := obj.Deleting() && !obj.Status.Retriable &&
			obj.Status.FailureKind == string(ReasonBudgetExhausted)
		if !terminal && !gaveUpDeleting {
			break
		}
		next := obj.DeepCopy()
		next.Status.Reason = ""
		next.Status.FailureKind = ""
		next.Status.Retriable = true
		if !next.Deleting() {
			next.Status.Phase = object.PhasePending
		}
		if _, err := e.store.CAS(next, obj.ResourceVersion); err != nil {
			if errors.Is(err, store.ErrConflict) {
				continue // 与并发写入撞车：读最新版本重试
			}
			return err
		}
		e.log.Info("reset: terminal failure state cleared",
			"key", k, "basis", "manual reset")
		break
	}
	e.tracker.reset(k)
	e.Trigger(k)
	e.log.Info("reset: failure counter cleared and requeued", "key", k)
	return nil
}
