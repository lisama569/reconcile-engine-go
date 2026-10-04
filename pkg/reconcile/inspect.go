package reconcile

import (
	"sort"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// ConvergenceView 是单个对象某一时刻的收敛视图（只读快照）。
// 查询本身不阻塞协调、不产生副作用、不改变对象版本。
type ConvergenceView struct {
	Key    object.Key
	Exists bool // 对象是否仍存在

	// 以下字段来自 Status（观测状态）。
	Phase      object.Phase
	ReadyItems map[string]string // 已确认完成的目标项
	// LastFailure 是最近一次失败原因文本（无失败为空）。
	LastFailure string
	// FailureClass 区分最近一次失败的类别：可恢复 / 配置性 / 预算耗尽。
	FailureClass FailureReason
	// Retriable 表示最近一次失败是否可自动恢复。
	Retriable bool

	// 以下字段来自引擎侧退避追踪。
	ConsecutiveFailures int           // 同一原因连续可恢复失败次数
	CurrentBackoff      time.Duration // 当前退避档位（最近一次排定的等待）
	NextRetryAt         time.Time     // 下一次重试排定时间（零值=无挂起重试）
	Abandoned           bool          // 预算耗尽，等待人工介入

	// WaitingForDependents 表示正在等待依赖对象（children）清理完成。
	WaitingForDependents bool

	ResourceVersion    int64
	Generation         int64
	ObservedGeneration int64
}

// Inspect 查询单个对象的收敛视图。
// 只读：不阻塞协调（只取存储读锁与追踪器快照锁）、不写存储、
// 不改变对象版本、不产生副作用。视图中的重试计划与引擎最新
// 调度状态一致 —— 任何取代挂起重试的新写入都会同步清除该计划。
func (e *Engine) Inspect(k object.Key) ConvergenceView {
	v := ConvergenceView{Key: k}
	obj, err := e.store.Get(k)
	if err != nil {
		return v // 对象已不存在
	}
	v.Exists = true
	v.Phase = obj.Status.Phase
	v.ReadyItems = object.CloneMap(obj.Status.ReadyItems)
	v.LastFailure = obj.Status.Reason
	v.Retriable = obj.Status.Retriable
	v.ResourceVersion = obj.ResourceVersion
	v.Generation = obj.Spec.Generation
	v.ObservedGeneration = obj.Status.ObservedGeneration
	v.Abandoned = obj.Status.Phase == object.PhaseAbandoned

	// 最近一次失败的类别（可区分：配置性 / 可恢复 / 预算耗尽）。
	switch {
	case v.Abandoned:
		v.FailureClass = ReasonBudgetExhausted
	case obj.Status.Phase == object.PhaseFailed && obj.Status.Retriable:
		v.FailureClass = ReasonTransient
	case obj.Status.Phase == object.PhaseFailed:
		v.FailureClass = ReasonConfig
	default:
		v.FailureClass = ReasonNone
	}

	// 是否在等依赖对象（children）清理完。
	if obj.Deleting() {
		v.WaitingForDependents = len(e.store.ListChildren(k)) > 0
	}

	// 引擎侧退避状态：连续失败次数、当前退避档位、下一次重试时间。
	if st, ok := e.tracker.snapshot(k); ok {
		v.ConsecutiveFailures = st.failures
		v.CurrentBackoff = st.lastDelay
		v.NextRetryAt = st.nextRetryAt
		if st.abandoned {
			v.Abandoned = true
		}
	}
	return v
}

// InspectAll 返回所有现存对象的收敛视图（按 Key 排序，结果稳定）。
func (e *Engine) InspectAll() []ConvergenceView {
	objs := e.store.List()
	views := make([]ConvergenceView, 0, len(objs))
	for _, o := range objs {
		views = append(views, e.Inspect(o.Key))
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Key < views[j].Key })
	return views
}

// ResetRetry 运维显式重置：清除该对象的放弃状态与退避计数，
// 使其从最小等待重新开始自动收敛。
func (e *Engine) ResetRetry(k object.Key) {
	e.tracker.reset(k)
	e.rec.ClearAbandoned(k)
	e.Trigger(k)
}
