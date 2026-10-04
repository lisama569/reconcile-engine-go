package reconcile

import (
	"sync"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// retryState 是单个对象的退避/预算状态（引擎侧内存态，不写入存储）。
type retryState struct {
	failures    int           // 同一 Generation 下连续可恢复失败次数
	generation  int64         // 计数对应的 Spec.Generation
	lastDelay   time.Duration // 最近一次排定的退避等待（当前退避档位）
	nextRetryAt time.Time     // 已排定的下一次重试时间（零值=无挂起重试）
	abandoned   bool          // 预算已耗尽，等待人工介入
}

// RetryTracker 跟踪每个对象的连续失败次数与退避档位，保证：
//   - 连续可恢复失败的等待逐次变长直到封顶；
//   - 被重新触发/重新排队不会回退到最短等待（计数不清零）；
//   - 连续失败次数与退避档位严格对应：第 N 次失败后等待 Backoff(base,max,N-1)；
//   - 只有成功收敛、新一代 Spec 或显式重置才会让计数归零。
//
// 并发安全；所有方法都是纯内存操作，不触碰存储，不产生副作用。
type RetryTracker struct {
	mu     sync.Mutex
	base   time.Duration
	max    time.Duration
	states map[object.Key]*retryState
	now    func() time.Time
}

// newRetryTracker 创建追踪器；base/max 为退避参数，now 可注入测试时钟。
func newRetryTracker(base, max time.Duration, now func() time.Time) *RetryTracker {
	if now == nil {
		now = time.Now
	}
	return &RetryTracker{base: base, max: max, states: map[object.Key]*retryState{}, now: now}
}

// get 返回该键的状态，按需创建。
func (t *RetryTracker) get(k object.Key) *retryState {
	st, ok := t.states[k]
	if !ok {
		st = &retryState{generation: -1}
		t.states[k] = st
	}
	return st
}

// recordFailure 记录一次可恢复失败，返回应排定的退避等待与预算是否耗尽。
//   - generation 与上次不同（外部提交了新一代 Spec）：先归零重新计数；
//   - budget < 0 表示不限次数；budget == 0 表示不允许任何自动重试；
//   - 预算未耗尽时：failures 递增，等待 = Backoff(base, max, failures-1)。
func (t *RetryTracker) recordFailure(k object.Key, generation int64, budget int) (delay time.Duration, exhausted bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(k)
	if st.generation != generation {
		st.failures = 0
		st.lastDelay = 0
		st.abandoned = false
		st.generation = generation
	}
	st.failures++
	if budget >= 0 && st.failures > budget {
		st.abandoned = true
		st.nextRetryAt = time.Time{}
		return 0, true
	}
	delay = Backoff(t.base, t.max, st.failures-1)
	st.lastDelay = delay
	st.nextRetryAt = t.now().Add(delay)
	return delay, false
}

// recordSuccess 记录一次成功收敛：清零连续失败计数与退避档位。
func (t *RetryTracker) recordSuccess(k object.Key, generation int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(k)
	st.failures = 0
	st.lastDelay = 0
	st.nextRetryAt = time.Time{}
	st.abandoned = false
	st.generation = generation
}

// noteSchedule 记录一次非失败性质的延迟排队（如等待依赖清理），
// 只更新下一次重试时间，不影响失败计数与退避档位。
func (t *RetryTracker) noteSchedule(k object.Key, delay time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(k)
	st.nextRetryAt = t.now().Add(delay)
}

// clearSchedule 清除挂起的重试时间（被新触发取代 / 已开始执行 / 已终止）。
// 关键：不清零失败计数与退避档位 —— 重新触发不会回到最短等待。
func (t *RetryTracker) clearSchedule(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.states[k]; ok {
		st.nextRetryAt = time.Time{}
	}
}

// markAbandoned 标记预算耗尽（与 recordFailure 的 exhausted 配套，
// 供引擎在写入 Abandoned 状态后显式确认）。
func (t *RetryTracker) markAbandoned(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.get(k)
	st.abandoned = true
	st.nextRetryAt = time.Time{}
}

// reset 运维显式重置：删除全部退避状态，下次失败从最小等待重新开始。
func (t *RetryTracker) reset(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, k)
}

// remove 对象已消失，清理状态避免泄漏。
func (t *RetryTracker) remove(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, k)
}

// snapshot 读取状态副本（只读，供收敛视图使用）。
func (t *RetryTracker) snapshot(k object.Key) (retryState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.states[k]
	if !ok {
		return retryState{}, false
	}
	return *st, true
}
