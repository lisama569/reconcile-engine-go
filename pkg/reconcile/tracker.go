package reconcile

import (
	"sync"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// failureRecord 记录某对象当前代配置下的连续失败状态。
type failureRecord struct {
	count      int           // 连续失败次数（同一原因、同一代配置）
	reason     FailureReason // 最近一次失败分类
	generation int64         // 该记录针对的 Spec.Generation
}

// retryTracker 跟踪每个对象的连续失败次数与退避档位。
// 它独立于工作队列：重新触发/重新排队不会清零计数，
// 只有成功、新一代配置或显式重置才会清零。
type retryTracker struct {
	mu      sync.Mutex
	records map[object.Key]failureRecord
}

func newRetryTracker() *retryTracker {
	return &retryTracker{records: map[object.Key]failureRecord{}}
}

// countFor 返回该对象在指定代配置下的当前连续失败次数。
func (t *retryTracker) countFor(k object.Key, generation int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.records[k]
	if !ok || rec.generation != generation {
		return 0
	}
	return rec.count
}

// recordFailure 记录一次失败并返回新的连续失败次数；
// 若配置换代则从头计数。
func (t *retryTracker) recordFailure(k object.Key, reason FailureReason, generation int64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.records[k]
	if rec.generation != generation {
		rec = failureRecord{generation: generation}
	}
	rec.count++
	rec.reason = reason
	t.records[k] = rec
	return rec.count
}

// recordSuccess 清除该对象的失败记录（成功收敛后退避回到最小等待）。
func (t *retryTracker) recordSuccess(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.records, k)
}

// reset 显式清零（运维重置）。
func (t *retryTracker) reset(k object.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.records, k)
}

// snapshot 返回视图查询所需的只读快照。
func (t *retryTracker) snapshot(k object.Key, generation int64) (count int, reason FailureReason) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.records[k]
	if !ok || rec.generation != generation {
		return 0, ReasonNone
	}
	return rec.count, rec.reason
}
