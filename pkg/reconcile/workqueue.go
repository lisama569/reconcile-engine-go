package reconcile

import (
	"sync"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// queuedItem 记录一个待处理键及其最早可取用时间。
type queuedItem struct {
	key     object.Key
	readyAt time.Time
	attempt int
}

// WorkQueue 是带去重与延迟能力的每键工作队列：
//   - 同一 Key 在"排队中/处理中"期间重复加入会被折叠（dirty 集合），
//     保证该键任意时刻最多只有一个协调在执行，绝不并行交叉协调；
//   - AddAfter 支持退避延迟入队；
//   - 处理结束必须调用 Done(key)；若处理期间又收到触发，
//     Done 时会立即补排一轮。
type WorkQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	items   map[object.Key]*queuedItem
	order   []object.Key // 与 items 对应的取用时序（按 readyAt 惰性排序）
	working map[object.Key]bool
	dirty   map[object.Key]bool
	stopped bool
	nowFunc func() time.Time
}

// NewWorkQueue 创建队列。
func NewWorkQueue() *WorkQueue {
	q := &WorkQueue{
		items:   map[object.Key]*queuedItem{},
		working: map[object.Key]bool{},
		dirty:   map[object.Key]bool{},
		nowFunc: time.Now,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Add 立即入队。
// 已在队列中：折叠（且若原本是延迟项则提前为立即）。
// 正在处理：打 dirty 标记，Done 时补排。
func (q *WorkQueue) Add(k object.Key) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.working[k] {
		q.dirty[k] = true
		return
	}
	if it, ok := q.items[k]; ok {
		it.readyAt = q.nowFunc()
	} else {
		q.items[k] = &queuedItem{key: k, readyAt: q.nowFunc()}
		q.order = append(q.order, k)
	}
	q.cond.Broadcast()
}

// AddAfter 延迟入队。若该键已排队且更早可取用，则保留更早时间；
// 正在处理则等价于 dirty 立即补排（避免延迟丢失新一轮触发）。
func (q *WorkQueue) AddAfter(k object.Key, delay time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.working[k] {
		q.dirty[k] = true
		q.cond.Broadcast()
		return
	}
	ready := q.nowFunc().Add(delay)
	if it, ok := q.items[k]; ok {
		if ready.Before(it.readyAt) {
			it.readyAt = ready
		}
	} else {
		q.items[k] = &queuedItem{key: k, readyAt: ready}
		q.order = append(q.order, k)
	}
	q.cond.Broadcast()
}

// Get 阻塞取下一个到期键；返回 ok=false 表示队列已停止且已排空。
func (q *WorkQueue) Get() (k object.Key, attempt int, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.stopped && len(q.items) == 0 {
			return "", 0, false
		}
		now := q.nowFunc()
		// 在所有到期项中取 readyAt 最早者。
		kept := make([]object.Key, 0, len(q.order))
		var next *queuedItem
		var nextIdx int
		for _, kk := range q.order {
			it, exists := q.items[kk]
			if !exists {
				continue
			}
			kept = append(kept, kk)
			if !it.readyAt.After(now) && (next == nil || it.readyAt.Before(next.readyAt)) {
				next = it
				nextIdx = len(kept) - 1
			}
		}
		q.order = kept
		if next != nil {
			delete(q.items, next.key)
			q.order = append(q.order[:nextIdx], q.order[nextIdx+1:]...)
			q.working[next.key] = true
			next.attempt++
			return next.key, next.attempt, true
		}
		// 没有到期项：计算到最近项的等待时间。
		var wait time.Duration
		if len(q.order) > 0 {
			nearest := q.items[q.order[0]].readyAt
			for _, kk := range q.order[1:] {
				if t := q.items[kk].readyAt; t.Before(nearest) {
					nearest = t
				}
			}
			wait = nearest.Sub(now)
			if wait < 0 {
				wait = 0
			}
		}
		if wait == 0 {
			// 无项可处理（全在途）或无需等待：阻塞等待新信号。
			q.cond.Wait()
			continue
		}
		// 定时器到点后唤醒；期间任何 Add/Stop 也会提前唤醒重判。
		timer := time.AfterFunc(wait, q.cond.Broadcast)
		q.cond.Wait()
		timer.Stop()
	}
}

// Done 标记该键本轮处理完毕，允许后续再次调度；
// 处理期间若收到过新触发，立即补排一轮，并返回 true。
// 调用方据此决定是否还要按旧结果的退避延迟再排：已补排则跳过。
func (q *WorkQueue) Done(k object.Key) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.working, k)
	if q.dirty[k] {
		delete(q.dirty, k)
		q.items[k] = &queuedItem{key: k, readyAt: q.nowFunc()}
		q.order = append(q.order, k)
		q.cond.Broadcast()
		return true
	}
	q.cond.Broadcast()
	return false
}

// Len 返回排队中（不含处理中）的键数量。
func (q *WorkQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Inflight 返回正在处理中的键数量。
func (q *WorkQueue) Inflight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.working)
}

// Stop 停止队列，唤醒所有阻塞的 Get。
func (q *WorkQueue) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	q.cond.Broadcast()
}
