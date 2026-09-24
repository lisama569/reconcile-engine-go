package reconcile

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// WorkQueue 是去重 + 延迟 + 按 key 互斥的工作队列。
//
// 语义：
//   - Add 对同一 key 去重：已在等待队列中的重复 Add 合并为一次；
//     key 正在被处理时再次 Add 会在当前轮次结束后自动补一轮（防止漏事件）。
//   - AddAfter 在延迟后入队，用于退避重试；同一 key 的多个延迟取最早者。
//   - 同一 key 在任何时刻最多被一个 worker 取走（同一对象不交叉协调），
//     不同 key 可并行被取走。
type WorkQueue interface {
	Add(key string)
	AddAfter(key string, delay time.Duration)
	// Get 阻塞取下一个就绪且未在处理中的 key；ctx 取消或 Shutdown 时返回 false。
	Get(ctx context.Context) (string, bool)
	// Done 标记该 key 本轮处理结束，释放 key 的互斥锁，并补入处理期间到达的事件。
	Done(key string)
	Shutdown()
}

type scheduledItem struct {
	key string
	at  time.Time
}

type scheduleHeap []scheduledItem

func (h scheduleHeap) Len() int           { return len(h) }
func (h scheduleHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h scheduleHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scheduleHeap) Push(x any)        { *h = append(*h, x.(scheduledItem)) }
func (h *scheduleHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// dedupQueue 是 WorkQueue 的本地实现。
type dedupQueue struct {
	mu sync.Mutex

	// queued 记录每个 key 当前在堆中权威的最早就绪时间；堆里可能残留过期条目。
	queued map[string]time.Time
	sch    scheduleHeap
	// busy 已被 Get 取走但尚未 Done 的 key（同一 key 不交叉协调）。
	busy map[string]bool
	// requeueAt：key 处理期间再次 Add/AddAfter 的最早就绪时间，Done 时补入。
	requeueAt map[string]time.Time

	shutdown bool
	// signal 缓冲为 1 的唤醒信号，状态变化时非阻塞发送。
	signal chan struct{}
}

// NewWorkQueue 创建工作队列。
func NewWorkQueue() WorkQueue {
	return &dedupQueue{
		queued:    make(map[string]time.Time),
		busy:      make(map[string]bool),
		requeueAt: make(map[string]time.Time),
		signal:    make(chan struct{}, 1),
	}
}

func (q *dedupQueue) Add(key string) {
	q.addAt(key, time.Now())
}

func (q *dedupQueue) AddAfter(key string, delay time.Duration) {
	q.addAt(key, time.Now().Add(delay))
}

func (q *dedupQueue) addAt(key string, at time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown {
		return
	}
	if q.busy[key] {
		// 处理进行中：记住补入时间，Done 时统一安排，避免与当前轮次交叉。
		if old, ok := q.requeueAt[key]; !ok || at.Before(old) {
			q.requeueAt[key] = at
		}
		q.wake()
		return
	}
	if old, ok := q.queued[key]; ok && !at.Before(old) {
		return // 已有更早或相同的排程，重复事件去重
	}
	q.queued[key] = at
	heap.Push(&q.sch, scheduledItem{key: key, at: at})
	q.wake()
}

func (q *dedupQueue) Get(ctx context.Context) (string, bool) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		wait := q.prepare(timer)
		if wait < 0 {
			return "", false
		}
		if wait == 0 {
			if key, ok := q.takeReady(); ok {
				return key, true
			}
			continue
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return "", false
		case <-q.signal:
		case <-timer.C:
		}
	}
}

// prepare 返回需要等待的时长：0 表示已有就绪项，-1 表示已关闭，正数表示睡眠时长。
func (q *dedupQueue) prepare(timer *time.Timer) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown {
		return -1
	}
	if q.sch.Len() == 0 {
		return time.Hour
	}
	d := time.Until(q.sch[0].at)
	if d <= 0 {
		return 0
	}
	return d
}

// takeReady 弹出一个就绪且未在处理中的 key，清理堆中过期/失效条目。
func (q *dedupQueue) takeReady() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for q.sch.Len() > 0 && !q.sch[0].at.After(now) {
		it := heap.Pop(&q.sch).(scheduledItem)
		if authoritative, ok := q.queued[it.key]; !ok || authoritative != it.at {
			continue // 已被更早的排程取代
		}
		delete(q.queued, it.key)
		if q.busy[it.key] {
			continue // 理论上不会发生（入队路径保证互斥），保守跳过
		}
		q.busy[it.key] = true
		return it.key, true
	}
	return "", false
}

func (q *dedupQueue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.busy, key)
	if at, ok := q.requeueAt[key]; ok {
		delete(q.requeueAt, key)
		now := time.Now()
		if at.Before(now) {
			at = now
		}
		if old, exists := q.queued[key]; !exists || at.Before(old) {
			q.queued[key] = at
			heap.Push(&q.sch, scheduledItem{key: key, at: at})
		}
		q.wake()
	}
}

func (q *dedupQueue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdown = true
	q.wake()
}

func (q *dedupQueue) wake() {
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// len 返回待处理（含延迟中）的 key 数，用于测试排空判定。
func (q *dedupQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queued) + len(q.busy)
}
