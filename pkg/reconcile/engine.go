package reconcile

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// Engine 驱动协调循环：N 个 worker 并行处理不同对象，
// 同一对象始终被串行协调（由 WorkQueue 去重保证）；
// 监听 Store 的版本变更通知自动重排队。
type Engine struct {
	rec        *Reconciler
	store      *store.Store
	queue      *WorkQueue
	tracker    *retryTracker
	selfWrites *selfWriteLog
	log        *slog.Logger
	workers    int

	wg       sync.WaitGroup
	cancel   context.CancelFunc
	notifyWg sync.WaitGroup

	mu       sync.Mutex
	inflight map[object.Key]int
	lastSeen map[object.Key]int64
}

// Config 是引擎配置。
type Config struct {
	Store     *store.Store
	Applier   Applier
	Logger    *slog.Logger
	Workers   int
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Clock     Clock
	// DefaultRetryBudget 是全局默认重试预算（同一对象连续可恢复失败的
	// 最大次数，超过即进入"已放弃、需人工介入"状态）；<=0 表示不限。
	// 单个对象可用 Spec.RetryBudget 覆盖（负数表示该对象不限）。
	DefaultRetryBudget int
}

// New 构造引擎（尚未启动）。
func New(cfg Config) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = DefaultBaseDelay
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultMaxDelay
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	tracker := newRetryTracker()
	selfWrites := newSelfWriteLog()
	rec := &Reconciler{
		Store:         cfg.Store,
		Apply:         cfg.Applier,
		Clock:         cfg.Clock,
		BaseDelay:     cfg.BaseDelay,
		MaxDelay:      cfg.MaxDelay,
		Log:           cfg.Logger,
		Tracker:       tracker,
		DefaultBudget: cfg.DefaultRetryBudget,
		SelfWrites:    selfWrites,
	}
	return &Engine{
		rec:        rec,
		store:      cfg.Store,
		queue:      NewWorkQueue(),
		tracker:    tracker,
		selfWrites: selfWrites,
		log:        cfg.Logger,
		workers:    cfg.Workers,
		inflight:   map[object.Key]int{},
		lastSeen:   map[object.Key]int64{},
	}
}

// Start 启动 worker 与存储变更监听，并对现存对象做一次初始入队。
func (e *Engine) Start(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)

	// 初始入队：引导收敛全部现存对象。
	for _, o := range e.store.List() {
		e.Trigger(o.Key)
	}

	// 存储版本变更监听：外部写入（含测试并发改写）自动触发重判。
	e.notifyWg.Add(1)
	go e.watchNotify(ctx)

	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}
}

func (e *Engine) watchNotify(ctx context.Context) {
	defer e.notifyWg.Done()
	ch := e.store.Notify()
	for {
		select {
		case <-ctx.Done():
			return
		case k, ok := <-ch:
			if !ok {
				return
			}
			// 仅当版本比本引擎上次观测更新时才触发，避免自我写入抖动。
			rv := e.store.Version(k)
			// 引擎自己的写入（如记录失败状态）产生的通知直接消费跳过：
			// 否则自写会把刚排好的退避等待折叠成立即重判，破坏逐次变长。
			if e.selfWrites.consume(k, rv) {
				continue
			}
			e.mu.Lock()
			seen := e.lastSeen[k]
			if rv > seen {
				e.lastSeen[k] = rv
			}
			e.mu.Unlock()
			if rv > seen || rv == 0 {
				e.queue.Add(k)
			}
		}
	}
}

func (e *Engine) worker(ctx context.Context, id int) {
	defer e.wg.Done()
	for {
		k, attempt, ok := e.queue.Get()
		if !ok {
			return
		}
		select {
		case <-ctx.Done():
			e.queue.Done(k)
			return
		default:
		}

		e.mu.Lock()
		e.inflight[k]++
		// 同一键任何时刻最多只有一个在途协调（WorkQueue 已保证去重，
		// 这里再断言一次，供测试与防御）。
		if e.inflight[k] > 1 {
			e.log.Error("internal: same key reconciled concurrently",
				"key", k, "inflight", e.inflight[k])
		}
		e.mu.Unlock()

		rv := e.store.Version(k)
		e.mu.Lock()
		if rv > e.lastSeen[k] {
			e.lastSeen[k] = rv
		}
		e.mu.Unlock()

		e.log.Info("worker: reconcile begin",
			"worker", id, "key", k, "attempt", attempt, "observedRV", rv)

		d := e.rec.ReconcileOnce(ctx, k, attempt-1)

		// 协调过程中（含引擎自身写入）可能产生了新版本：刷新观测水位。
		if newRV := e.store.Version(k); newRV > 0 {
			e.mu.Lock()
			if newRV > e.lastSeen[k] {
				e.lastSeen[k] = newRV
			}
			e.mu.Unlock()
		} else {
			e.mu.Lock()
			delete(e.lastSeen, k)
			e.mu.Unlock()
		}

		e.mu.Lock()
		e.inflight[k]--
		if e.inflight[k] <= 0 {
			delete(e.inflight, k)
		}
		e.mu.Unlock()

		e.log.Info("worker: reconcile end",
			"worker", id, "key", k, "requeue", d.Requeue,
			"delay", d.Delay, "terminal", d.Terminal, "reason", d.Reason)

		// 先结束本轮：处理期间若有新变更已补排（立即重判最新版本），
		// 则不再按本轮的旧退避延迟重复入队；否则按判定结果排下一轮。
		if e.queue.Done(k) {
			continue
		}
		if d.Requeue {
			if d.Delay > 0 {
				e.queue.AddAfter(k, d.Delay)
			} else {
				e.queue.Add(k)
			}
		}
	}
}

// Trigger 请求协调某个键（供外部变更/测试主动触发）。
func (e *Engine) Trigger(k object.Key) {
	e.mu.Lock()
	rv := e.store.Version(k)
	e.lastSeen[k] = rv
	e.mu.Unlock()
	e.queue.Add(k)
}

// Stop 停止引擎并等待所有 worker 退出。
func (e *Engine) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
	e.queue.Stop()
	e.wg.Wait()
	e.notifyWg.Wait()
}

// WaitIdle 等待队列排空且没有在途协调（便于测试断言）。超时返回 false。
func (e *Engine) WaitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.queue.Len() == 0 && e.queue.Inflight() == 0 {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return e.queue.Len() == 0 && e.queue.Inflight() == 0
}

// Inflight 返回某键正在协调的次数（正常只可能是 0/1，用于测试"不交叉"）。
func (e *Engine) Inflight(k object.Key) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inflight[k]
}
