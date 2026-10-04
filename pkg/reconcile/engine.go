package reconcile

import (
	"context"
	"fmt"
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
	rec     *Reconciler
	store   *store.Store
	queue   *WorkQueue
	log     *slog.Logger
	workers int
	tracker *RetryTracker
	// defaultBudget 是未在 Spec.RetryBudget 中显式配置时使用的
	// 默认重试预算；<=0 表示不限次数（保持既有行为）。
	defaultBudget int

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
	// DefaultRetryBudget 是对象的默认自动重试预算（仅统计可恢复失败）。
	// 对象可用 Spec.RetryBudget 覆盖；<=0 表示不限次数。
	DefaultRetryBudget int
	Clock              Clock
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
	rec := &Reconciler{
		Store:     cfg.Store,
		Apply:     cfg.Applier,
		Clock:     cfg.Clock,
		BaseDelay: cfg.BaseDelay,
		MaxDelay:  cfg.MaxDelay,
		Log:       cfg.Logger,
	}
	return &Engine{
		rec:           rec,
		store:         cfg.Store,
		queue:         NewWorkQueue(),
		log:           cfg.Logger,
		workers:       cfg.Workers,
		tracker:       newRetryTracker(cfg.BaseDelay, cfg.MaxDelay, cfg.Clock.Now),
		defaultBudget: cfg.DefaultRetryBudget,
		inflight:      map[object.Key]int{},
		lastSeen:      map[object.Key]int64{},
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
			e.mu.Lock()
			seen := e.lastSeen[k]
			if rv > seen {
				e.lastSeen[k] = rv
			}
			e.mu.Unlock()
			if rv > seen || rv == 0 {
				// 新写入取代了挂起的延迟重试：队列折叠为立即执行，
				// 视图中的"下一次重试时间"同步清除，保持一致。
				e.tracker.clearSchedule(k)
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

		// 该键已被取出执行：上一轮的"挂起重试时间"已被消费，清除，
		// 避免视图展示一个其实已经在执行的重试计划。
		e.tracker.clearSchedule(k)

		e.log.Info("worker: reconcile begin",
			"worker", id, "key", k, "attempt", attempt, "observedRV", rv)

		d := e.rec.ReconcileOnce(ctx, k, attempt-1)
		// 应用退避/预算策略：失败计数、逐次变长的等待、预算耗尽转放弃。
		d = e.applyRetryPolicy(k, d)

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

		e.log.Info("worker: reconcile end",
			"worker", id, "key", k, "requeue", d.Requeue,
			"delay", d.Delay, "terminal", d.Terminal, "reason", d.Reason)

		// 先结束本轮：处理期间若有新变更已补排（立即重判最新版本），
		// 则不再按本轮的旧退避延迟重复入队；否则按判定结果排下一轮。
		if e.queue.Done(k) {
			// 补排的立即重判取代了本轮刚排定的延迟重试。
			e.tracker.clearSchedule(k)
			e.decInflight(k)
			continue
		}
		if d.Requeue {
			if d.Delay > 0 {
				e.queue.AddAfter(k, d.Delay)
			} else {
				e.queue.Add(k)
			}
		}
		// 在"重排队已完成"之后才解除在途标记：WaitIdle 据此不会
		// 在"协调已结束但下一轮尚未入队"的缝隙里误判为空闲。
		e.decInflight(k)
	}
}

func (e *Engine) decInflight(k object.Key) {
	e.mu.Lock()
	e.inflight[k]--
	if e.inflight[k] <= 0 {
		delete(e.inflight, k)
	}
	e.mu.Unlock()
}

// applyRetryPolicy 把一轮协调判定映射为引擎侧的退避/预算动作：
//   - 成功收敛：清零连续失败计数与退避档位；
//   - 可恢复失败：连续失败计数 +1，等待按档位逐次变长（封顶），
//     预算耗尽则写入 Abandoned 状态并停止自动重试；
//   - 冲突重判/等待依赖等非失败重排：只记录排定时间，不计数；
//   - 终止/放弃：清除挂起的重试计划。
func (e *Engine) applyRetryPolicy(k object.Key, d Decision) Decision {
	switch {
	case d.Gone:
		e.tracker.remove(k)
	case d.Converged:
		e.tracker.recordSuccess(k, d.Generation)
	case d.Terminal:
		e.tracker.clearSchedule(k)
	case d.Requeue && d.Failure && d.Reason == ReasonTransient:
		budget := e.budgetFor(k)
		delay, exhausted := e.tracker.recordFailure(k, d.Generation, budget)
		if exhausted {
			st, _ := e.tracker.snapshot(k)
			msg := fmt.Sprintf("retry budget exhausted after %d consecutive failures: %s",
				st.failures, d.Message)
			e.rec.MarkAbandoned(k, msg)
			e.log.Info("engine: retry budget exhausted, object abandoned",
				"key", k, "failures", st.failures, "budget", budget,
				"basis", "consecutive transient failures exceeded budget")
			return Decision{Key: k, Reason: ReasonBudgetExhausted, Message: msg,
				Generation: d.Generation}
		}
		d.Delay = delay
		if st, ok := e.tracker.snapshot(k); ok {
			e.log.Info("engine: backoff retry scheduled",
				"key", k, "consecutiveFailures", st.failures,
				"delay", delay, "nextRetryAt", st.nextRetryAt,
				"basis", "transient failure; backoff tier matches failure count")
		}
	case d.Requeue && d.Delay > 0:
		// 非失败性质的延迟重排（如等待依赖清理）：只同步排定时间。
		e.tracker.noteSchedule(k, d.Delay)
	}
	return d
}

// budgetFor 计算对象的有效重试预算：
// Spec.RetryBudget > 0 优先；< 0 表示不限；否则用引擎默认（<=0 同样不限）。
func (e *Engine) budgetFor(k object.Key) int {
	b := e.defaultBudget
	if obj, err := e.store.Get(k); err == nil {
		switch {
		case obj.Spec.RetryBudget > 0:
			b = obj.Spec.RetryBudget
		case obj.Spec.RetryBudget < 0:
			b = -1
		}
	}
	if b <= 0 {
		return -1 // 不限次数
	}
	return b
}

// Trigger 请求协调某个键（供外部变更/测试主动触发）。
func (e *Engine) Trigger(k object.Key) {
	e.mu.Lock()
	rv := e.store.Version(k)
	e.lastSeen[k] = rv
	e.mu.Unlock()
	// 新触发取代挂起的延迟重试（队列会把延迟项折叠为立即）。
	e.tracker.clearSchedule(k)
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
// 空闲判定：队列无排队/在处理项，且引擎侧无"已取出但尚未完成重排队"
// 的协调 —— 避免在"协调结束与下一轮入队之间"的缝隙里误判空闲。
func (e *Engine) WaitIdle(timeout time.Duration) bool {
	idle := func() bool {
		if e.queue.Len() != 0 || e.queue.Inflight() != 0 {
			return false
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		return len(e.inflight) == 0
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if idle() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return idle()
}

// Inflight 返回某键正在协调的次数（正常只可能是 0/1，用于测试"不交叉"）。
func (e *Engine) Inflight(k object.Key) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.inflight[k]
}
