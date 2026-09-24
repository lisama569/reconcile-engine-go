package reconcile

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

// Controller 是多对象调度器：
//   - 启动时全量入队，并监听 Store 变更事件按 key 入队（派生对象变更同时唤醒属主）；
//   - worker 池跨 key 并行处理，同一 key 经队列互斥串行（同一对象不交叉协调）；
//   - 可恢复失败按 Backoff 指数退避重试；不可恢复失败终止重试；
//     冲突错误立即以最新版本重试（不等待退避）；
//   - 单个对象的失败只影响自身，不阻塞其他对象。
type Controller struct {
	queue   WorkQueue
	rec     Reconciler
	backoff Backoff
	log     Logger
	store   Store
	workers int

	// failures 记录每个 key 的连续可恢复失败次数，用于退避计算；成功即清零。
	failuresMu sync.Mutex
	failures   map[string]int

	totalReconciles   atomic.Int64
	transientFailures atomic.Int64
	terminalFailures  atomic.Int64
	conflictRequeues  atomic.Int64

	wg sync.WaitGroup
}

// Config 是 Controller 的构造参数。
type Config struct {
	Store   Store
	Rec     Reconciler
	Backoff Backoff
	Log     Logger
	Workers int
}

// Metrics 暴露可断言的调度计数。
type Metrics struct {
	TotalReconciles   int64
	TransientFailures int64
	TerminalFailures  int64
	ConflictRequeues  int64
}

// NewController 构造调度器（尚未启动）。
func NewController(cfg Config) *Controller {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Log == nil {
		cfg.Log = noopLogger{}
	}
	if cfg.Backoff == nil {
		cfg.Backoff = ExponentialBackoff{}
	}
	return &Controller{
		queue:    NewWorkQueue(),
		rec:      cfg.Rec,
		backoff:  cfg.Backoff,
		log:      cfg.Log,
		store:    cfg.Store,
		workers:  cfg.Workers,
		failures: make(map[string]int),
	}
}

// Run 启动事件订阅与 worker，直到 ctx 取消且在途协调结束后返回。
func (c *Controller) Run(ctx context.Context) {
	events, cancel := c.store.Subscribe()
	defer cancel()
	c.enqueueAll()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				c.enqueueEvent(ev)
			}
		}
	}()

	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	<-ctx.Done()
	c.queue.Shutdown()
	c.wg.Wait()
}

func (c *Controller) enqueueAll() {
	for _, o := range c.store.List(KindService) {
		c.queue.Add(keyOf(o.ID))
	}
	for _, o := range c.store.List(KindEndpoint) {
		c.queue.Add(keyOf(o.ID))
	}
}

func (c *Controller) enqueueEvent(ev Event) {
	c.queue.Add(keyOf(ev.ID))
	// 派生对象 Upsert（含其状态变化）立即唤醒属主重新计数就绪副本；
	// Delete 事件对象已消失，属主靠安全重扫发现，不在此伪造引用。
	if ev.Type == EventUpsert && ev.ID.Kind == KindEndpoint && ev.Owner != "" {
		c.queue.Add(keyOf(ObjectID{Kind: KindService, Name: ev.Owner}))
	}
}

func (c *Controller) worker(ctx context.Context, id int) {
	defer c.wg.Done()
	for {
		key, ok := c.queue.Get(ctx)
		if !ok {
			return
		}
		c.process(key)
		c.queue.Done(key)
	}
}

func (c *Controller) process(key string) {
	defer func() {
		// 单个对象的 panic 不得拖垮 worker / 阻塞其他对象。
		if r := recover(); r != nil {
			c.log.Logf("[controller] key=%s panic: %v -> 按可恢复失败退避重试", key, r)
			c.scheduleRetry(key)
		}
	}()

	id, ok := parseKey(key)
	if !ok {
		c.log.Logf("[controller] key=%q 非法，丢弃", key)
		return
	}

	c.totalReconciles.Add(1)
	res, err := c.rec.Reconcile(id)
	switch {
	case err == nil:
		if res.Requeue {
			if res.Delay > 0 {
				c.queue.AddAfter(key, res.Delay) // 安全重扫
			} else {
				c.queue.Add(key) // 立即重跑（finalizer 注册后续跑等）
			}
			return
		}
		c.clearFailure(key) // 已收敛：重置退避
	case IsClass(err, ClassTerminal):
		// 不可恢复：不再入队。配置被用户改好后会产生新事件，重新入队判定。
		c.terminalFailures.Add(1)
		c.clearFailure(key)
		c.log.Logf("[controller] key=%s 终止重试，原因=%v", key, err)
	case IsClass(err, ClassConflict):
		c.conflictRequeues.Add(1)
		c.queue.Add(key) // 冲突：立即以最新版本重试，不退避
	default:
		// 可恢复失败：指数退避，保留上次成功状态。
		c.transientFailures.Add(1)
		c.log.Logf("[controller] key=%s 可恢复失败：%v -> 退避重试", key, err)
		c.scheduleRetry(key)
	}
}

func (c *Controller) scheduleRetry(key string) {
	c.failuresMu.Lock()
	c.failures[key]++
	n := c.failures[key]
	c.failuresMu.Unlock()
	delay := c.backoff.Next(n)
	c.log.Logf("[controller] key=%s 第%d次连续失败，%s 后重试", key, n, delay)
	c.queue.AddAfter(key, delay)
}

func (c *Controller) clearFailure(key string) {
	c.failuresMu.Lock()
	delete(c.failures, key)
	c.failuresMu.Unlock()
}

// SnapshotMetrics 返回指标快照。
func (c *Controller) SnapshotMetrics() Metrics {
	return Metrics{
		TotalReconciles:   c.totalReconciles.Load(),
		TransientFailures: c.transientFailures.Load(),
		TerminalFailures:  c.terminalFailures.Load(),
		ConflictRequeues:  c.conflictRequeues.Load(),
	}
}

func keyOf(id ObjectID) string {
	return string(id.Kind) + "/" + id.Name
}

func parseKey(key string) (ObjectID, bool) {
	kind, name, ok := strings.Cut(key, "/")
	if !ok || name == "" {
		return ObjectID{}, false
	}
	return ObjectID{Kind: Kind(kind), Name: name}, true
}
