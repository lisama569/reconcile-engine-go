package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"
)

func testBackoff() Backoff {
	return ExponentialBackoff{Base: 5 * time.Millisecond, Max: 500 * time.Millisecond}
}

func runController(t *testing.T, store Store, rec Reconciler, workers int) (func(), *Controller) {
	t.Helper()
	c := NewController(Config{
		Store:   store,
		Rec:     rec,
		Backoff: testBackoff(),
		Log:     testLogger{t},
		Workers: workers,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	stop := func() {
		cancel()
		<-done // 等待所有 worker 退出，避免测试结束后仍有日志写入
	}
	return stop, c
}

// 场景5：可恢复失败按指数退避重试，且保留上次成功写入的状态。
func TestController_TransientRetry_PreservesLastGoodStatus(t *testing.T) {
	store, ext, rec := newEngine(t)

	mustCreate(t, store, newService("svc", 1))
	ep := mustCreate(t, store, newEndpoint("svc-0", "svc", 100))
	// 先完成一次成功协调：外部资源已创建，状态为 Applied。
	if _, err := rec.Reconcile(ep.ID); err != nil {
		t.Fatal(err)
	}
	writeCountAtSuccess := store.WriteCount(ep.ID)

	// 注入 3 次“已存在资源的幂等确认”可恢复故障。
	ext.SetEnsureFail(3)
	start := time.Now()
	stop, c := runController(t, store, rec, 2)
	defer stop()

	// 等到观察到 3 次可恢复失败；故障期间状态必须始终保留上次成功值。
	eventually(t, time.Second, func() bool {
		cur, ok := store.Get(ep.ID)
		if !ok {
			return false
		}
		if cur.EndpointStatus.Phase != PhaseApplied || !cur.EndpointStatus.Provisioned {
			t.Fatalf("可恢复失败期间上次成功状态被覆盖: %+v", cur.EndpointStatus)
		}
		return c.SnapshotMetrics().TransientFailures >= 3
	})

	// 退避必须真实生效：5ms+10ms（第三次失败发生在约15ms后）。
	if elapsed := time.Since(start); elapsed < 12*time.Millisecond {
		t.Fatalf("退避未生效，3 次失败仅耗时 %s", elapsed)
	}

	// 故障耗尽后自动收敛：外部资源仍在，无新副作用，状态不再被改写。
	eventually(t, time.Second, func() bool {
		return ext.IsProvisioned(ep.ID) && store.WriteCount(ep.ID) == writeCountAtSuccess
	})
	if ext.Mutations() != 1 {
		t.Fatalf("重试不应产生额外副作用, mutations=%d", ext.Mutations())
	}

	cur, _ := store.Get(ep.ID)
	if cur.EndpointStatus.Phase != PhaseApplied || !cur.EndpointStatus.Provisioned {
		t.Fatalf("最终状态异常: %+v", cur.EndpointStatus)
	}
}

// 场景6：不可恢复失败终止重试，且单个对象失败不阻塞其他对象。
func TestController_TerminalFailureIsolated(t *testing.T) {
	store, _, rec := newEngine(t)

	bad := mustCreate(t, store, newService("bad", 1))
	bad.Service.Invalid = true
	if _, err := store.UpdateSpec(bad); err != nil {
		t.Fatal(err)
	}
	good := mustCreate(t, store, newService("good", 2))

	stop, c := runController(t, store, rec, 4)
	defer stop()

	// 好对象正常收敛。
	eventually(t, 2*time.Second, func() bool {
		s, ok := store.Get(good.ID)
		return ok && s.ServiceStatus.Phase == PhaseApplied
	})
	if n := len(store.List(KindEndpoint)); n != 2 {
		t.Fatalf("好 Service 应派生 2 个对象，实际 %d", n)
	}

	// 坏对象进入 Terminal，指标可区分。
	eventually(t, time.Second, func() bool {
		s, _ := store.Get(bad.ID)
		return s.ServiceStatus.Phase == PhaseTerminal && s.ServiceStatus.Message != ""
	})
	m := c.SnapshotMetrics()
	if m.TerminalFailures < 1 {
		t.Fatalf("应记录 Terminal 失败, metrics=%+v", m)
	}

	// 等待足够长时间，确认 Terminal 对象不再被重试（指标不增长）。
	base := c.SnapshotMetrics().TotalReconciles
	time.Sleep(80 * time.Millisecond)
	if c.SnapshotMetrics().TotalReconciles != base {
		t.Fatalf("Terminal 对象仍被反复协调: %d -> %d",
			base, c.SnapshotMetrics().TotalReconciles)
	}
	b, _ := store.Get(bad.ID)
	if b.ServiceStatus.Message == "" {
		t.Fatalf("Terminal 失败原因缺失")
	}
}

// 场景7：已收敛对象不会被控制器反复改写（事件驱动 + 去重）。
func TestController_NoChurnAfterConverged(t *testing.T) {
	store, _, rec := newEngine(t)
	svc := mustCreate(t, store, newService("calm", 1))
	stop, c := runController(t, store, rec, 2)
	defer stop()

	eventually(t, time.Second, func() bool {
		s, ok := store.Get(svc.ID)
		return ok && s.ServiceStatus.Phase == PhaseApplied
	})
	before := c.SnapshotMetrics().TotalReconciles
	beforeW := store.WriteCount(svc.ID)
	time.Sleep(120 * time.Millisecond) // 超过 resyncDelay，观察是否空转
	if c.SnapshotMetrics().TotalReconciles != before {
		t.Fatalf("已收敛后仍发生协调: %d -> %d",
			before, c.SnapshotMetrics().TotalReconciles)
	}
	if store.WriteCount(svc.ID) != beforeW {
		t.Fatalf("已收敛 Service 被反复写入")
	}
}

// 场景8：不同对象可并行处理（跨 key 并行度大于 1）。
func TestController_ParallelAcrossKeys(t *testing.T) {
	store, ext := NewMemoryStore(), NewFakeExternal()
	rec := NewReconciler(store, ext, testLogger{t})

	gate := &concurrencyGate{}
	wrapped := &gateExternal{inner: ext, gate: gate, hold: 4 * time.Millisecond}
	rec.external = wrapped

	for i := 0; i < 8; i++ {
		mustCreate(t, store, newService("svc"+itoa(i), 1))
	}
	stop, _ := runController(t, store, rec, 8)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for i := 0; i < 8; i++ {
			s, ok := store.Get(ObjectID{Kind: KindService, Name: "svc" + itoa(i)})
			if !ok || s.ServiceStatus.Phase != PhaseApplied {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 8; i++ {
		s, ok := store.Get(ObjectID{Kind: KindService, Name: "svc" + itoa(i)})
		if !ok || s.ServiceStatus.Phase != PhaseApplied {
			t.Fatalf("svc%d 未收敛", i)
		}
	}
	if gate.maxObserved() < 2 {
		t.Fatalf("未观察到跨 key 并行，最大并发=%d", gate.maxObserved())
	}
}

type concurrencyGate struct {
	mu  sync.Mutex
	cur int
	max int
}

func (g *concurrencyGate) enter() {
	g.mu.Lock()
	g.cur++
	if g.cur > g.max {
		g.max = g.cur
	}
	g.mu.Unlock()
}
func (g *concurrencyGate) leave() {
	g.mu.Lock()
	g.cur--
	g.mu.Unlock()
}
func (g *concurrencyGate) maxObserved() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.max
}

type gateExternal struct {
	inner ExternalSystem
	gate  *concurrencyGate
	hold  time.Duration
}

func (g *gateExternal) Provision(ep *Object) (bool, error) {
	g.gate.enter()
	defer g.gate.leave()
	time.Sleep(g.hold)
	return g.inner.Provision(ep)
}
func (g *gateExternal) Delete(ep *Object) (bool, error) { return g.inner.Delete(ep) }
func (g *gateExternal) IsProvisioned(id ObjectID) bool  { return g.inner.IsProvisioned(id) }
func (g *gateExternal) ResourceIDs() []ObjectID         { return g.inner.ResourceIDs() }
func (g *gateExternal) Mutations() int                  { return g.inner.Mutations() }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
