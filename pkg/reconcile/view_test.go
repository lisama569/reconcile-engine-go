package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// newHarnessCfg 允许测试定制引擎配置（退避参数、预算等）。
func newHarnessCfg(t *testing.T, inner Applier, cfgFn func(*Config), seed ...*object.Object) *harness {
	t.Helper()
	st := store.New(seed...)
	counter := NewCountingApplier(inner)
	log := newTestLogger(t)
	cfg := Config{
		Store:     st,
		Applier:   counter,
		Logger:    log,
		Workers:   4,
		BaseDelay: 5 * time.Millisecond,
		MaxDelay:  40 * time.Millisecond,
		Clock:     SystemClock{},
	}
	if cfgFn != nil {
		cfgFn(&cfg)
	}
	return &harness{t: t, st: st, counter: counter, eng: New(cfg), log: log}
}

// 视图 1：已收敛对象的视图 —— 阶段、已完成项、无失败、无待重试。
func TestView_ConvergedObject(t *testing.T) {
	h := newHarness(t, ApplierFunc(func(context.Context, ApplyRequest) error { return nil }),
		ownerObject("view-ok", map[string]string{"bucket": "b1"}))
	h.start()
	h.waitIdle()

	v := h.eng.View("view-ok")
	t.Logf("view: %+v", v)
	if !v.Exists {
		t.Fatal("object should exist")
	}
	if v.Phase != object.PhaseReady {
		t.Fatalf("want Ready, got %s", v.Phase)
	}
	if len(v.ReadyItems) != 1 || v.ReadyItems["bucket"] != "b1" {
		t.Fatalf("ready items mismatch: %v", v.ReadyItems)
	}
	if v.LastFailureKind != ReasonNone {
		t.Fatalf("converged object should have no failure, got %s", v.LastFailureKind)
	}
	if v.ConsecutiveFailures != 0 {
		t.Fatalf("converged object should have 0 consecutive failures, got %d", v.ConsecutiveFailures)
	}
	if v.RetryScheduled {
		t.Fatalf("converged object must not have a pending retry, next=%v", v.NextRetryAt)
	}
	if v.GaveUp || v.WaitingOnChildren || v.Deleting {
		t.Fatal("converged object must not be deleting/gave-up/waiting")
	}

	// 不存在的对象：Exists=false，不报错。
	if nv := h.eng.View("ghost"); nv.Exists {
		t.Fatal("ghost object must not exist in view")
	}
}

// 视图 2：持续可恢复失败中的对象 —— 视图暴露失败分类、连续失败次数与下次重试时间。
func TestView_FailingObjectShowsRetryState(t *testing.T) {
	gate := newGateApplier()
	gate.gate("flaky-item")
	h := newHarnessCfg(t, gate, func(c *Config) {
		c.BaseDelay = 50 * time.Millisecond
		c.MaxDelay = 400 * time.Millisecond
	}, ownerObject("view-fail", map[string]string{"flaky-item": "x"}))
	h.start()
	gate.release("flaky-item", false) // 放行但始终可恢复失败

	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.View("view-fail")
		return v.ConsecutiveFailures >= 2
	}) {
		v := h.eng.View("view-fail")
		t.Fatalf("expected consecutive failures to accumulate, view=%+v", v)
	}

	v := h.eng.View("view-fail")
	t.Logf("view under failure: %+v", v)
	if v.Phase != object.PhaseFailed {
		t.Fatalf("want Failed, got %s", v.Phase)
	}
	if v.LastFailureKind != ReasonTransient {
		t.Fatalf("want Transient failure kind, got %s", v.LastFailureKind)
	}
	if !v.LastFailureRetriable {
		t.Fatal("transient failure must be marked retriable")
	}
	if !v.RetryScheduled {
		t.Fatal("a retry should be scheduled while failing")
	}
	if until := time.Until(v.NextRetryAt); until <= 0 || until > 2*time.Second {
		t.Fatalf("next retry should be in the near future, got %v", until)
	}
	if v.GaveUp {
		t.Fatal("no budget configured: must not give up")
	}
}

// 视图 3：查询无副作用 —— 并发查询不改变版本、不产生额外副作用调用。
func TestView_QueryHasNoSideEffects(t *testing.T) {
	gate := newGateApplier()
	gate.gate("flaky-item")
	h := newHarnessCfg(t, gate, func(c *Config) {
		c.BaseDelay = 100 * time.Millisecond
		c.MaxDelay = 800 * time.Millisecond
	}, ownerObject("view-ro", map[string]string{"flaky-item": "x"}))
	h.start()
	gate.release("flaky-item", false)

	if !h.pollFor(3*time.Second, func() bool {
		return h.eng.View("view-ro").ConsecutiveFailures >= 1
	}) {
		t.Fatal("object never entered failure state")
	}

	rvBefore := h.st.Version("view-ro")
	callsBefore := h.counter.TotalCalls()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = h.eng.View("view-ro")
			}
		}()
	}
	wg.Wait()

	if rv := h.st.Version("view-ro"); rv != rvBefore {
		t.Fatalf("view query must not bump version: %d -> %d", rvBefore, rv)
	}
	if n := h.counter.TotalCalls(); n != callsBefore {
		t.Fatalf("view query must not trigger side-effects: %d -> %d", callsBefore, n)
	}
}

// 视图 4：新写入取消旧重试后，视图里的重试计划必须立即与最新状态一致，
// 不允许还挂着一条已被取消的远期重试。
func TestView_RetryPlanConsistentAfterNewWrite(t *testing.T) {
	gate := newGateApplier()
	gate.gate("flaky-item")
	h := newHarnessCfg(t, gate, func(c *Config) {
		c.BaseDelay = 5 * time.Second // 超长退避：若旧计划未被取消，测试必然超时
		c.MaxDelay = 5 * time.Second
	}, ownerObject("view-cancel", map[string]string{"flaky-item": "x"}))
	h.start()
	gate.release("flaky-item", false)

	// 等到第一次失败发生、远期重试已排上。
	var oldNext time.Time
	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.View("view-cancel")
		if v.RetryScheduled && v.ConsecutiveFailures >= 1 {
			oldNext = v.NextRetryAt
			return true
		}
		return false
	}) {
		t.Fatal("expected a scheduled far-future retry after first failure")
	}
	if until := time.Until(oldNext); until < 2*time.Second {
		t.Fatalf("test setup: retry should be scheduled far out, got %v", until)
	}
	t.Logf("old retry scheduled at %v (in %v)", oldNext, time.Until(oldNext))

	// 外部修复：让副作用恢复成功，并提交新一代 Spec（取消旧的退避计划）。
	gate.release("flaky-item", true)
	cur := h.get("view-cancel")
	next := cur.DeepCopy()
	next.Spec.Generation++
	if _, err := h.st.CAS(next, cur.ResourceVersion); err != nil {
		t.Fatalf("bump generation: %v", err)
	}

	// 远早于旧重试到点就收敛：证明旧计划被取消而不是被等待。
	start := time.Now()
	if !h.pollFor(2*time.Second, func() bool {
		return h.get("view-cancel").Status.Phase == object.PhaseReady
	}) {
		t.Fatal("object did not converge promptly after new spec; stale retry plan may still be in effect")
	}
	if elapsed := time.Since(start); elapsed >= time.Until(oldNext) {
		t.Fatalf("convergence waited for the canceled retry: elapsed=%v", elapsed)
	}

	v := h.eng.View("view-cancel")
	t.Logf("view after new write: %+v", v)
	if v.RetryScheduled {
		t.Fatalf("canceled retry must not appear in view, next=%v", v.NextRetryAt)
	}
	if v.ConsecutiveFailures != 0 {
		t.Fatalf("new generation must reset failure count, got %d", v.ConsecutiveFailures)
	}
	if v.LastFailureKind != ReasonNone {
		t.Fatalf("recovered object should show no failure, got %s", v.LastFailureKind)
	}
}

// 视图 5：删除路径 —— 视图能看出对象在等依赖 child 清理完。
func TestView_DeletingWaitsOnChildren(t *testing.T) {
	gate := newGateApplier()
	gate.gate("owned-by-owner-v") // child 的 Cleanup 副作用将持续可恢复失败
	h := newHarness(t, gate,
		readyOwner("owner-v"),
		childObject("cv1", "owner-v"))
	h.start()
	h.waitIdle()

	o := h.get("owner-v")
	o.MarkDeleted("2026-10-04T00:00:00Z")
	if _, err := h.st.CAS(o, h.st.Version("owner-v")); err != nil {
		t.Fatal(err)
	}
	h.eng.Trigger("owner-v")
	gate.release("owned-by-owner-v", false) // 放行但以可恢复失败返回

	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.View("owner-v")
		return v.Deleting && v.WaitingOnChildren && v.Phase == object.PhaseDeleting
	}) {
		v := h.eng.View("owner-v")
		t.Fatalf("view should show waiting on children cleanup, view=%+v", v)
	}
	v := h.eng.View("owner-v")
	t.Logf("view while blocked on children: %+v", v)
	if !v.Exists {
		t.Fatal("owner should still exist while children remain")
	}

	// 放行清理：child 回收、finalizer 摘除后，视图不再显示等待。
	gate.release("owned-by-owner-v", true)
	if !h.pollFor(3*time.Second, func() bool {
		return len(h.st.ListChildren("owner-v")) == 0 &&
			!h.get("owner-v").HasFinalizer(object.FinalizerOwnerCleanup)
	}) {
		t.Fatal("cascade cleanup did not finish after gate released")
	}
	h.waitIdle()
	v = h.eng.View("owner-v")
	if v.WaitingOnChildren {
		t.Fatal("view must not report waiting after children are gone")
	}
}
