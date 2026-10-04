package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// 视图内容：进行中的可恢复失败能被完整读出 —— 阶段、已确认项、
// 失败类别、连续失败次数、下一次重试时间。
func TestInspect_ViewDuringTransientFailure(t *testing.T) {
	flaky := newFlakyApplier()
	flaky.failTimes("svc-view", "bad", 1<<20) // bad 项长时间可恢复失败
	h := newHarness(t, flaky,
		ownerObject("svc-view", map[string]string{"good": "g", "bad": "b"}))
	h.start()

	var v ConvergenceView
	if !h.pollFor(3*time.Second, func() bool {
		v = h.eng.Inspect("svc-view")
		return v.ConsecutiveFailures >= 1
	}) {
		t.Fatal("expected consecutive failures to be observable in view")
	}
	if !v.Exists || v.Phase != object.PhaseFailed {
		t.Fatalf("want Failed phase, got %+v", v)
	}
	if !v.Retriable || v.FailureClass != ReasonTransient {
		t.Fatalf("want retriable transient failure, got class=%s retriable=%v",
			v.FailureClass, v.Retriable)
	}
	if v.LastFailure == "" {
		t.Fatal("view must carry last failure message")
	}
	if _, ok := v.ReadyItems["good"]; !ok {
		t.Fatalf("confirmed ready item must be visible: %v", v.ReadyItems)
	}
	if v.NextRetryAt.IsZero() || time.Until(v.NextRetryAt) > 40*time.Millisecond+10*time.Millisecond {
		t.Fatalf("next retry must be scheduled within max delay, got %v", v.NextRetryAt)
	}
	if v.WaitingForDependents || v.Abandoned {
		t.Fatalf("unexpected flags: %+v", v)
	}
	// 连续两次查询结果一致：查询本身不改变重试计划。
	v2 := h.eng.Inspect("svc-view")
	if !v2.NextRetryAt.Equal(v.NextRetryAt) && !v2.NextRetryAt.IsZero() {
		// 允许重试到点被执行（时间推进），但不允许查询改变计划。
		if v2.NextRetryAt.Before(v.NextRetryAt) {
			t.Fatalf("query must not alter retry plan: %v -> %v", v.NextRetryAt, v2.NextRetryAt)
		}
	}
}

// 查询无副作用：反复 Inspect/InspectAll 不改变对象版本、不触发副作用调用。
func TestInspect_QueryHasNoSideEffects(t *testing.T) {
	h := newHarness(t, ApplierFunc(func(context.Context, ApplyRequest) error { return nil }),
		ownerObject("svc-q", map[string]string{"a": "1", "b": "2"}))
	h.start()
	h.waitIdle()

	rv := h.st.Version("svc-q")
	calls := h.counter.TotalCalls()
	for i := 0; i < 200; i++ {
		v := h.eng.Inspect("svc-q")
		if !v.Exists || v.Phase != object.PhaseReady {
			t.Fatalf("unexpected view: %+v", v)
		}
	}
	for i := 0; i < 50; i++ {
		views := h.eng.InspectAll()
		if len(views) != 1 {
			t.Fatalf("InspectAll want 1 view, got %d", len(views))
		}
	}
	if got := h.st.Version("svc-q"); got != rv {
		t.Fatalf("query must not bump version: %d -> %d", rv, got)
	}
	if got := h.counter.TotalCalls(); got != calls {
		t.Fatalf("query must not trigger side-effects: %d -> %d", calls, got)
	}
}

// pauseApplier 可暂停的副作用器：暂停期间 Apply 阻塞（响应 ctx 取消），
// 恢复后返回可恢复失败。用于精确观测"重试计划被取消"的瞬间。
type pauseApplier struct {
	mu     sync.Mutex
	paused bool
}

func (p *pauseApplier) setPaused(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = v
}

func (p *pauseApplier) Apply(ctx context.Context, _ ApplyRequest) error {
	for {
		p.mu.Lock()
		paused := p.paused
		p.mu.Unlock()
		if !paused {
			return Transient(errBoom)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// 重试计划一致性：挂起的重试被新写入取消后，视图不得再展示该计划；
// 新写入触发的重判产生的新计划随后可见。
func TestInspect_RetryPlanConsistentWithLatestWrite(t *testing.T) {
	pauser := &pauseApplier{}
	st := store.New(&object.Object{
		Key:  "plan",
		Spec: object.Spec{Generation: 1, DesiredItems: map[string]string{"x": "1"}},
	})
	log := newTestLogger(t)
	eng := New(Config{
		Store: st, Applier: pauser, Logger: log,
		Workers: 1, BaseDelay: 500 * time.Millisecond, MaxDelay: 2 * time.Second,
	})
	eng.Start(context.Background())
	t.Cleanup(eng.Stop)

	// 第一次失败后排定了一个 500ms 后的重试（竞态下可能已越过第 1 次，
	// 只要存在挂起的重试计划即可）。
	if !pollForT(t, 3*time.Second, func() bool {
		v := eng.Inspect("plan")
		return v.ConsecutiveFailures >= 1 && !v.NextRetryAt.IsZero()
	}) {
		t.Fatalf("expected a scheduled retry, got %+v", eng.Inspect("plan"))
	}

	// 暂停副作用器，然后外部写入新一代 Spec：
	// 挂起的 500ms 重试被新写入取代，worker 会立即重判并阻塞在 Apply 里。
	pauser.setPaused(true)
	cur, _ := st.Get("plan")
	nxt := cur.DeepCopy()
	nxt.Spec.Generation++
	if _, err := st.CAS(nxt, cur.ResourceVersion); err != nil {
		t.Fatal(err)
	}

	// worker 阻塞期间：视图不得再挂着那条已被取消的重试。
	if !pollForT(t, 3*time.Second, func() bool {
		v := eng.Inspect("plan")
		return v.NextRetryAt.IsZero() && v.ResourceVersion == nxt.ResourceVersion+1
	}) {
		t.Fatalf("cancelled retry must not linger in view: %+v", eng.Inspect("plan"))
	}
	v := eng.Inspect("plan")
	if !v.NextRetryAt.IsZero() {
		t.Fatalf("stale retry plan still visible: %v", v.NextRetryAt)
	}

	// 恢复：重判完成，产生与最新状态一致的新重试计划。
	pauser.setPaused(false)
	if !pollForT(t, 3*time.Second, func() bool {
		return !eng.Inspect("plan").NextRetryAt.IsZero()
	}) {
		t.Fatal("a fresh retry plan should appear after rejudge")
	}
}

// 等待依赖清理：owner 删除中且 child 清理持续失败时，
// 视图报告 WaitingForDependents；清理恢复后标志解除、finalizer 摘除。
func TestInspect_WaitingForDependents(t *testing.T) {
	clean := &cleanupApplier{}
	clean.failUntil.Store(1 << 20) // 清理持续可恢复失败
	h := newHarness(t, clean,
		readyOwner("owner-v"),
		childObject("cv1", "owner-v"))
	h.start()
	h.waitIdle()

	o := h.get("owner-v")
	o.MarkDeleted("2026-10-04T00:00:00Z")
	if _, err := h.st.CAS(o, o.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	h.eng.Trigger("owner-v")

	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.Inspect("owner-v")
		return v.WaitingForDependents && v.Phase == object.PhaseDeleting
	}) {
		t.Fatalf("view must report waiting for dependents: %+v", h.eng.Inspect("owner-v"))
	}

	// 外部系统恢复：清理完成，标志解除，finalizer 摘除。
	clean.failUntil.Store(0)
	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.Inspect("owner-v")
		return !v.WaitingForDependents && len(h.st.ListChildren("owner-v")) == 0
	}) {
		t.Fatal("view must clear waiting flag after dependents reclaimed")
	}
	if h.get("owner-v").HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer must be removed after cleanup")
	}
}

func pollForT(t *testing.T, d time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}
