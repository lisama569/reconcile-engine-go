package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// timestampApplier 记录每个 token 每次副作用调用的时间，并始终返回可恢复失败。
type timestampApplier struct {
	mu    sync.Mutex
	times map[string][]time.Time
}

func newTimestampApplier() *timestampApplier {
	return &timestampApplier{times: map[string][]time.Time{}}
}

func (a *timestampApplier) Apply(_ context.Context, req ApplyRequest) error {
	a.mu.Lock()
	a.times[req.Token] = append(a.times[req.Token], time.Now())
	a.mu.Unlock()
	return Transient(errBoom)
}

func (a *timestampApplier) callTimes(token string) []time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]time.Time{}, a.times[token]...)
}

// 退避 1：同一对象连续可恢复失败时，相邻两次重试的等待逐次变长直到封顶；
// 中途被重新触发（Trigger）不会回到最短等待；连续失败次数与退避档位对得上。
func TestRetry_DelayEscalatesUntilCapped(t *testing.T) {
	ts := newTimestampApplier()
	const base = 20 * time.Millisecond
	const max = 160 * time.Millisecond
	h := newHarnessCfg(t, ts, func(c *Config) {
		c.BaseDelay = base
		c.MaxDelay = max
	}, ownerObject("backoff-svc", map[string]string{"flaky": "x"}))
	h.start()

	// 等够 6 次失败调用（计划等待：20,40,80,160,160ms）。
	if !h.pollFor(5*time.Second, func() bool {
		return len(ts.callTimes("flaky")) >= 6
	}) {
		t.Fatalf("expected >=6 failing calls, got %d", len(ts.callTimes("flaky")))
	}
	// 退避途中主动重新触发：会立即补一次重判（Trigger 语义），
	// 但退避档位不允许被重置回最短等待。
	h.eng.Trigger("backoff-svc")
	h.eng.Trigger("backoff-svc")
	// 等触发引起的额外重判结束、调度恢复节奏（再观察 3 次调用）。
	if !h.pollFor(3*time.Second, func() bool {
		return len(ts.callTimes("flaky")) >= 9
	}) {
		t.Fatalf("expected >=9 calls after manual triggers, got %d", len(ts.callTimes("flaky")))
	}

	times := ts.callTimes("flaky")
	var intervals []time.Duration
	for i := 1; i < len(times); i++ {
		intervals = append(intervals, times[i].Sub(times[i-1]))
	}
	t.Logf("call intervals: %v", intervals)

	// 前 4 个间隔（计划 20/40/80/160ms）必须严格变长：
	// 留 8ms 余量吸收调度抖动，但 20->40->80->160 的翻倍关系必须可观测。
	for i := 1; i < 4; i++ {
		if intervals[i] < intervals[i-1]+8*time.Millisecond {
			t.Fatalf("backoff must grow: interval[%d]=%v not > interval[%d]=%v",
				i, intervals[i], i-1, intervals[i-1])
		}
	}
	// 封顶：第 5 个间隔不得显著超过 max（允许处理耗时，但不允许再翻倍）。
	if intervals[4] > max+120*time.Millisecond {
		t.Fatalf("backoff must be capped at %v, interval[4]=%v", max, intervals[4])
	}
	// 触发风暴平息后的最后一个间隔必须仍在封顶档位：
	// 若 Trigger 把退避重置回最短等待，这里会掉到 ~20ms。
	last := intervals[len(intervals)-1]
	if last < max-10*time.Millisecond {
		t.Fatalf("manual trigger must not reset backoff to base: last interval=%v (cap=%v)", last, max)
	}
	if last > max+150*time.Millisecond {
		t.Fatalf("backoff must stay capped, last interval=%v", last)
	}

	// 连续失败次数与退避档位对得上号：视图里的计数与已失败调用数一致
	// （采样非原子，允许 1 次在途调用的误差），且下一次重试间隔不超过封顶档。
	callsBefore := len(ts.callTimes("flaky"))
	v := h.eng.View("backoff-svc")
	callsAfter := len(ts.callTimes("flaky"))
	t.Logf("view sampled between %d and %d calls: %+v", callsBefore, callsAfter, v)
	if v.ConsecutiveFailures < callsBefore-1 || v.ConsecutiveFailures > callsAfter {
		t.Fatalf("consecutive failures must track failing calls: view=%d calls in [%d,%d]",
			v.ConsecutiveFailures, callsBefore, callsAfter)
	}
	if !v.RetryScheduled {
		t.Fatal("a retry should be scheduled while failures continue")
	}
	want := Backoff(base, max, v.ConsecutiveFailures-1)
	got := time.Until(v.NextRetryAt)
	if got <= 0 || got > want+50*time.Millisecond {
		t.Fatalf("next retry delay out of range: want <=%v, view shows %v (count=%d)",
			want, got, v.ConsecutiveFailures)
	}
	if v.LastFailureKind != ReasonTransient || !v.LastFailureRetriable {
		t.Fatalf("failure classification wrong: kind=%s retriable=%v",
			v.LastFailureKind, v.LastFailureRetriable)
	}
}

// 预算 1：连续可恢复失败超出对象预算后进入"已放弃、需人工介入"：
// 不再自动重试、状态可区分，且不影响其他对象继续收敛/重试。
func TestRetryBudget_GiveUpNeedsManualIntervention(t *testing.T) {
	gate := newGateApplier()
	gate.gate("doomed")
	gate.gate("endless")
	h := newHarnessCfg(t, gate, func(c *Config) {
		c.BaseDelay = 10 * time.Millisecond
		c.MaxDelay = 40 * time.Millisecond
	}, &object.Object{
		Key: "limited-svc",
		Spec: object.Spec{
			Generation:   1,
			DesiredItems: map[string]string{"doomed": "x"},
			RetryBudget:  2, // 允许 2 次重试；第 3 次连续失败即放弃
		},
	}, ownerObject("unlimited-svc", map[string]string{"endless": "y"}))
	h.start()
	gate.release("doomed", false)
	gate.release("endless", false)

	// 等待进入放弃态。
	if !h.pollFor(3*time.Second, func() bool {
		return h.eng.View("limited-svc").GaveUp
	}) {
		t.Fatalf("limited-svc should give up after budget exhausted, view=%+v",
			h.eng.View("limited-svc"))
	}
	v := h.eng.View("limited-svc")
	t.Logf("gave-up view: %+v", v)
	if v.Phase != object.PhaseFailed || v.LastFailureRetriable {
		t.Fatalf("gave-up object must be terminally Failed, phase=%s retriable=%v",
			v.Phase, v.LastFailureRetriable)
	}
	if v.LastFailureKind != ReasonBudgetExhausted {
		t.Fatalf("failure kind must be distinguishable as BudgetExhausted, got %s",
			v.LastFailureKind)
	}
	if v.RetryScheduled {
		t.Fatal("gave-up object must not have any retry scheduled")
	}
	// 预算语义：budget=2 -> 失败第 3 次时放弃，副作用恰好下发 3 次。
	if n := gate.callCount("doomed"); n != 3 {
		t.Fatalf("budget=2 should stop after 3 consecutive failures, got %d", n)
	}

	// 放弃后保持稳定：不再自动重试、不再改写版本。
	calls := gate.callCount("doomed")
	rv := h.st.Version("limited-svc")
	time.Sleep(150 * time.Millisecond)
	if n := gate.callCount("doomed"); n != calls {
		t.Fatalf("gave-up object must stop retrying: calls %d -> %d", calls, n)
	}
	if rv2 := h.st.Version("limited-svc"); rv2 != rv {
		t.Fatalf("gave-up object must not be rewritten: rv %d -> %d", rv, rv2)
	}

	// 其他对象不受影响：无预算对象仍在持续重试。
	endless1 := gate.callCount("endless")
	time.Sleep(80 * time.Millisecond)
	if endless2 := gate.callCount("endless"); endless2 <= endless1 {
		t.Fatalf("other objects must keep retrying: endless calls %d -> %d", endless1, endless2)
	}

	// 外部系统恢复也不自动解除放弃态（必须显式重置或新配置）。
	gate.release("doomed", true)
	time.Sleep(120 * time.Millisecond)
	if !h.eng.View("limited-svc").GaveUp {
		t.Fatal("gave-up state must persist until reset or new spec")
	}

	// 运维显式重置：解除放弃态，从最小等待重新收敛。
	if err := h.eng.Reset("limited-svc"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !h.pollFor(3*time.Second, func() bool {
		return h.eng.View("limited-svc").Phase == object.PhaseReady
	}) {
		t.Fatalf("object should converge after reset, view=%+v", h.eng.View("limited-svc"))
	}
	v = h.eng.View("limited-svc")
	if v.GaveUp || v.ConsecutiveFailures != 0 {
		t.Fatalf("after reset+success: want clean state, view=%+v", v)
	}
}

// 预算 2：外部提交新一代配置同样解除放弃态（无需显式重置）。
func TestRetryBudget_NewGenerationReleases(t *testing.T) {
	gate := newGateApplier()
	gate.gate("broken")
	h := newHarnessCfg(t, gate, nil, &object.Object{
		Key: "gen-svc",
		Spec: object.Spec{
			Generation:   1,
			DesiredItems: map[string]string{"broken": "x"},
			RetryBudget:  1,
			// 第 2 次连续失败即放弃
		},
	})
	h.start()
	gate.release("broken", false)

	if !h.pollFor(3*time.Second, func() bool {
		return h.eng.View("gen-svc").GaveUp
	}) {
		t.Fatalf("expected give-up, view=%+v", h.eng.View("gen-svc"))
	}

	// 修复外部系统 + 提交新一代 Spec。
	gate.release("broken", true)
	cur := h.get("gen-svc")
	next := cur.DeepCopy()
	next.Spec.Generation++
	if _, err := h.st.CAS(next, cur.ResourceVersion); err != nil {
		t.Fatalf("bump generation: %v", err)
	}
	h.eng.Trigger("gen-svc")

	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.View("gen-svc")
		return v.Phase == object.PhaseReady && !v.GaveUp && v.ConsecutiveFailures == 0
	}) {
		t.Fatalf("new generation should release give-up and converge, view=%+v",
			h.eng.View("gen-svc"))
	}
}

// 预算 3：配置性失败仍然立即终止（不消耗预算、不重试），
// 且与"预算耗尽"在视图中可区分。
func TestRetryBudget_ConfigFailureStillTerminalImmediately(t *testing.T) {
	flaky := newFlakyApplier()
	flaky.permanentFail("cfg-svc", "rogue") // permanentFail 对 Provision 返回 InvalidConfig
	h := newHarnessCfg(t, flaky, nil, &object.Object{
		Key: "cfg-svc",
		Spec: object.Spec{
			Generation:   1,
			DesiredItems: map[string]string{"rogue": "x"},
			RetryBudget:  10, // 预算再大也不该被配置错误消耗
		},
	})
	h.start()
	h.waitIdle()

	v := h.eng.View("cfg-svc")
	t.Logf("config-failure view: %+v", v)
	if v.Phase != object.PhaseFailed || v.LastFailureRetriable {
		t.Fatalf("config failure must be terminally Failed, phase=%s retriable=%v",
			v.Phase, v.LastFailureRetriable)
	}
	if v.LastFailureKind != ReasonConfig {
		t.Fatalf("want InvalidConfig kind, got %s", v.LastFailureKind)
	}
	if v.GaveUp {
		t.Fatal("config failure must not be reported as budget give-up")
	}
	if n := flaky.calls["cfg-svc|rogue"]; n != 1 {
		t.Fatalf("config failure must stop after the first attempt, got %d calls", n)
	}
	if v.RetryScheduled {
		t.Fatal("config failure must not schedule retries")
	}
}

// 预算 4：删除路径上清理反复失败也会耗尽预算 —— 保留 finalizer 并放弃，
// 运维重置后级联清理继续完成，child 一个不剩。
func TestRetryBudget_DeletePathGiveUpAndReset(t *testing.T) {
	clean := &cleanupApplier{}
	clean.failUntil.Store(1 << 30) // cleanup 永远可恢复失败
	h := newHarnessCfg(t, clean, nil,
		&object.Object{
			Key:        "del-owner",
			Finalizers: []string{object.FinalizerOwnerCleanup},
			Spec:       object.Spec{Generation: 1, RetryBudget: 2},
			Status: object.Status{
				Phase:              object.PhaseReady,
				ReadyItems:         map[string]string{},
				ObservedGeneration: 1,
			},
		},
		childObject("del-c1", "del-owner"))
	h.start()
	h.waitIdle()

	o := h.get("del-owner")
	o.MarkDeleted("2026-10-04T00:00:00Z")
	if _, err := h.st.CAS(o, h.st.Version("del-owner")); err != nil {
		t.Fatal(err)
	}
	h.eng.Trigger("del-owner")

	// 预算耗尽：放弃但仍持有 finalizer、child 仍在（等待人工介入）。
	if !h.pollFor(3*time.Second, func() bool {
		v := h.eng.View("del-owner")
		return v.GaveUp && v.Deleting
	}) {
		t.Fatalf("delete path should give up after budget exhausted, view=%+v",
			h.eng.View("del-owner"))
	}
	v := h.eng.View("del-owner")
	t.Logf("delete give-up view: %+v", v)
	if !v.WaitingOnChildren {
		t.Fatal("view must show it is still waiting on children cleanup")
	}
	if !h.get("del-owner").HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer must be kept while cleanup is incomplete")
	}
	if len(h.st.ListChildren("del-owner")) != 1 {
		t.Fatal("child must remain while cleanup keeps failing")
	}
	attempts := clean.attempts.Load()
	time.Sleep(120 * time.Millisecond)
	if clean.attempts.Load() != attempts {
		t.Fatal("gave-up delete must stop issuing cleanup side-effects")
	}

	// 运维介入：恢复外部系统 + 显式重置 -> 级联完成，child 一个不剩。
	clean.failUntil.Store(0)
	if err := h.eng.Reset("del-owner"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !h.pollFor(3*time.Second, func() bool {
		return len(h.st.ListChildren("del-owner")) == 0 &&
			!h.get("del-owner").HasFinalizer(object.FinalizerOwnerCleanup)
	}) {
		t.Fatalf("cascade should finish after reset, children=%d finalizers=%v",
			len(h.st.ListChildren("del-owner")), h.get("del-owner").Finalizers)
	}
	v = h.eng.View("del-owner")
	if v.GaveUp || v.WaitingOnChildren {
		t.Fatalf("view must be clean after cascade finished: %+v", v)
	}
}
