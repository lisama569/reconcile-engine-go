package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// switchApplier 按对象切换"可恢复失败/成功"，线程安全。
type switchApplier struct {
	mu      sync.Mutex
	failing map[object.Key]bool
	calls   map[object.Key]int
}

func newSwitchApplier() *switchApplier {
	return &switchApplier{failing: map[object.Key]bool{}, calls: map[object.Key]int{}}
}

func (s *switchApplier) setFailing(k object.Key, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing[k] = fail
}

func (s *switchApplier) Apply(_ context.Context, req ApplyRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[req.Object]++
	if s.failing[req.Object] {
		return Transient(errBoom)
	}
	return nil
}

func (s *switchApplier) callsFor(k object.Key) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[k]
}

// 预算耗尽：对象进入可区分的 Abandoned 状态、不再自动重试、
// 版本与副作用调用都冻结；同引擎其他对象不受影响。
func TestBackoff_BudgetExhaustedAbandonsAndStops(t *testing.T) {
	sw := newSwitchApplier()
	sw.setFailing("limited", true)
	h := newHarness(t, sw,
		&object.Object{
			Key: "limited",
			Spec: object.Spec{Generation: 1, RetryBudget: 3,
				DesiredItems: map[string]string{"item": "v"}},
		},
		ownerObject("unrelated", map[string]string{"ok": "v"}))
	h.start()

	// 等到放弃态：连续失败 4 次（预算 3 次重试用尽）。
	if !h.pollFor(5*time.Second, func() bool {
		return h.get("limited").Status.Phase == object.PhaseAbandoned
	}) {
		t.Fatalf("object should be abandoned after budget exhausted, got %s",
			h.get("limited").Status.Phase)
	}
	h.waitIdle()

	v := h.eng.Inspect("limited")
	if !v.Abandoned || v.FailureClass != ReasonBudgetExhausted {
		t.Fatalf("view must show abandoned/budget-exhausted: %+v", v)
	}
	if !v.NextRetryAt.IsZero() {
		t.Fatalf("abandoned object must have no pending retry, got %v", v.NextRetryAt)
	}
	if v.ConsecutiveFailures != 4 {
		t.Fatalf("want 4 consecutive failures (budget 3 + 1), got %d", v.ConsecutiveFailures)
	}

	// 冻结观测：不再自动重试 —— 调用次数与版本在长窗口内保持稳定。
	calls := sw.callsFor("limited")
	rv := h.st.Version("limited")
	time.Sleep(5 * 40 * time.Millisecond) // 远大于最大退避 40ms
	if got := sw.callsFor("limited"); got != calls {
		t.Fatalf("abandoned object must stop retrying: calls %d -> %d", calls, got)
	}
	if got := h.st.Version("limited"); got != rv {
		t.Fatalf("abandoned object must not be rewritten: rv %d -> %d", rv, got)
	}

	// 其他对象正常收敛，不受放弃对象影响。
	if got := h.get("unrelated").Status.Phase; got != object.PhaseReady {
		t.Fatalf("unrelated object must converge, got %s", got)
	}
}

// 退避可解释：视图中连续失败次数与退避档位严格对应且逐次变长（封顶）。
func TestBackoff_ViewShowsGrowingBackoffMatchingFailureCount(t *testing.T) {
	sw := newSwitchApplier()
	sw.setFailing("grow", true)
	st := store.New(&object.Object{
		Key: "grow",
		Spec: object.Spec{Generation: 1, RetryBudget: 6,
			DesiredItems: map[string]string{"item": "v"}},
	})
	// 自定义较大的退避参数，让每个档位的观测窗口足够宽（-race 下也稳定）。
	eng := New(Config{
		Store: st, Applier: sw, Logger: newTestLogger(t),
		Workers: 1, BaseDelay: 20 * time.Millisecond, MaxDelay: 320 * time.Millisecond,
	})
	eng.Start(context.Background())
	t.Cleanup(eng.Stop)

	base, maxDelay := 20*time.Millisecond, 320*time.Millisecond
	// 采集 (连续失败次数 -> 退避档位)，直到预算耗尽。
	seen := map[int]time.Duration{}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v := eng.Inspect("grow")
		if v.ConsecutiveFailures > 0 && !v.Abandoned {
			if _, ok := seen[v.ConsecutiveFailures]; !ok {
				seen[v.ConsecutiveFailures] = v.CurrentBackoff
			}
		}
		if v.Abandoned {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(seen) < 3 {
		t.Fatalf("expected to observe >=3 backoff tiers, got %v", seen)
	}
	var prev time.Duration
	for n := 1; n <= 6; n++ {
		d, ok := seen[n]
		if !ok {
			continue // 该档位窗口太短未采到，跳过
		}
		if want := Backoff(base, maxDelay, n-1); d != want {
			t.Fatalf("failures=%d: backoff tier %v, want %v (count/tier mismatch)", n, d, want)
		}
		if d < prev {
			t.Fatalf("backoff must never shrink as failures grow: tier %d = %v < %v", n, d, prev)
		}
		prev = d
	}
	// 最终进入放弃态，且计数到预算+1。
	if !pollForT(t, 2*time.Second, func() bool { return eng.Inspect("grow").Abandoned }) {
		t.Fatal("object should be abandoned after budget exhausted")
	}
	if got := eng.Inspect("grow").ConsecutiveFailures; got != 7 {
		t.Fatalf("want 7 consecutive failures (budget 6 + 1), got %d", got)
	}
}

// 放弃态的两种解除方式：新一代 Spec、运维显式重置；解除后从最小等待重新开始。
func TestBackoff_AbandonedReleasedByNewGenerationAndReset(t *testing.T) {
	sw := newSwitchApplier()
	sw.setFailing("obj", true)
	h := newHarness(t, sw,
		&object.Object{
			Key: "obj",
			Spec: object.Spec{Generation: 1, RetryBudget: 2,
				DesiredItems: map[string]string{"item": "v"}},
		})
	h.start()
	if !h.pollFor(5*time.Second, func() bool {
		return h.get("obj").Status.Phase == object.PhaseAbandoned
	}) {
		t.Fatal("object should abandon with budget=2")
	}

	// 方式一：外部修复并提交新一代 Spec —— 粘滞解除，重新收敛。
	sw.setFailing("obj", false)
	cur := h.get("obj")
	nxt := cur.DeepCopy()
	nxt.Spec.Generation++
	if _, err := h.st.CAS(nxt, cur.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	if !h.pollFor(3*time.Second, func() bool {
		return h.get("obj").Status.Phase == object.PhaseReady
	}) {
		t.Fatalf("new generation must release abandoned state, got %s",
			h.get("obj").Status.Phase)
	}
	v := h.eng.Inspect("obj")
	if v.Abandoned || v.ConsecutiveFailures != 0 || v.CurrentBackoff != 0 {
		t.Fatalf("success must reset backoff view: %+v", v)
	}

	// 再次失败到放弃（新一代预算独立计数；新增一个未就绪项才会触发副作用）。
	sw.setFailing("obj", true)
	cur = h.get("obj")
	nxt = cur.DeepCopy()
	nxt.Spec.Generation++
	nxt.Spec.DesiredItems["item2"] = "v2"
	if _, err := h.st.CAS(nxt, cur.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	if !h.pollFor(5*time.Second, func() bool {
		return h.get("obj").Status.Phase == object.PhaseAbandoned
	}) {
		t.Fatal("object should abandon again under new generation")
	}

	// 方式二：运维显式重置 —— 解除放弃态，重试从零重新开始：
	// 仍在失败时会"再次"走到放弃（这本身就证明重试已恢复），
	// 且连续失败计数重新从 0 累计（预算 2 => 恰好 3 次后放弃）。
	h.eng.ResetRetry("obj")
	if !h.pollFor(5*time.Second, func() bool {
		return h.get("obj").Status.Phase == object.PhaseAbandoned
	}) {
		t.Fatal("object should re-abandon after reset while still failing")
	}
	if got := h.eng.Inspect("obj").ConsecutiveFailures; got != 3 {
		t.Fatalf("after reset, failures must recount from zero: want 3, got %d", got)
	}

	// 修复外部系统后再重置一次：对象重新收敛到 Ready。
	sw.setFailing("obj", false)
	h.eng.ResetRetry("obj")
	if !h.pollFor(3*time.Second, func() bool {
		return h.get("obj").Status.Phase == object.PhaseReady
	}) {
		t.Fatal("object must converge after fix + reset")
	}
}

// 配置性失败不消耗重试预算、不经退避，仍然立即终止（原有行为不退化）。
func TestBackoff_ConfigFailureBypassesBudget(t *testing.T) {
	h := newHarness(t, ApplierFunc(func(context.Context, ApplyRequest) error {
		return InvalidConfig("bad item")
	}),
		&object.Object{
			Key: "cfg-bad",
			Spec: object.Spec{Generation: 1, RetryBudget: 10,
				DesiredItems: map[string]string{"x": "1"}},
		})
	h.start()
	h.waitIdle()

	o := h.get("cfg-bad")
	if o.Status.Phase != object.PhaseFailed || o.Status.Retriable {
		t.Fatalf("config failure must be terminal Failed, got %s retriable=%v",
			o.Status.Phase, o.Status.Retriable)
	}
	v := h.eng.Inspect("cfg-bad")
	if v.FailureClass != ReasonConfig {
		t.Fatalf("view must classify config failure, got %s", v.FailureClass)
	}
	if v.Abandoned {
		t.Fatal("config failure must not be reported as abandoned")
	}
	if v.ConsecutiveFailures != 0 {
		t.Fatalf("config failure must not consume retry budget, failures=%d",
			v.ConsecutiveFailures)
	}
	if n := h.counter.TotalCalls(); n != 1 {
		t.Fatalf("config failure must stop after first attempt, calls=%d", n)
	}
}
