package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// flakyApplier 支持按 (object|token) 注入"前 N 次可恢复失败，之后成功"
// 或"永远失败"；线程安全，可被并发 worker 调用。
type flakyApplier struct {
	mu       sync.Mutex
	failLeft map[string]int // 剩余可恢复失败次数
	permFail map[string]bool
	calls    map[string]int
}

func newFlakyApplier() *flakyApplier {
	return &flakyApplier{
		failLeft: map[string]int{},
		permFail: map[string]bool{},
		calls:    map[string]int{},
	}
}

func (f *flakyApplier) failTimes(k object.Key, token string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failLeft[string(k)+"|"+token] = n
}

func (f *flakyApplier) permanentFail(k object.Key, token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.permFail[string(k)+"|"+token] = true
}

func (f *flakyApplier) Apply(_ context.Context, req ApplyRequest) error {
	id := string(req.Object) + "|" + req.Token
	f.mu.Lock()
	f.calls[id]++
	if f.permFail[id] {
		f.mu.Unlock()
		if req.Kind == object.EffectCleanup {
			return Transient(errBoom)
		}
		return InvalidConfig("permanently bad item")
	}
	if n := f.failLeft[id]; n > 0 {
		f.failLeft[id] = n - 1
		f.mu.Unlock()
		return Transient(errBoom)
	}
	f.mu.Unlock()
	return nil
}

// gateApplier 对指定 token 阻塞调用，直到测试显式放行（Release）。
// 放行后每次调用按当前模式返回成功或可恢复错误（release 可重复切换）；
// 用于精确制造"部分项已成功、另一项可恢复失败、稍后恢复"的时序。
type gateApplier struct {
	mu      sync.Mutex
	gates   map[string]chan struct{}
	succeed map[string]bool
	calls   map[string]int
}

func newGateApplier() *gateApplier {
	return &gateApplier{
		gates:   map[string]chan struct{}{},
		succeed: map[string]bool{},
		calls:   map[string]int{},
	}
}

// gate 安装一个初始为"失败模式"的阻塞闸门。
func (g *gateApplier) gate(token string) {
	g.mu.Lock()
	g.gates[token] = make(chan struct{})
	g.succeed[token] = false
	g.mu.Unlock()
}

// release 打开闸门并设定放行后的返回模式（true=成功，false=可恢复失败）。
// 可重复调用以在失败/成功之间切换。
func (g *gateApplier) release(token string, succeed bool) {
	g.mu.Lock()
	g.succeed[token] = succeed
	ch := g.gates[token]
	opened := false
	select {
	case <-ch:
		opened = true
	default:
	}
	g.mu.Unlock()
	if !opened {
		close(ch)
	}
}

func (g *gateApplier) Apply(_ context.Context, req ApplyRequest) error {
	g.mu.Lock()
	g.calls[req.Token]++
	ch, gated := g.gates[req.Token]
	g.mu.Unlock()
	if gated {
		<-ch // 闸门打开后，后续调用也立即通过并读取当前模式
		g.mu.Lock()
		ok := g.succeed[req.Token]
		g.mu.Unlock()
		if !ok {
			return Transient(errBoom)
		}
	}
	return nil
}

func (g *gateApplier) callCount(token string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[token]
}

func ownerObject(key object.Key, items map[string]string) *object.Object {
	return &object.Object{
		Key:  key,
		Spec: object.Spec{DesiredItems: items},
	}
}

// 场景 1：重复协调幂等 —— 副作用只下发一次，已收敛后版本不再被改写。
func TestReconcile_IdempotentNoDuplicateSideEffects(t *testing.T) {
	flaky := newFlakyApplier()
	h := newHarness(t, flaky,
		ownerObject("svc-a", map[string]string{"bucket": "b1", "topic": "t1"}))
	h.start()
	h.waitIdle()

	svc := h.get("svc-a")
	if svc.Status.Phase != object.PhaseReady {
		t.Fatalf("want Ready, got %s reason=%q", svc.Status.Phase, svc.Status.Reason)
	}
	if len(svc.Status.ReadyItems) != 2 {
		t.Fatalf("want 2 ready items, got %v", svc.Status.ReadyItems)
	}
	if !svc.HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("owner should carry cleanup finalizer")
	}
	// 每个 token 的真实副作用恰好 1 次（Provision）。
	if h.counter.CallCount("bucket") != 1 || h.counter.CallCount("topic") != 1 {
		t.Fatalf("each side-effect must apply once, calls: bucket=%d topic=%d",
			h.counter.CallCount("bucket"), h.counter.CallCount("topic"))
	}
	rvAfterFirstConverge := h.st.Version("svc-a")

	// 再次触发同一对象的重复协调：不应再产生任何写入与副作用。
	h.eng.Trigger("svc-a")
	h.waitIdle()
	time.Sleep(20 * time.Millisecond) // 留出"万一写错"的窗口
	if rv := h.st.Version("svc-a"); rv != rvAfterFirstConverge {
		t.Fatalf("converged object must not be rewritten: rv changed %d -> %d",
			rvAfterFirstConverge, rv)
	}
	if total := h.counter.TotalCalls(); total != 2 {
		t.Fatalf("repeated reconcile must not duplicate side-effects, total calls=%d", total)
	}

	// 第三次重复触发，结论一致。
	h.eng.Trigger("svc-a")
	h.waitIdle()
	if total := h.counter.TotalCalls(); total != 2 {
		t.Fatalf("third reconcile still must be side-effect free, total calls=%d", total)
	}
}

// 场景 2：可恢复失败按退避重试，最终收敛；失败期间保留上次成功写入的状态。
func TestReconcile_RetryableFailureBackoffKeepsLastGoodState(t *testing.T) {
	gate := newGateApplier()
	// "topic" 初始被闸门挡住；"bucket" 不受闸门控制、立即成功。
	gate.gate("topic")
	h := newHarness(t, gate,
		ownerObject("svc-b", map[string]string{"bucket": "b1", "topic": "t1"}))
	h.start()

	// 放行 topic 但让它先以"可恢复失败"返回，制造稳定可观测的中间态：
	// bucket 已保留在 ReadyItems，topic 尚缺，整体 Failed/Retriable。
	if !h.pollFor(2*time.Second, func() bool {
		return gate.callCount("topic") >= 1
	}) {
		t.Fatal("topic apply was never invoked")
	}
	gate.release("topic", false)

	observedPartial := h.pollFor(2*time.Second, func() bool {
		o := h.get("svc-b")
		_, bucketOK := o.Status.ReadyItems["bucket"]
		return bucketOK && o.Status.Phase == object.PhaseFailed && o.Status.Retriable
	})
	if !observedPartial {
		o := h.get("svc-b")
		t.Fatalf("expected transient Failed state retaining bucket, got phase=%s ready=%v retriable=%v",
			o.Status.Phase, o.Status.ReadyItems, o.Status.Retriable)
	}
	partial := h.get("svc-b")
	if _, ok := partial.Status.ReadyItems["bucket"]; !ok {
		t.Fatal("last-successful item 'bucket' must be retained across retries")
	}
	if partial.Status.Reason == "" {
		t.Fatal("retryable failure should carry a reason")
	}
	bucketCallsAtPartial := h.counter.CallCount("bucket")
	if bucketCallsAtPartial != 1 {
		t.Fatalf("ready item must not be reapplied during retries, bucket calls=%d", bucketCallsAtPartial)
	}

	// 外部系统恢复：切换为成功，退避重试最终收敛。
	gate.release("topic", true)
	if !h.pollFor(3*time.Second, func() bool {
		return h.get("svc-b").Status.Phase == object.PhaseReady
	}) {
		t.Fatalf("did not converge after recovery: %+v", h.get("svc-b").Status)
	}
	h.waitIdle()
	final := h.get("svc-b")
	if len(final.Status.ReadyItems) != 2 {
		t.Fatalf("want both items ready, got %v", final.Status.ReadyItems)
	}
	// bucket 全程只成功下发 1 次（重试不重放已成功项）；topic 经历失败后成功。
	if h.counter.CallCount("bucket") != 1 {
		t.Fatalf("already-ready item must not be reapplied, bucket calls=%d",
			h.counter.CallCount("bucket"))
	}
	if n := h.counter.CallCount("topic"); n < 2 {
		t.Fatalf("topic should be retried after transient failure then succeed, calls=%d", n)
	}
}

// 场景 3：不可恢复的配置错误终止重试，给出可区分原因，且不影响其他对象。
func TestReconcile_InvalidConfigTerminalAndIsolated(t *testing.T) {
	flaky := newFlakyApplier()
	flaky.permanentFail("bad-svc", "rogue")
	h := newHarness(t, flaky,
		ownerObject("bad-svc", map[string]string{"rogue": "x"}),
		ownerObject("good-svc", map[string]string{"ok-item": "y"}))
	h.start()
	h.waitIdle()

	bad := h.get("bad-svc")
	if bad.Status.Phase != object.PhaseFailed {
		t.Fatalf("bad object want Failed, got %s", bad.Status.Phase)
	}
	if bad.Status.Retriable {
		t.Fatal("invalid config failure must not be marked retriable")
	}
	if bad.Status.Reason == "" {
		t.Fatal("terminal failure must carry a distinguishable reason")
	}
	good := h.get("good-svc")
	if good.Status.Phase != object.PhaseReady {
		t.Fatalf("one object's failure must not block others: good-svc phase=%s reason=%q",
			good.Status.Phase, good.Status.Reason)
	}
	if h.counter.CallCount("ok-item") != 1 {
		t.Fatalf("good item should apply once, got %d", h.counter.CallCount("ok-item"))
	}

	// 终止后不再重试：rogue 的调用次数与 bad-svc 版本在一段窗口内保持稳定。
	calls := h.counter.CallCount("rogue")
	rv := h.st.Version("bad-svc")
	time.Sleep(120 * time.Millisecond)
	if h.counter.CallCount("rogue") != calls {
		t.Fatalf("terminal failure must stop retrying, calls changed %d -> %d",
			calls, h.counter.CallCount("rogue"))
	}
	if h.st.Version("bad-svc") != rv {
		t.Fatal("terminally failed object must not keep being rewritten")
	}
}

// 场景 3b：对象 Spec 自身声明 FailConfigured，直接判定不可恢复。
func TestReconcile_SpecFlagInvalidConfig(t *testing.T) {
	h := newHarness(t, ApplierFunc(func(context.Context, ApplyRequest) error { return nil }),
		&object.Object{
			Key:  "flag-bad",
			Spec: object.Spec{FailConfigured: true, DesiredItems: map[string]string{"a": "1"}},
		})
	h.start()
	h.waitIdle()
	o := h.get("flag-bad")
	if o.Status.Phase != object.PhaseFailed || o.Status.Retriable {
		t.Fatalf("want terminal Failed, got phase=%s retriable=%v", o.Status.Phase, o.Status.Retriable)
	}
	if h.counter.TotalCalls() != 0 {
		t.Fatalf("invalid spec must not trigger side-effects, calls=%d", h.counter.TotalCalls())
	}
	rv := h.st.Version("flag-bad")
	time.Sleep(80 * time.Millisecond)
	if h.st.Version("flag-bad") != rv {
		t.Fatal("terminal flag-failed object must not be rewritten")
	}
}
