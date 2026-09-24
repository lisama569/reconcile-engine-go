package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// racingApplier 在首次处理 token "mover" 时，以"外部写入方"身份
// 基于最新版本推进对象（并发更新），使协调器本轮的状态写入必然撞版本。
// 协调器必须放弃过期快照、立即以最新版本重判并最终收敛。
type racingApplier struct {
	mu      sync.Mutex
	st      storeLike
	mutated bool
	calls   int
}

type storeLike interface {
	Get(k object.Key) (*object.Object, error)
	CAS(o *object.Object, rv int64) (*object.Object, error)
}

func (a *racingApplier) Apply(_ context.Context, req ApplyRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if req.Token == "mover" && !a.mutated {
		a.mutated = true
		cur, err := a.st.Get(req.Object)
		if err != nil {
			return err
		}
		// 外部写入者：在协调器 Apply 成功后、写 Status 前推进版本。
		// 仅推进一个与协调无关的注解字段，模拟并发变更。
		nxt := cur.DeepCopy()
		nxt.Spec.DesiredItems = map[string]string{"mover": "v1"}
		if _, err := a.st.CAS(nxt, cur.ResourceVersion); err != nil {
			return err
		}
	}
	return nil
}

func TestConflict_RejudgeWithLatestVersion(t *testing.T) {
	// 先用 nil 构造，再回填 store（harness 内部创建 store）。
	racer := &racingApplier{}
	h := newHarness(t, racer,
		ownerObject("race-obj", map[string]string{"mover": "v1"}))
	racer.st = h.st
	h.start()

	if !h.pollFor(4*time.Second, func() bool {
		o := h.get("race-obj")
		return o.Status.Phase == object.PhaseReady && len(o.Status.ReadyItems) == 1
	}) {
		o := h.get("race-obj")
		t.Fatalf("did not converge after mid-reconcile version change: %+v", o.Status)
	}
	h.waitIdle()

	o := h.get("race-obj")
	if _, ok := o.Status.ReadyItems["mover"]; !ok {
		t.Fatalf("latest state must win, ReadyItems=%v", o.Status.ReadyItems)
	}
	if !o.HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer must still be present after conflict rejudge")
	}
	// mover 最多经历：首轮成功（外部同时改版本）+ 重判轮跳过（已 Ready），
	// 不允许因为冲突把成功的副作用重放多次。
	if n := h.counter.CallCount("mover"); n > 2 {
		t.Fatalf("conflict must not cause duplicate side-effects, mover calls=%d", n)
	}
}

// 场景补充：外部在终结失败后提交"新一代"Spec 修复配置，
// 引擎必须解除粘滞、重新判定并收敛。
func TestTerminalFailure_RecoversAfterNewGeneration(t *testing.T) {
	flaky := newFlakyApplier()
	flaky.permanentFail("fix-me", "rogue")
	h := newHarness(t, flaky,
		ownerObject("fix-me", map[string]string{"rogue": "x"}))
	h.start()
	h.waitIdle()
	bad := h.get("fix-me")
	if bad.Status.Phase != object.PhaseFailed || bad.Status.Retriable {
		t.Fatalf("want terminal failure first, got %+v", bad.Status)
	}

	// 外部修复配置：换一个不再失败的 token，并递增 generation。
	fixed := bad.DeepCopy()
	fixed.Spec = object.Spec{
		Generation:   2,
		DesiredItems: map[string]string{"healthy": "y"},
	}
	// 让 flaky 不再拦截（rogue 已从 spec 中移除）。
	stored, err := h.st.CAS(fixed, bad.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	h.eng.Trigger("fix-me")

	if !h.pollFor(4*time.Second, func() bool {
		o, err := h.st.Get("fix-me")
		return err == nil && o.Status.Phase == object.PhaseReady &&
			o.Status.ObservedGeneration == stored.Spec.Generation
	}) {
		t.Fatalf("object should recover with new generation: %+v", h.get("fix-me").Status)
	}
}
