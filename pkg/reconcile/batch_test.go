package reconcile

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// 批量演练的分类规模（总计 280 个对象，worker 只有 3 个）。
const (
	batchPlain   = 120 // 普通对象：直接收敛
	batchFlap    = 30  // 持续被外部更新
	batchFlaky   = 30  // 前 2 次可恢复失败，之后成功
	batchDoomed  = 20  // 永久可恢复失败 + 预算 2：最终放弃
	batchInvalid = 20  // 配置非法：立即终止
	batchOwners  = 20  // 删除中的 owner，各带 2 个 child
	batchKids    = 2
)

// batchInner 是批量演练的底层执行器：按脚本注入可恢复失败，统计真实调用次数。
type batchInner struct {
	mu       sync.Mutex
	calls    map[string]int // object|kind|token -> 真实下发次数
	failLeft map[string]int // 剩余可恢复失败次数
}

func newBatchInner() *batchInner {
	return &batchInner{calls: map[string]int{}, failLeft: map[string]int{}}
}

func batchID(k object.Key, kind object.SideEffectKind, token string) string {
	return string(k) + "|" + string(kind) + "|" + token
}

func (b *batchInner) failTimes(k object.Key, kind object.SideEffectKind, token string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failLeft[batchID(k, kind, token)] = n
}

func (b *batchInner) Apply(_ context.Context, req ApplyRequest) error {
	id := batchID(req.Object, req.Kind, req.Token)
	b.mu.Lock()
	b.calls[id]++
	n := b.failLeft[id]
	if n > 0 {
		b.failLeft[id] = n - 1
	}
	b.mu.Unlock()
	if n > 0 {
		return Transient(errBoom)
	}
	return nil
}

func (b *batchInner) count(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[id]
}

func (b *batchInner) total() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, c := range b.calls {
		n += c
	}
	return n
}

// batchFingerprint 是一次批量演练最终态的可观测指纹：
// 两次运行的指纹必须完全一致（结果可复现，不是偶尔碰对）。
type batchFingerprint struct {
	states       map[object.Key]string // key -> "Phase|FailureKind|GaveUp"
	innerCalls   map[string]int        // 每个副作用幂等键的真实下发次数
	totalInner   int
	childrenLeft int
}

func (f batchFingerprint) String() string {
	var sb strings.Builder
	keys := make([]string, 0, len(f.states))
	for k := range f.states {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%s\n", k, f.states[object.Key(k)])
	}
	fmt.Fprintf(&sb, "totalInner=%d childrenLeft=%d", f.totalInner, f.childrenLeft)
	return sb.String()
}

// runBatchScenario 执行一次完整的批量混合负载演练，返回最终态指纹。
func runBatchScenario(t *testing.T, runIdx int) batchFingerprint {
	t.Helper()
	inner := newBatchInner()

	var seed []*object.Object
	mkOwner := func(key object.Key, budget int) *object.Object {
		return &object.Object{
			Key: key,
			Spec: object.Spec{
				Generation:   1,
				DesiredItems: map[string]string{"item": "v"},
				RetryBudget:  budget,
			},
		}
	}
	for i := 0; i < batchPlain; i++ {
		seed = append(seed, mkOwner(object.Key(fmt.Sprintf("plain-%d", i)), 0))
	}
	for i := 0; i < batchFlap; i++ {
		seed = append(seed, mkOwner(object.Key(fmt.Sprintf("flap-%d", i)), 0))
	}
	for i := 0; i < batchFlaky; i++ {
		k := object.Key(fmt.Sprintf("flaky-%d", i))
		seed = append(seed, mkOwner(k, 0))
		inner.failTimes(k, object.EffectProvision, "item", 2)
	}
	for i := 0; i < batchDoomed; i++ {
		k := object.Key(fmt.Sprintf("doomed-%d", i))
		seed = append(seed, mkOwner(k, 2)) // 预算 2：第 3 次连续失败放弃
		inner.failTimes(k, object.EffectProvision, "item", 1<<30)
	}
	for i := 0; i < batchInvalid; i++ {
		k := object.Key(fmt.Sprintf("invalid-%d", i))
		o := mkOwner(k, 0)
		o.Spec.FailConfigured = true
		seed = append(seed, o)
	}
	for i := 0; i < batchOwners; i++ {
		k := object.Key(fmt.Sprintf("owner-%d", i))
		seed = append(seed, mkOwner(k, 0))
		for j := 0; j < batchKids; j++ {
			seed = append(seed, childObject(object.Key(fmt.Sprintf("owner-%d-c%d", i, j)), k))
		}
	}

	st := store.New(seed...)
	counter := NewCountingApplier(inner)
	eng := New(Config{
		Store:     st,
		Applier:   counter,
		Logger:    newTestLogger(t),
		Workers:   3, // 并发处理能力有限：对象数远超 worker 数
		BaseDelay: 5 * time.Millisecond,
		MaxDelay:  40 * time.Millisecond,
	})
	eng.Start(context.Background())
	defer eng.Stop()

	// 一路持续更新 flap-* 对象（约 300ms），与协调/删除交错。
	stopFlap := make(chan struct{})
	var flapWg sync.WaitGroup
	flapWg.Add(1)
	go func() {
		defer flapWg.Done()
		n := 0
		for {
			select {
			case <-stopFlap:
				return
			default:
			}
			for i := 0; i < batchFlap; i++ {
				k := object.Key(fmt.Sprintf("flap-%d", i))
				cur, err := st.Get(k)
				if err != nil {
					continue
				}
				up := cur.DeepCopy()
				up.Status.Reason = fmt.Sprintf("heartbeat-%d", n)
				_, _ = st.CAS(up, cur.ResourceVersion) // 撞车就跳过本轮
			}
			n++
			time.Sleep(3 * time.Millisecond)
		}
	}()

	// 删除请求与更新交错进行：先等全部 owner 完成初始收敛（finalizer 在位），
	// 再在系统运行 50ms 后标记删除 —— 删除的是已收敛对象，终态确定。
	if !h2pollFor(10*time.Second, func() bool {
		for i := 0; i < batchOwners; i++ {
			o, err := st.Get(object.Key(fmt.Sprintf("owner-%d", i)))
			if err != nil || !o.HasFinalizer(object.FinalizerOwnerCleanup) ||
				o.Status.Phase != object.PhaseReady {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("run %d: owners did not converge before delete", runIdx)
	}
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < batchOwners; i++ {
		k := object.Key(fmt.Sprintf("owner-%d", i))
		o, err := st.Get(k)
		if err != nil {
			t.Fatalf("owner %s missing: %v", k, err)
		}
		o.MarkDeleted("2026-10-04T00:00:00Z")
		if _, err := st.CAS(o, o.ResourceVersion); err != nil {
			t.Fatalf("mark delete %s: %v", k, err)
		}
		eng.Trigger(k)
	}

	time.Sleep(250 * time.Millisecond)
	close(stopFlap)
	flapWg.Wait()

	// 等待每个对象都到达确定终态。
	expectOK := func() bool {
		for i := 0; i < batchPlain; i++ {
			if v := eng.View(object.Key(fmt.Sprintf("plain-%d", i))); v.Phase != object.PhaseReady || v.GaveUp {
				return false
			}
		}
		for i := 0; i < batchFlap; i++ {
			if v := eng.View(object.Key(fmt.Sprintf("flap-%d", i))); v.Phase != object.PhaseReady || v.GaveUp {
				return false
			}
		}
		for i := 0; i < batchFlaky; i++ {
			if v := eng.View(object.Key(fmt.Sprintf("flaky-%d", i))); v.Phase != object.PhaseReady {
				return false
			}
		}
		for i := 0; i < batchDoomed; i++ {
			v := eng.View(object.Key(fmt.Sprintf("doomed-%d", i)))
			if !v.GaveUp || v.LastFailureKind != ReasonBudgetExhausted {
				return false
			}
		}
		for i := 0; i < batchInvalid; i++ {
			v := eng.View(object.Key(fmt.Sprintf("invalid-%d", i)))
			if v.Phase != object.PhaseFailed || v.LastFailureKind != ReasonConfig || v.GaveUp {
				return false
			}
		}
		for i := 0; i < batchOwners; i++ {
			k := object.Key(fmt.Sprintf("owner-%d", i))
			v := eng.View(k)
			if !v.Deleting || v.WaitingOnChildren {
				return false
			}
			o, err := st.Get(k)
			if err != nil || o.HasFinalizer(object.FinalizerOwnerCleanup) {
				return false
			}
		}
		return true
	}
	if !h2pollFor(20*time.Second, expectOK) {
		t.Fatalf("run %d: batch did not reach deterministic final state", runIdx)
	}
	if !eng.WaitIdle(5 * time.Second) {
		t.Fatalf("run %d: engine not idle after convergence", runIdx)
	}

	// 收敛后系统必须静止：不再有无谓的版本递增或副作用。
	versions := map[object.Key]int64{}
	for _, o := range st.List() {
		versions[o.Key] = o.ResourceVersion
	}
	totalBefore := inner.total()
	time.Sleep(120 * time.Millisecond)
	for _, o := range st.List() {
		if o.ResourceVersion != versions[o.Key] {
			t.Fatalf("run %d: object %s version moved after convergence: %d -> %d",
				runIdx, o.Key, versions[o.Key], o.ResourceVersion)
		}
	}
	if inner.total() != totalBefore {
		t.Fatalf("run %d: side-effects continued after convergence: %d -> %d",
			runIdx, totalBefore, inner.total())
	}

	// 汇总指纹。
	fp := batchFingerprint{
		states:     map[object.Key]string{},
		innerCalls: map[string]int{},
	}
	for _, o := range st.List() {
		v := eng.View(o.Key)
		fp.states[o.Key] = fmt.Sprintf("%s|%s|%v", v.Phase, v.LastFailureKind, v.GaveUp)
		if o.Owner != "" {
			fp.childrenLeft++
		}
	}
	for i := 0; i < batchPlain+batchFlap+batchFlaky+batchDoomed+batchOwners; i++ {
		var k object.Key
		switch {
		case i < batchPlain:
			k = object.Key(fmt.Sprintf("plain-%d", i))
		case i < batchPlain+batchFlap:
			k = object.Key(fmt.Sprintf("flap-%d", i-batchPlain))
		case i < batchPlain+batchFlap+batchFlaky:
			k = object.Key(fmt.Sprintf("flaky-%d", i-batchPlain-batchFlap))
		case i < batchPlain+batchFlap+batchFlaky+batchDoomed:
			k = object.Key(fmt.Sprintf("doomed-%d", i-batchPlain-batchFlap-batchFlaky))
		default:
			k = object.Key(fmt.Sprintf("owner-%d", i-batchPlain-batchFlap-batchFlaky-batchDoomed))
		}
		fp.innerCalls[string(k)] = inner.count(batchID(k, object.EffectProvision, "item"))
	}
	for i := 0; i < batchOwners; i++ {
		for j := 0; j < batchKids; j++ {
			c := object.Key(fmt.Sprintf("owner-%d-c%d", i, j))
			fp.innerCalls[string(c)] = inner.count(batchID(c, object.EffectCleanup, "owned-by-"+string(fmt.Sprintf("owner-%d", i))))
		}
	}
	fp.totalInner = inner.total()
	t.Logf("run %d fingerprint:\n%s", runIdx, fp.String())
	return fp
}

func h2pollFor(d time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}

// 批量演练：数百个对象的混合负载（持续更新、删除、反复重试、预算放弃、
// 配置非法）在有限并发下全部收敛到确定结果；同一场景连跑两遍，指纹一致。
func TestBatch_MixedLoadDeterministic(t *testing.T) {
	fp1 := runBatchScenario(t, 1)
	fp2 := runBatchScenario(t, 2)

	// 依赖对象一个不剩。
	if fp1.childrenLeft != 0 || fp2.childrenLeft != 0 {
		t.Fatalf("orphan children left: run1=%d run2=%d", fp1.childrenLeft, fp2.childrenLeft)
	}
	// 清理动作不能重复下发：每个 child 的 Cleanup 恰好 1 次。
	for i := 0; i < batchOwners; i++ {
		for j := 0; j < batchKids; j++ {
			id := fmt.Sprintf("owner-%d-c%d", i, j)
			if fp1.innerCalls[id] != 1 {
				t.Fatalf("cleanup for %s must be issued exactly once, got %d", id, fp1.innerCalls[id])
			}
		}
	}
	// 各类对象的副作用次数符合脚本预期（可复现的关键）。
	for i := 0; i < batchFlaky; i++ {
		id := fmt.Sprintf("flaky-%d", i)
		if fp1.innerCalls[id] != 3 { // 2 次失败 + 1 次成功
			t.Fatalf("flaky %s: want 3 provision calls, got %d", id, fp1.innerCalls[id])
		}
	}
	for i := 0; i < batchDoomed; i++ {
		id := fmt.Sprintf("doomed-%d", i)
		if fp1.innerCalls[id] != 3 { // 预算 2：第 3 次连续失败后放弃
			t.Fatalf("doomed %s: want 3 provision calls then give up, got %d", id, fp1.innerCalls[id])
		}
	}
	for i := 0; i < batchPlain; i++ {
		id := fmt.Sprintf("plain-%d", i)
		if fp1.innerCalls[id] != 1 {
			t.Fatalf("plain %s: want exactly 1 provision call, got %d", id, fp1.innerCalls[id])
		}
	}
	// 两次运行指纹完全一致：终态与副作用序列可复现。
	if !reflect.DeepEqual(fp1, fp2) {
		t.Fatalf("batch runs must be reproducible:\nrun1:\n%s\nrun2:\n%s", fp1.String(), fp2.String())
	}
}
