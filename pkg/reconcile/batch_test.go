package reconcile

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// drillApplier 是批量演练的内层副作用器（真实副作用计数在这里）：
//   - B 组：每对象前 N 次 Provision 可恢复失败；
//   - E 组：Provision 永久可恢复失败（耗尽预算）；
//   - C 组：统计每个 child 的 Cleanup 次数。
type drillApplier struct {
	mu             sync.Mutex
	failLeft       map[object.Key]int
	permFail       map[object.Key]bool
	provisionCalls map[string]int // object|token -> 内层调用次数
	cleanupCalls   map[object.Key]int
	totalProvision int
	totalCleanup   int
}

func newDrillApplier() *drillApplier {
	return &drillApplier{
		failLeft:       map[object.Key]int{},
		permFail:       map[object.Key]bool{},
		provisionCalls: map[string]int{},
		cleanupCalls:   map[object.Key]int{},
	}
}

func (d *drillApplier) Apply(_ context.Context, req ApplyRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if req.Kind == object.EffectCleanup {
		d.totalCleanup++
		d.cleanupCalls[req.Object]++
		return nil
	}
	d.totalProvision++
	d.provisionCalls[string(req.Object)+"|"+req.Token]++
	if d.permFail[req.Object] {
		return Transient(errBoom)
	}
	if d.failLeft[req.Object] > 0 {
		d.failLeft[req.Object]--
		return Transient(errBoom)
	}
	return nil
}

func (d *drillApplier) provisionsFor(k object.Key, token string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.provisionCalls[string(k)+"|"+token]
}

func (d *drillApplier) cleanupsFor(k object.Key) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cleanupCalls[k]
}

func (d *drillApplier) totals() (provision, cleanup int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.totalProvision, d.totalCleanup
}

// drillSummary 是一轮演练的可观测终态摘要，用于跨轮次比对一致性。
type drillSummary struct {
	provisionCalls int
	cleanupCalls   int
	ready          int
	abandoned      int
	deletingDone   int // finalizer 已摘除的 owner 数
	orphans        int // 残留的 child 数
	stuck          int // 未进入任何确定终态的对象数
}

const (
	drillA = 50 // 普通对象
	drillB = 30 // 反复重试后成功
	drillC = 25 // 级联删除 owner（各 2 个 child）
	drillD = 25 // 持续被更新
	drillE = 10 // 预算耗尽放弃
)

// runBatchDrill 执行一轮混合负载演练并返回终态摘要。
// 场景：A 收敛 / B 重试 / C 级联删除 / D 并发更新 / E 预算耗尽，
// 全部共享 3 个 worker 的有限并发。
func runBatchDrill(t *testing.T) drillSummary {
	t.Helper()
	drill := newDrillApplier()

	var seed []*object.Object
	// A：普通对象。
	for i := 0; i < drillA; i++ {
		seed = append(seed, &object.Object{
			Key:  object.Key(fmt.Sprintf("a-%02d", i)),
			Spec: object.Spec{Generation: 1, DesiredItems: map[string]string{"item": "v"}},
		})
	}
	// B：前 i%3 次可恢复失败。
	for i := 0; i < drillB; i++ {
		k := object.Key(fmt.Sprintf("b-%02d", i))
		drill.failLeft[k] = i % 3
		seed = append(seed, &object.Object{
			Key:  k,
			Spec: object.Spec{Generation: 1, DesiredItems: map[string]string{"item": "v"}},
		})
	}
	// C：owner（稍后在收敛后统一标记删除）+ 各 2 个 child。
	for i := 0; i < drillC; i++ {
		ok := object.Key(fmt.Sprintf("c-%02d", i))
		seed = append(seed, &object.Object{
			Key:        ok,
			Finalizers: []string{object.FinalizerOwnerCleanup},
			Spec:       object.Spec{Generation: 1},
		})
		for j := 0; j < 2; j++ {
			seed = append(seed, &object.Object{
				Key:   object.Key(fmt.Sprintf("c-%02d-ch%d", i, j)),
				Owner: ok,
			})
		}
	}
	// D：将被并发更新 4 代，每代新增一个期望项。
	for i := 0; i < drillD; i++ {
		seed = append(seed, &object.Object{
			Key:  object.Key(fmt.Sprintf("d-%02d", i)),
			Spec: object.Spec{Generation: 1, DesiredItems: map[string]string{"d0": "v"}},
		})
	}
	// E：永久可恢复失败，预算 2（第 3 次连续失败后放弃）。
	for i := 0; i < drillE; i++ {
		k := object.Key(fmt.Sprintf("e-%02d", i))
		drill.permFail[k] = true
		seed = append(seed, &object.Object{
			Key: k,
			Spec: object.Spec{Generation: 1, RetryBudget: 2,
				DesiredItems: map[string]string{"item": "v"}},
		})
	}

	st := store.New(seed...)
	counter := NewCountingApplier(drill)
	eng := New(Config{
		Store: st, Applier: counter, Logger: newTestLogger(t),
		Workers: 3, BaseDelay: 2 * time.Millisecond, MaxDelay: 20 * time.Millisecond,
	})
	eng.Start(context.Background())
	defer eng.Stop()

	// D 组并发更新：每对象推进 4 代，每代新增一个期望项。
	var updaters sync.WaitGroup
	for i := 0; i < drillD; i++ {
		k := object.Key(fmt.Sprintf("d-%02d", i))
		updaters.Add(1)
		go func() {
			defer updaters.Done()
			for g := 1; g <= 4; g++ {
				for {
					cur, err := st.Get(k)
					if err != nil {
						break
					}
					nxt := cur.DeepCopy()
					nxt.Spec.Generation = int64(g + 1)
					nxt.Spec.DesiredItems[fmt.Sprintf("d%d", g)] = "v"
					if _, err := st.CAS(nxt, cur.ResourceVersion); err == nil {
						break
					}
					time.Sleep(time.Millisecond) // 与引擎写入撞版本，重试
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	// 第一阶段：A/B/E 收敛到各自终态，C 的 child 先收敛（版本稳定），
	// 保证后续级联删除中 cleanup 对每个 child 恰好下发一次。
	if !eng.WaitIdle(15 * time.Second) {
		t.Fatalf("phase 1 did not settle: queued=%d inflight=%d",
			eng.queue.Len(), eng.queue.Inflight())
	}

	// 请求删除全部 C owner（软删除）。显式 Trigger 使入队与
	// 后续 WaitIdle 之间有时序保证（notify 通道投递是异步的）。
	for i := 0; i < drillC; i++ {
		k := object.Key(fmt.Sprintf("c-%02d", i))
		o, err := st.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		o.MarkDeleted("2026-10-04T00:00:00Z")
		if _, err := st.CAS(o, o.ResourceVersion); err != nil {
			t.Fatal(err)
		}
		eng.Trigger(k)
	}

	// D 组更新全部落地后，显式触发最终代际的重判。
	updaters.Wait()
	for i := 0; i < drillD; i++ {
		eng.Trigger(object.Key(fmt.Sprintf("d-%02d", i)))
	}
	if !eng.WaitIdle(15 * time.Second) {
		t.Fatalf("phase 2 did not settle: queued=%d inflight=%d",
			eng.queue.Len(), eng.queue.Inflight())
	}

	// ---------- 终态断言（全部落在可观测事实上） ----------
	var sum drillSummary
	sum.provisionCalls, sum.cleanupCalls = drill.totals()

	for i := 0; i < drillA; i++ {
		k := object.Key(fmt.Sprintf("a-%02d", i))
		o, err := st.Get(k)
		if err != nil || o.Status.Phase != object.PhaseReady {
			t.Fatalf("A object %s not Ready: %v", k, err)
		}
		if n := drill.provisionsFor(k, "item"); n != 1 {
			t.Fatalf("A object %s provisioned %d times, want exactly 1", k, n)
		}
		sum.ready++
	}
	for i := 0; i < drillB; i++ {
		k := object.Key(fmt.Sprintf("b-%02d", i))
		o, err := st.Get(k)
		if err != nil || o.Status.Phase != object.PhaseReady {
			t.Fatalf("B object %s not Ready: %v", k, err)
		}
		if want := i%3 + 1; drill.provisionsFor(k, "item") != want {
			t.Fatalf("B object %s provision calls=%d, want %d (failures+1)",
				k, drill.provisionsFor(k, "item"), want)
		}
		sum.ready++
	}
	for i := 0; i < drillD; i++ {
		k := object.Key(fmt.Sprintf("d-%02d", i))
		o, err := st.Get(k)
		if err != nil || o.Status.Phase != object.PhaseReady {
			t.Fatalf("D object %s not Ready: %v", k, err)
		}
		if len(o.Status.ReadyItems) != 5 {
			t.Fatalf("D object %s ready=%v, want 5 items (d0..d4)", k, o.Status.ReadyItems)
		}
		for g := 0; g <= 4; g++ {
			tok := fmt.Sprintf("d%d", g)
			if _, ok := o.Status.ReadyItems[tok]; !ok {
				t.Fatalf("D object %s missing ready token %s", k, tok)
			}
			if n := drill.provisionsFor(k, tok); n != 1 {
				t.Fatalf("D object %s token %s provisioned %d times, want 1", k, tok, n)
			}
		}
		sum.ready++
	}
	for i := 0; i < drillE; i++ {
		k := object.Key(fmt.Sprintf("e-%02d", i))
		o, err := st.Get(k)
		if err != nil || o.Status.Phase != object.PhaseAbandoned {
			t.Fatalf("E object %s not Abandoned: %v", k, err)
		}
		if n := drill.provisionsFor(k, "item"); n != 3 {
			t.Fatalf("E object %s calls=%d, want exactly budget+1=3", k, n)
		}
		v := eng.Inspect(k)
		if !v.Abandoned || !v.NextRetryAt.IsZero() || v.ConsecutiveFailures != 3 {
			t.Fatalf("E object %s view inconsistent: %+v", k, v)
		}
		sum.abandoned++
	}
	for i := 0; i < drillC; i++ {
		ok := object.Key(fmt.Sprintf("c-%02d", i))
		if left := len(st.ListChildren(ok)); left != 0 {
			t.Fatalf("owner %s has %d orphan children left", ok, left)
		}
		for j := 0; j < 2; j++ {
			ck := object.Key(fmt.Sprintf("c-%02d-ch%d", i, j))
			if n := drill.cleanupsFor(ck); n != 1 {
				t.Fatalf("child %s cleanup issued %d times, want exactly 1", ck, n)
			}
		}
		o, err := st.Get(ok)
		if err != nil {
			t.Fatal(err)
		}
		if o.HasFinalizer(object.FinalizerOwnerCleanup) {
			t.Fatalf("owner %s finalizer must be removed after cleanup", ok)
		}
		sum.deletingDone++
	}
	// 全库无残留 child。
	for _, o := range st.List() {
		if o.Owner != "" {
			sum.orphans++
		}
	}
	if sum.orphans != 0 {
		t.Fatalf("orphan children remain: %d", sum.orphans)
	}
	return sum
}

// 批量混合负载演练：成百对象在有限并发下收敛到确定终态；
// 同一场景连续跑 3 轮，终态摘要必须完全一致（可复现）。
func TestBatch_MixedLoadDrillDeterministic(t *testing.T) {
	var first *drillSummary
	for round := 1; round <= 3; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			sum := runBatchDrill(t)
			t.Logf("round %d summary: %+v", round, sum)
			if first == nil {
				first = &sum
				return
			}
			if sum != *first {
				t.Fatalf("round %d summary differs from round 1:\n got %+v\nwant %+v",
					round, sum, *first)
			}
		})
	}
}
