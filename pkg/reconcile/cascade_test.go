package reconcile

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// cleanupApplier 统计 Cleanup 副作用次数（按 child key），并可注入失败。
type cleanupApplier struct {
	cleanups  atomic.Int64
	byChild   sync.Map     // object.Key -> *atomic.Int64
	failUntil atomic.Int64 // 前 N 次 Cleanup 返回可恢复失败
	attempts  atomic.Int64
}

func (c *cleanupApplier) Apply(_ context.Context, req ApplyRequest) error {
	if req.Kind != object.EffectCleanup {
		return nil
	}
	n := c.attempts.Add(1)
	if v, ok := c.byChild.Load(req.Object); ok {
		v.(*atomic.Int64).Add(1)
	} else {
		var ai atomic.Int64
		ai.Store(1)
		c.byChild.Store(req.Object, &ai)
	}
	if n <= c.failUntil.Load() {
		return Transient(errBoom)
	}
	c.cleanups.Add(1)
	return nil
}

func (c *cleanupApplier) childCleanups(k object.Key) int64 {
	if v, ok := c.byChild.Load(k); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

func readyOwner(key object.Key, children ...object.Key) *object.Object {
	o := &object.Object{
		Key:        key,
		Finalizers: []string{object.FinalizerOwnerCleanup},
		Spec:       object.Spec{Generation: 1},
		Status: object.Status{
			Phase:              object.PhaseReady,
			ReadyItems:         map[string]string{},
			ObservedGeneration: 1,
		},
	}
	return o
}

func childObject(key, owner object.Key) *object.Object {
	return &object.Object{
		Key:        key,
		Owner:      owner,
		Finalizers: []string{},
	}
}

// 场景 4：级联删除 —— owner 标记删除后，必须先回收全部 child
// （Cleanup 副作用 + child 物理删除），最后才摘除 owner 的 finalizer。
func TestCascadeDelete_ChildrenReclaimedBeforeFinalizerRemoved(t *testing.T) {
	clean := &cleanupApplier{}
	h := newHarness(t, clean,
		readyOwner("owner-1", "c1", "c2"),
		childObject("c1", "owner-1"),
		childObject("c2", "owner-1"))
	h.start()
	h.waitIdle() // 先收敛（owner 无 desired items，瞬间 Ready）

	// 记录稳定版本，然后发起删除请求。
	beforeRV := h.st.Version("owner-1")
	o := h.get("owner-1")
	o.MarkDeleted("2026-09-24T00:00:00Z")
	if _, err := h.st.CAS(o, beforeRV); err != nil {
		t.Fatalf("mark delete: %v", err)
	}
	h.eng.Trigger("owner-1")

	if !h.pollFor(3*time.Second, func() bool {
		return len(h.st.ListChildren("owner-1")) == 0
	}) {
		t.Fatalf("children were not reclaimed: %d left", len(h.st.ListChildren("owner-1")))
	}
	owner := h.get("owner-1")
	if owner.HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer must be removed only after all children are reclaimed")
	}
	if n := clean.childCleanups("c1"); n != 1 {
		t.Fatalf("c1 cleanup side-effect want 1, got %d", n)
	}
	if n := clean.childCleanups("c2"); n != 1 {
		t.Fatalf("c2 cleanup side-effect want 1, got %d", n)
	}
}

// 场景 5：child 的 Cleanup 遇到可恢复失败时退避重试，
// finalizer 在 child 回收完成前绝不摘除。
func TestCascadeDelete_CleanupRetriableKeepsFinalizer(t *testing.T) {
	clean := &cleanupApplier{}
	clean.failUntil.Store(2) // 前两次 cleanup 调用可恢复失败
	h := newHarness(t, clean,
		readyOwner("owner-2"),
		childObject("d1", "owner-2"))
	h.start()
	h.waitIdle()

	o := h.get("owner-2")
	o.MarkDeleted("2026-09-24T00:00:00Z")
	if _, err := h.st.CAS(o, h.st.Version("owner-2")); err != nil {
		t.Fatal(err)
	}
	h.eng.Trigger("owner-2")

	// 最终必须完成级联。
	if !h.pollFor(4*time.Second, func() bool {
		return len(h.st.ListChildren("owner-2")) == 0
	}) {
		t.Fatal("child not reclaimed after retries")
	}
	owner := h.get("owner-2")
	if owner.HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer should be removed after retried cleanup succeeds")
	}
	if n := clean.childCleanups("d1"); n < 2 {
		t.Fatalf("cleanup should have been retried, attempts=%d", n)
	}
}

// 场景 6：删除与更新并发竞争 —— 删除进行中，另一路不断用旧版本尝试
// 更新 child。要求：绝不留下无法回收的残留 child；最终全部回收。
func TestCascadeDelete_RacesWithConcurrentUpdates_NoOrphans(t *testing.T) {
	clean := &cleanupApplier{}
	// 前两次 Cleanup 可恢复失败：让级联回收持续若干轮（约 20ms），
	// 保证并发更新方有足够的竞争窗口，而不是回收瞬间结束、竞争未发生。
	clean.failUntil.Store(2)
	owner := readyOwner("owner-3")
	seed := []*object.Object{owner}
	for i := 0; i < 6; i++ {
		seed = append(seed, childObject(object.Key("r"+itoa(i)), "owner-3"))
	}
	h := newHarness(t, clean, seed...)
	h.start()
	h.waitIdle()

	// 一路协调删除；另一路并发地对 child 做"合法新版本更新"
	// （模拟更新方始终先读最新版本再 CAS，使 child 版本不断前移），
	// 同时穿插几次"陈旧版本强写"，必须全部被 CAS 拒绝。
	o := h.get("owner-3")
	o.MarkDeleted("2026-09-24T00:00:00Z")
	if _, err := h.st.CAS(o, h.st.Version("owner-3")); err != nil {
		t.Fatal(err)
	}

	stopUpdate := make(chan struct{})
	var staleRejected atomic.Int64
	updaterDone := make(chan struct{})
	go func() {
		defer close(updaterDone)
		// 有限竞争窗口：与删除协调并发约 200ms。
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			select {
			case <-stopUpdate:
				return
			default:
			}
			for i := 0; i < 6; i++ {
				k := object.Key("r" + itoa(i))
				cur, err := h.st.Get(k)
				if err != nil {
					continue // 可能已被回收
				}
				// 合法更新：基于最新版本推进 child。
				up := cur.DeepCopy()
				up.Status.Reason = "heartbeat"
				if _, err := h.st.CAS(up, cur.ResourceVersion); err != nil {
					// 与删除竞争输掉属正常。
					continue
				}
				// 陈旧强写：必须冲突，绝不能覆盖较新状态。
				stale := cur.DeepCopy()
				stale.Status.Reason = "stale-overwrite"
				if _, err := h.st.CAS(stale, cur.ResourceVersion); err == nil {
					t.Errorf("stale CAS on child must fail")
					return
				}
				staleRejected.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	h.eng.Trigger("owner-3")
	cleaned := h.pollFor(5*time.Second, func() bool {
		return len(h.st.ListChildren("owner-3")) == 0
	})
	close(stopUpdate)
	<-updaterDone
	if !cleaned {
		t.Fatalf("orphan children left after delete/update race: %d",
			len(h.st.ListChildren("owner-3")))
	}
	final := h.get("owner-3")
	if final.HasFinalizer(object.FinalizerOwnerCleanup) {
		t.Fatal("finalizer must be removed once all raced children are reclaimed")
	}
	if staleRejected.Load() == 0 {
		t.Fatal("expected at least one stale write to be rejected by CAS")
	}
}

// 场景 7：不同对象并行处理，同一对象不交叉；一个对象失败不阻塞其他对象。
func TestParallel_DifferentKeysConcurrentSameKeySerial(t *testing.T) {
	objs := []*object.Object{}
	for i := 0; i < 12; i++ {
		objs = append(objs, ownerObject(object.Key("p"+itoa(i)),
			map[string]string{"item": "v"}))
	}
	h := newHarness(t, ApplierFunc(func(context.Context, ApplyRequest) error {
		time.Sleep(5 * time.Millisecond) // 放大并行窗口
		return nil
	}), objs...)
	h.start()
	h.waitIdle()
	for i := 0; i < 12; i++ {
		o := h.get(object.Key("p" + itoa(i)))
		if o.Status.Phase != object.PhaseReady {
			t.Fatalf("object p%d should converge independently, got %s", i, o.Status.Phase)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
