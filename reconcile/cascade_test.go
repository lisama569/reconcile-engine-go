package reconcile

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var errDeletedObjectRevived = errors.New("deleted object was revived by stale snapshot")

func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found")
}

// 完整收敛一个 Service（控制器外的确定性驱动）：交替协调属主与派生对象，
// 直到 Service Applied。
func convergeService(t *testing.T, rec Reconciler, store Store, svcID ObjectID) {
	t.Helper()
	for round := 0; round < 50; round++ {
		if _, err := rec.Reconcile(svcID); err != nil {
			t.Fatalf("reconcile service: %v", err)
		}
		for _, ep := range store.List(KindEndpoint) {
			if _, err := rec.Reconcile(ep.ID); err != nil {
				t.Fatalf("reconcile endpoint %s: %v", ep.ID.Name, err)
			}
		}
		s, ok := store.Get(svcID)
		if ok && s.ServiceStatus.Phase == PhaseApplied {
			return
		}
	}
	t.Fatalf("service %s 未在限定轮次内收敛", svcID.Name)
}

// 场景13：级联删除语义——属主删除时，派生对象与外部资源全部确认清理后，
// 属主才允许物理消失；顺序相反即失败。
func TestCascadeDelete_OwnerWaitsForDependents(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("db", 3))
	convergeService(t, rec, store, svc.ID)
	if ext.Mutations() != 3 {
		t.Fatalf("准备阶段副作用数=%d", ext.Mutations())
	}

	// 用户删除属主。
	if _, err := store.MarkDeleting(svc.ID); err != nil {
		t.Fatal(err)
	}

	// 只协调 Service 多次：它必须等待派生对象清理，自身不得物理消失。
	for i := 0; i < 3; i++ {
		if res, err := rec.Reconcile(svc.ID); err != nil {
			t.Fatalf("删除中协调失败: %v", err)
		} else if !res.Requeue {
			t.Fatalf("派生对象未清理完，Service 不应结束删除")
		}
		if _, ok := store.Get(svc.ID); !ok {
			t.Fatalf("属主在派生对象确认清理前被删除")
		}
	}

	// 派生对象逐个完成删除清理。
	for round := 0; round < 20; round++ {
		for _, ep := range store.List(KindEndpoint) {
			if _, err := rec.Reconcile(ep.ID); err != nil {
				t.Fatalf("删除派生对象: %v", err)
			}
		}
		if _, err := rec.Reconcile(svc.ID); err != nil {
			t.Fatalf("删除属主: %v", err)
		}
		if _, ok := store.Get(svc.ID); !ok {
			break
		}
	}

	if _, ok := store.Get(svc.ID); ok {
		t.Fatalf("派生对象清理完成后属主仍未删除")
	}
	if n := len(store.List(KindEndpoint)); n != 0 {
		t.Fatalf("仍有 %d 个派生对象残留", n)
	}
	if len(ext.ResourceIDs()) != 0 {
		t.Fatalf("外部资源残留: %v", ext.ResourceIDs())
	}
	if ext.Mutations() != 6 { // 3 创建 + 3 删除
		t.Fatalf("副作用总数应为 6(3创3删), got %d", ext.Mutations())
	}
}

// 场景14：删除进行中，派生对象外部资源删除遇到可恢复失败——
// finalizer 保留、对象残留但最终在退避后被回收，无泄漏。
func TestCascadeDelete_RetryThenReap(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("mail", 2))
	convergeService(t, rec, store, svc.ID)

	ext.SetDeleteFail(4) // 前 4 次删除尝试失败（跨两个派生对象）

	if _, err := store.MarkDeleting(svc.ID); err != nil {
		t.Fatal(err)
	}

	stop, _ := runController(t, store, rec, 4)
	defer stop()

	eventually(t, 3*time.Second, func() bool {
		_, ownerExists := store.Get(svc.ID)
		return !ownerExists
	})
	if n := len(store.List(KindEndpoint)); n != 0 {
		t.Fatalf("派生对象残留 %d", n)
	}
	if len(ext.ResourceIDs()) != 0 {
		t.Fatalf("外部资源泄漏: %v", ext.ResourceIDs())
	}
}

// 场景15（核心竞争）：删除与更新并发发生。
// 删除流程进行的同时，另一路不断尝试用旧快照“纠正”派生对象的 spec：
//   - 对已删除对象的更新必须失败（不能复活）；
//   - CAS 冲突绝不能导致派生对象/外部资源残留；
//   - 属主在确认派生对象全部清理后才物理删除。
func TestCascadeDelete_ConcurrentUpdateDoesNotLeak(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("race", 3))
	convergeService(t, rec, store, svc.ID)

	if _, err := store.MarkDeleting(svc.ID); err != nil {
		t.Fatal(err)
	}
	// 收集删除前快照（随即过期）。
	staleSnaps := store.List(KindEndpoint)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 竞争方 A：删除进行的同时，不断并发尝试更新仍存在的派生对象 spec，
	// 并对已物理删除的对象用旧快照发起更新（必须失败，不能复活）。
	var raceErr error
	var raceOnce sync.Once
	failRace := func(err error) {
		raceOnce.Do(func() { raceErr = err })
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			alive := 0
			for _, snap := range staleSnaps {
				cur, ok := store.Get(snap.ID)
				if !ok {
					// 对象已物理删除：用旧快照更新必须失败，不能重建。
					if _, err := store.UpdateSpec(snap.Clone()); err == nil {
						failRace(errDeletedObjectRevived)
						return
					}
					continue
				}
				alive++
				upd := cur.Clone()
				upd.Endpoint.Image = "race-" + snap.ID.Name
				if _, err := store.UpdateSpec(upd); err != nil &&
					!IsClass(err, ClassConflict) && !isNotFound(err) {
					failRace(err)
					return
				}
			}
			if alive == 0 {
				return // 派生对象全部物理消失，更新方退出
			}
			runtime.Gosched() // 让出 P，保证删除方在多 P 上推进
		}
	}()

	// 竞争方 B：正常级联删除驱动。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 200; round++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := rec.Reconcile(svc.ID); err != nil {
				t.Errorf("reconcile deleting svc: %v", err)
				return
			}
			for _, ep := range store.List(KindEndpoint) {
				if _, err := rec.Reconcile(ep.ID); err != nil {
					t.Errorf("reconcile deleting ep: %v", err)
					return
				}
			}
			if _, ok := store.Get(svc.ID); !ok {
				return
			}
		}
		t.Errorf("级联删除在 200 轮内未完成")
	}()

	wg.Wait()
	close(stop)
	if raceErr != nil {
		t.Fatalf("并发更新方出现非法结果: %v", raceErr)
	}

	if _, ok := store.Get(svc.ID); ok {
		t.Fatalf("属主未删除")
	}
	if n := len(store.List(KindEndpoint)); n != 0 {
		t.Fatalf("竞争结束后残留 %d 个派生对象: %v", n, store.List(KindEndpoint))
	}
	if len(ext.ResourceIDs()) != 0 {
		t.Fatalf("竞争结束后外部资源泄漏: %v", ext.ResourceIDs())
	}
	if ext.Mutations() != 6 {
		t.Fatalf("期望恰好 3创+3删=6 次副作用（幂等不得重复），实际 %d", ext.Mutations())
	}
}

// 场景16：属主删除中尝试新建派生对象，Store 孤儿防护必须拒绝，
// 控制器驱动下删除最终无残留。
func TestCascadeDelete_OrphanAdmission(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("adm", 1))
	convergeService(t, rec, store, svc.ID)

	if _, err := store.MarkDeleting(svc.ID); err != nil {
		t.Fatal(err)
	}
	// 删除中的属主：新建派生对象必须被准入规则拒绝。
	rogue := newEndpoint("adm-rogue", "adm", 100)
	if _, err := store.Create(rogue); !IsClass(err, ClassConflict) {
		t.Fatalf("期望以冲突拒绝孤儿创建, got %v", err)
	}

	// 即便绕过准入（模拟陈旧协调器在标记前拿到快照），属主协调也会补标记。
	stop, _ := runController(t, store, rec, 2)
	defer stop()
	eventually(t, 2*time.Second, func() bool {
		_, ok := store.Get(svc.ID)
		return !ok
	})
	if n := len(store.List(KindEndpoint)); n != 0 {
		t.Fatalf("残留派生对象 %d", n)
	}
	if len(ext.ResourceIDs()) != 0 {
		t.Fatalf("外部资源泄漏: %v", ext.ResourceIDs())
	}
}

// 场景17：缩容（属主仍存活）与派生对象更新并发，多余副本必须被回收且不影响保留副本。
func TestCascadeDelete_ScaleDownConcurrent(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("sd", 4))
	convergeService(t, rec, store, svc.ID)

	cur, _ := store.Get(svc.ID)
	cur.Service.Replicas = 1
	if _, err := store.UpdateSpec(cur); err != nil {
		t.Fatal(err)
	}

	stop, c := runController(t, store, rec, 4)
	defer stop()
	_ = c

	eventually(t, 2*time.Second, func() bool {
		s, ok := store.Get(svc.ID)
		// 必须是反映了最新 Generation 的 Applied，而不是缩容前遗留的旧状态。
		return ok && s.ServiceStatus.Phase == PhaseApplied &&
			s.ServiceStatus.ObservedGen == s.Generation
	})
	if n := len(store.List(KindEndpoint)); n != 1 {
		t.Fatalf("应只保留 1 个副本, got %d", n)
	}
	if kept, ok := store.Get(ObjectID{Kind: KindEndpoint, Name: "sd-0"}); !ok ||
		kept.DeletionMarked || !kept.EndpointStatus.Provisioned {
		t.Fatalf("保留副本状态异常: %+v", kept)
	}
	alive := 0
	for _, id := range ext.ResourceIDs() {
		if id.Name == "sd-0" {
			alive++
		}
	}
	if alive != 1 {
		t.Fatalf("外部世界应只剩 sd-0, got %v", ext.ResourceIDs())
	}
}
