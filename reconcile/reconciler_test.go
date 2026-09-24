package reconcile

import (
	"testing"
)

// 场景1：重复协调幂等。
// 同一对象被重复协调时不得产生重复副作用（外部 Mutations 稳定），
// 已收敛对象不得被反复改写（Store WriteCount 在首次收敛后稳定）。
func TestReconcile_IdempotentRepeat(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("web", 3))
	svcID := svc.ID

	// 第一轮：创建 3 个派生对象（Service 自身本轮尚不能 Applied）。
	if res, err := rec.Reconcile(svcID); err != nil {
		t.Fatalf("reconcile#1: %v", err)
	} else if !res.Requeue {
		t.Fatalf("第一轮派生对象刚创建，应要求复查，得到 %+v", res)
	}

	eps := store.List(KindEndpoint)
	if len(eps) != 3 {
		t.Fatalf("期望 3 个派生对象，实际 %d", len(eps))
	}

	// 协调每个派生对象：每个产生恰好 1 次外部创建副作用。
	for _, ep := range eps {
		if _, err := rec.Reconcile(ep.ID); err != nil {
			t.Fatalf("reconcile endpoint %s: %v", ep.ID.Name, err)
		}
	}
	if got := ext.Mutations(); got != 3 {
		t.Fatalf("期望 3 次外部副作用，实际 %d", got)
	}

	// 再次协调 Service：应回写 Applied 并收敛。
	if _, err := rec.Reconcile(svcID); err != nil {
		t.Fatalf("reconcile service applied: %v", err)
	}
	got, ok := store.Get(svcID)
	if !ok {
		t.Fatalf("service 消失")
	}
	if got.ServiceStatus.Phase != PhaseApplied || got.ServiceStatus.ReadyReplicas != 3 {
		t.Fatalf("service 状态异常: %+v", got.ServiceStatus)
	}

	mutationsAfterApply := ext.Mutations()
	writeCounts := map[ObjectID]int{}
	for _, id := range append([]ObjectID{svcID},
		ObjectID{Kind: KindEndpoint, Name: "web-0"},
		ObjectID{Kind: KindEndpoint, Name: "web-1"},
		ObjectID{Kind: KindEndpoint, Name: "web-2"}) {
		writeCounts[id] = store.WriteCount(id)
	}

	// 重复协调 10 轮：任何对象都不应再产生写入或副作用。
	for i := 0; i < 10; i++ {
		if _, err := rec.Reconcile(svcID); err != nil {
			t.Fatalf("repeat service reconcile: %v", err)
		}
		for _, ep := range store.List(KindEndpoint) {
			if _, err := rec.Reconcile(ep.ID); err != nil {
				t.Fatalf("repeat endpoint reconcile %s: %v", ep.ID.Name, err)
			}
		}
	}

	if ext.Mutations() != mutationsAfterApply {
		t.Fatalf("重复协调产生了额外副作用: before=%d after=%d",
			mutationsAfterApply, ext.Mutations())
	}
	if ext.Mutations() != 3 {
		t.Fatalf("外部副作用总次数应为 3，实际 %d", ext.Mutations())
	}
	for id, want := range writeCounts {
		if got := store.WriteCount(id); got != want {
			t.Fatalf("对象 %s/%s 被反复改写: 收敛后写入数 %d -> %d",
				id.Kind, id.Name, want, got)
		}
	}
}

// 场景2：期望状态扩容/缩容，多退少补，副作用幂等。
func TestReconcile_ScaleUpDown(t *testing.T) {
	store, ext, rec := newEngine(t)
	svc := mustCreate(t, store, newService("app", 2))

	converge := func() {
		t.Helper()
		if _, err := rec.Reconcile(svc.ID); err != nil {
			t.Fatalf("reconcile svc: %v", err)
		}
		// 反复多轮直到派生对象全部 Applied。
		for round := 0; round < 10; round++ {
			for _, ep := range store.List(KindEndpoint) {
				if _, err := rec.Reconcile(ep.ID); err != nil {
					t.Fatalf("reconcile ep: %v", err)
				}
			}
			if _, err := rec.Reconcile(svc.ID); err != nil {
				t.Fatalf("reconcile svc2: %v", err)
			}
			s, _ := store.Get(svc.ID)
			if s.ServiceStatus.Phase == PhaseApplied {
				return
			}
		}
		t.Fatalf("service 未收敛")
	}
	converge()
	if ext.Mutations() != 2 {
		t.Fatalf("期望 2 次副作用, got %d", ext.Mutations())
	}

	// 扩容到 4：只应新增 2 次创建。
	cur, _ := store.Get(svc.ID)
	cur.Service.Replicas = 4
	if _, err := store.UpdateSpec(cur); err != nil {
		t.Fatalf("update spec: %v", err)
	}
	converge()
	if ext.Mutations() != 4 {
		t.Fatalf("扩容后期望 4 次副作用, got %d", ext.Mutations())
	}

	// 缩容到 1：多余 3 个派生对象进入删除流程，各产生 1 次删除副作用。
	cur, _ = store.Get(svc.ID)
	cur.Service.Replicas = 1
	if _, err := store.UpdateSpec(cur); err != nil {
		t.Fatalf("update spec: %v", err)
	}
	for round := 0; round < 20; round++ {
		if _, err := rec.Reconcile(svc.ID); err != nil {
			t.Fatalf("reconcile svc scale-down: %v", err)
		}
		for _, ep := range store.List(KindEndpoint) {
			if _, err := rec.Reconcile(ep.ID); err != nil {
				t.Fatalf("reconcile ep scale-down: %v", err)
			}
		}
		s, _ := store.Get(svc.ID)
		if s.ServiceStatus.Phase == PhaseApplied {
			break
		}
	}
	if n := len(store.List(KindEndpoint)); n != 1 {
		t.Fatalf("缩容后应只剩 1 个派生对象, got %d", n)
	}
	if ext.Mutations() != 7 { // 4 次创建 + 3 次删除
		t.Fatalf("缩容清理后期望 7 次副作用, got %d", ext.Mutations())
	}
}

// 场景3：过期快照写入必须被 CAS 拒绝，协调器以最新版本重新判断。
func TestReconcile_StaleSnapshotConflict(t *testing.T) {
	store, _, rec := newEngine(t)
	svc := mustCreate(t, store, newService("web", 1))

	// 取得快照 A（rv=1）。
	snapA, _ := store.Get(svc.ID)
	// 另一协调过程推进 Service 自身到新版本（rv=2）。
	fresh := snapA.Clone()
	fresh.ServiceStatus = &ServiceStatus{Phase: PhasePending, ObservedGen: 1}
	if _, err := store.UpdateStatus(fresh); err != nil {
		t.Fatalf("推进版本失败: %v", err)
	}
	// 用过期快照 A 写状态：必须冲突，过期内容（ReadyReplicas=99）不得落地。
	stale := snapA.Clone()
	stale.ServiceStatus = &ServiceStatus{Phase: PhaseApplied, ObservedGen: 1, ReadyReplicas: 99}
	if _, err := store.UpdateStatus(stale); !IsClass(err, ClassConflict) {
		t.Fatalf("期望 ClassConflict，实际 %v", err)
	}
	cur, _ := store.Get(svc.ID)
	if cur.ServiceStatus.ReadyReplicas != 0 {
		t.Fatalf("过期快照污染了最新状态: %+v", cur.ServiceStatus)
	}

	// 完整协调必须以最新版本完成，且最终状态正确。
	if _, err := rec.Reconcile(svc.ID); err != nil {
		t.Fatal(err)
	}
	for _, ep := range store.List(KindEndpoint) {
		if _, err := rec.Reconcile(ep.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rec.Reconcile(svc.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(svc.ID)
	if got.ServiceStatus.Phase != PhaseApplied || got.ServiceStatus.ReadyReplicas != 1 {
		t.Fatalf("最终状态异常: %+v", got.ServiceStatus)
	}
}

// 场景4：不可恢复的配置错误写入可区分的 Terminal 原因，且重复协调不产生副作用。
func TestReconcile_TerminalInvalidConfig(t *testing.T) {
	store, ext, rec := newEngine(t)
	bad := mustCreate(t, store, newService("invalid", 1))
	bad.Service.Invalid = true
	if _, err := store.UpdateSpec(bad); err != nil {
		t.Fatal(err)
	}

	_, err := rec.Reconcile(bad.ID)
	if !IsClass(err, ClassTerminal) {
		t.Fatalf("期望 ClassTerminal，实际 %v", err)
	}
	got, _ := store.Get(bad.ID)
	if got.ServiceStatus.Phase != PhaseTerminal || got.ServiceStatus.Message == "" {
		t.Fatalf("Terminal 状态/原因缺失: %+v", got.ServiceStatus)
	}
	firstMsg := got.ServiceStatus.Message

	// 重复协调：返回同样的 Terminal 分类，但不得反复改写（WriteCount 稳定），
	// 也不得创建任何派生对象/副作用。
	wc := store.WriteCount(bad.ID)
	for i := 0; i < 3; i++ {
		if _, err := rec.Reconcile(bad.ID); !IsClass(err, ClassTerminal) {
			t.Fatalf("第 %d 次重复仍应是 Terminal", i+2)
		}
	}
	if store.WriteCount(bad.ID) != wc {
		t.Fatalf("Terminal 对象被反复改写: %d -> %d", wc, store.WriteCount(bad.ID))
	}
	if len(store.List(KindEndpoint)) != 0 {
		t.Fatalf("非法 Service 不应派生对象")
	}
	if ext.Mutations() != 0 {
		t.Fatalf("非法配置不应产生外部副作用, got %d", ext.Mutations())
	}
	got, _ = store.Get(bad.ID)
	if got.ServiceStatus.Message != firstMsg {
		t.Fatalf("失败原因不稳定: %q vs %q", firstMsg, got.ServiceStatus.Message)
	}

	// 配置被修复后，同一对象的协调重新成功——Terminal 不是物理终态。
	got.Service.Invalid = false
	got.Service.Replicas = 1
	if _, err := store.UpdateSpec(got); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.Reconcile(bad.ID); err != nil {
		t.Fatalf("修复后首次协调: %v", err)
	}
	for _, ep := range store.List(KindEndpoint) {
		if _, err := rec.Reconcile(ep.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rec.Reconcile(bad.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = store.Get(bad.ID)
	if got.ServiceStatus.Phase != PhaseApplied {
		t.Fatalf("修复后应重新 Applied, got %+v", got.ServiceStatus)
	}
}

// 场景18：协调器在写入窗口遇到 CAS 冲突时，返回 Requeue(Delay=0) 立即重排，
// 绝不基于过期快照覆盖；重排后以最新版本继续收敛。
func TestReconcile_ConflictTriggersImmediateRequeue(t *testing.T) {
	store, _, rec := newEngine(t)
	svc := mustCreate(t, store, newService("cas", 1))

	// 劫持协调器存储：第一次 UpdateStatus 调用时注入冲突（模拟写入窗口被他人抢先）。
	mem := store.(*MemoryStore)
	injected := false
	rec.store = &conflictOnceStore{
		MemoryStore: mem,
		onUpdateStatus: func(o *Object) error {
			if !injected && o.ID == svc.ID {
				injected = true
				return errConflict
			}
			return nil
		},
	}

	// 第一轮：创建派生对象并返回重扫。
	if res, err := rec.Reconcile(svc.ID); err != nil {
		t.Fatal(err)
	} else if !res.Requeue {
		t.Fatalf("第一轮应重扫, got %+v", res)
	}
	for _, ep := range store.List(KindEndpoint) {
		if _, err := rec.Reconcile(ep.ID); err != nil {
			t.Fatal(err)
		}
	}

	// 冲突轮：UpdateStatus 被拒，必须以 Requeue(0) 返回而不是错误/退避。
	res, err := rec.Reconcile(svc.ID)
	if err != nil {
		t.Fatalf("冲突应转为立即重排而非错误: %v", err)
	}
	if !res.Requeue || res.Delay != 0 {
		t.Fatalf("冲突后应 Requeue+Delay0, got %+v", res)
	}
	if !injected {
		t.Fatalf("冲突注入未触发")
	}

	// 重排轮：冲突已消除，正常收敛。
	if _, err := rec.Reconcile(svc.ID); err != nil {
		t.Fatalf("冲突重排后协调失败: %v", err)
	}
	got, _ := store.Get(svc.ID)
	if got.ServiceStatus.Phase != PhaseApplied || got.ServiceStatus.ReadyReplicas != 1 {
		t.Fatalf("冲突重排后最终状态异常: %+v", got.ServiceStatus)
	}
}

// conflictOnceStore 在指定写入动作上注入一次冲突。
type conflictOnceStore struct {
	*MemoryStore
	onUpdateStatus func(o *Object) error
}

func (s *conflictOnceStore) UpdateStatus(o *Object) (*Object, error) {
	if err := s.onUpdateStatus(o); err != nil {
		return nil, err
	}
	return s.MemoryStore.UpdateStatus(o)
}
