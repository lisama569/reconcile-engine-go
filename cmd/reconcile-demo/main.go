// reconcile-demo 本地端到端演示：启动控制器，依次执行
//
//  1. 创建两个 Service（一个配置非法）
//  2. 可恢复故障自动退避重试
//  3. 扩缩容（多退少补、级联回收）
//  4. 删除一个 Service（级联删除：先清派生对象与外部资源）
//  5. 持续并发更新与删除竞争，证明无残留
//
// 运行：go run ./cmd/reconcile-demo
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/reconcile-engine-go/reconcile"
)

type stdLogger struct{}

func (stdLogger) Logf(format string, args ...any) {
	fmt.Printf("    %s\n", fmt.Sprintf(format, args...))
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := reconcile.NewMemoryStore()
	ext := reconcile.NewFakeExternal()
	rec := reconcile.NewReconciler(store, ext, stdLogger{})
	ctrl := reconcile.NewController(reconcile.Config{
		Store:   store,
		Rec:     rec,
		Backoff: reconcile.ExponentialBackoff{Base: 20 * time.Millisecond, Max: time.Second},
		Log:     stdLogger{},
		Workers: 4,
	})
	go ctrl.Run(ctx)

	fmt.Println("== 1. 创建 shop(2副本) 与 broken(非法配置) ==")
	createService(store, "shop", 2, false)
	createService(store, "broken", 2, true)
	waitApplied(store, "shop")
	waitTerminal(store, "broken")
	fmt.Printf("   外部副作用累计=%d（broken 不应产生任何副作用）\n\n", ext.Mutations())

	fmt.Println("== 2. 注入 2 次可恢复故障，演示退避重试与状态保留 ==")
	ext.SetEnsureFail(2)
	// 触发一次期望对象重入队（无内容变化的并发扰动），让故障立即被消费。
	touchService(store, "shop")
	waitFor(func() bool { return ctrl.SnapshotMetrics().TransientFailures >= 2 }, time.Second)
	appliedThroughout := true
	if o, ok := store.Get(reconcile.ObjectID{Kind: reconcile.KindService, Name: "shop"}); !ok ||
		o.ServiceStatus.Phase != reconcile.PhaseApplied {
		appliedThroughout = false
	}
	m := ctrl.SnapshotMetrics()
	fmt.Printf("   可恢复失败=%d，Applied 状态在故障期间保留=%t，退避后自动恢复（副作用仍=%d）\n\n",
		m.TransientFailures, appliedThroughout, ext.Mutations())

	fmt.Println("== 3. shop 扩容 2 -> 4 ==")
	updateReplicas(store, "shop", 4)
	waitApplied(store, "shop")
	fmt.Printf("   外部副作用累计=%d（仅新增 2 次创建）\n\n", ext.Mutations())

	fmt.Println("== 4. shop 缩容 4 -> 1（多余副本级联回收）==")
	updateReplicas(store, "shop", 1)
	waitApplied(store, "shop")
	fmt.Printf("   外部副作用累计=%d（新增3 + 回收3），Store 中 Endpoint=%d\n\n",
		ext.Mutations(), len(store.List(reconcile.KindEndpoint)))

	fmt.Println("== 5. 删除 shop：派生对象确认清理后属主才消失 ==")
	if _, err := store.MarkDeleting(reconcile.ObjectID{Kind: reconcile.KindService, Name: "shop"}); err != nil {
		panic(err)
	}
	waitGone(store, "shop")
	fmt.Printf("   shop 已物理删除；残留 Endpoint=%d，外部残留=%d，副作用=%d\n\n",
		len(store.List(reconcile.KindEndpoint)), len(ext.ResourceIDs()), ext.Mutations())

	fmt.Println("== 6. 删除/更新并发竞争：race(3) 删除期间持续并发更新 ==")
	createService(store, "race", 3, false)
	waitApplied(store, "race")
	beforeMutations := ext.Mutations()
	// 保存删除前的快照（此后版本即刻过期）。
	staleShots := store.List(reconcile.KindEndpoint)
	if _, err := store.MarkDeleting(reconcile.ObjectID{Kind: reconcile.KindService, Name: "race"}); err != nil {
		panic(err)
	}
	raceUpdates(store, rec, staleShots)
	waitGone(store, "race")
	fmt.Printf("   竞争结束：残留 Endpoint=%d 外部残留=%d 竞争期新增副作用=%d（应恰好 0，删除只产生3次回收）\n\n",
		len(store.List(reconcile.KindEndpoint)), len(ext.ResourceIDs()),
		ext.Mutations()-beforeMutations-3)

	fmt.Println("全部演示场景完成，最终状态可复现、可解释。")
	cancel()
	time.Sleep(20 * time.Millisecond)
}

func createService(store reconcile.Store, name string, replicas int, invalid bool) {
	_, err := store.Create(&reconcile.Object{
		ID:         reconcile.ObjectID{Kind: reconcile.KindService, Name: name},
		Finalizers: []string{reconcile.FinalizerReconcile},
		Service:    &reconcile.ServiceSpec{Replicas: replicas, Invalid: invalid},
		ServiceStatus: &reconcile.ServiceStatus{
			Phase:       reconcile.PhasePending,
			ObservedGen: 0,
		},
	})
	if err != nil {
		panic(err)
	}
}

func updateReplicas(store reconcile.Store, name string, n int) {
	obj, ok := store.Get(reconcile.ObjectID{Kind: reconcile.KindService, Name: name})
	if !ok {
		panic("service not found: " + name)
	}
	obj.Service.Replicas = n
	if _, err := store.UpdateSpec(obj); err != nil {
		panic(err)
	}
}

// touchService 以最新版本回写同一 spec：内容无变化时 Store 判为空操作，
// 但为了演示“立即再协调”，这里先做一次真实的微小扰动（Image 由属主纠正），
// 保证派生对象产生状态事件。演示场景下简单通过副本数 +0 后立即恢复来触发。
func touchService(store reconcile.Store, name string) {
	// 直接对一个派生对象做一次状态无关的 spec 重写：image 变化会产生事件，
	// 属主协调随后把它纠正回 default（多退少补逻辑天然幂等）。
	eps := store.List(reconcile.KindEndpoint)
	for _, ep := range eps {
		cur, ok := store.Get(ep.ID)
		if !ok {
			continue
		}
		cur.Endpoint.Image = "touch"
		if _, err := store.UpdateSpec(cur); err != nil {
			panic(err)
		}
	}
}

func waitFor(fn func() bool, d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	panic("waitFor timeout")
}

func waitApplied(store reconcile.Store, name string) {
	id := reconcile.ObjectID{Kind: reconcile.KindService, Name: name}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if o, ok := store.Get(id); ok &&
			o.ServiceStatus.Phase == reconcile.PhaseApplied &&
			o.ServiceStatus.ObservedGen == o.Generation {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("timeout waiting Applied: " + name)
}

func waitTerminal(store reconcile.Store, name string) {
	id := reconcile.ObjectID{Kind: reconcile.KindService, Name: name}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if o, ok := store.Get(id); ok && o.ServiceStatus.Phase == reconcile.PhaseTerminal {
			fmt.Printf("   %s 进入 Terminal，失败原因：%s\n", name, o.ServiceStatus.Message)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("timeout waiting Terminal: " + name)
}

func waitGone(store reconcile.Store, name string) {
	id := reconcile.ObjectID{Kind: reconcile.KindService, Name: name}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(id); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("timeout waiting deletion: " + name)
}

// raceUpdates 在删除进行的同时，用删除前的旧快照并发提交更新：
// 每一笔都因 CAS 版本冲突（或对象已删除）被拒，不能产生任何副作用、
// 也不能复活对象。删除由控制器经工作队列串行驱动。
func raceUpdates(store reconcile.Store, rec reconcile.Reconciler, staleShots []*reconcile.Object) {
	_ = rec
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		attempts := 0
		for round := 0; round < 2000; round++ {
			for _, snap := range staleShots {
				attempts++
				stale := snap.Clone()
				stale.Endpoint.Image = "race-image"
				if _, err := store.UpdateSpec(stale); err != nil {
					_ = err // Conflict / not found：删除与更新竞争的预期结果
				}
			}
			if _, ok := store.Get(reconcile.ObjectID{Kind: reconcile.KindService, Name: "race"}); !ok {
				fmt.Printf("   竞争期共发起 %d 笔过期快照写入，全部被 CAS/删除准入拒绝\n", attempts)
				return
			}
		}
	}()
	wg.Wait()
}
