package reconcile

import (
	"fmt"
	"strconv"
	"strings"
)

// resyncDelay 是安全重扫间隔：只要对象未完全收敛/未删除干净，
// 即使事件丢失也会在此间隔后被再次协调（level-triggered 的兜底）。
const resyncDelay Duration = 100_000_000

// Reconciler 对单个对象执行一次“期望状态 -> 实际状态”协调。
//
// 协调必须是水平触发（level-triggered）且幂等的：不依赖调用顺序，
// 同一输入（最新快照 + 外部实际状态）永远得到同一判定。
type Reconciler interface {
	Reconcile(id ObjectID) (Result, error)
}

// Engine 是 Reconciler 的默认实现，包含 Service 与 Endpoint 两类协调逻辑。
type Engine struct {
	store    Store
	external ExternalSystem
	log      Logger
}

// NewReconciler 创建协调器。
func NewReconciler(store Store, external ExternalSystem, log Logger) *Engine {
	if log == nil {
		log = noopLogger{}
	}
	return &Engine{store: store, external: external, log: log}
}

// Reconcile 按对象 Kind 分派。对象已消失视为协调完成。
func (e *Engine) Reconcile(id ObjectID) (Result, error) {
	obj, ok := e.store.Get(id)
	if !ok {
		e.log.Logf("[reconcile] input=%s/%s 对象已不存在 -> 无需动作", id.Kind, id.Name)
		return Result{}, nil
	}
	e.log.Logf("[reconcile] input=%s/%s gen=%d rv=%d deleting=%t finalizers=%v",
		obj.ID.Kind, obj.ID.Name, obj.Generation, obj.ResourceVersion,
		obj.DeletionMarked, obj.Finalizers)

	switch obj.ID.Kind {
	case KindService:
		return e.reconcileService(obj)
	case KindEndpoint:
		return e.reconcileEndpoint(obj)
	default:
		return Result{}, terminalf("unknown kind %q", obj.ID.Kind)
	}
}

// ---- Service ----

func (e *Engine) reconcileService(svc *Object) (Result, error) {
	// 1) finalizer 保证：未删除的 Service 必须有 finalizer。
	if !svc.DeletionMarked && !hasString(svc.Finalizers, FinalizerReconcile) {
		svc.Finalizers = append(svc.Finalizers, FinalizerReconcile)
		updated, err := e.store.UpdateSpec(svc)
		if err != nil {
			return e.conflictRetry(svc, err, "注册 finalizer")
		}
		e.log.Logf("[service/%s] 判定=缺少finalizer，已注册 -> rv=%d", svc.ID.Name, updated.ResourceVersion)
		// 以最新版本继续本轮协调（避免多一次空转轮次）。
		return e.reconcileService(updated)
	}

	if svc.DeletionMarked {
		return e.deleteService(svc)
	}

	// 2) 不可恢复的配置错误：写入可区分的 Terminal 原因并终止重试。
	if svc.Service.Invalid || svc.Service.Replicas < 0 {
		return e.terminateService(svc, fmt.Sprintf(
			"invalid service config: invalid=%t replicas=%d (replicas must be >= 0)",
			svc.Service.Invalid, svc.Service.Replicas))
	}

	// 3) 收敛派生对象集合：按稳定命名多退少补。
	eps := e.store.List(KindEndpoint)
	owned := make(map[string]*Object)
	for _, ep := range eps {
		if ep.Endpoint.Owner.Name == svc.ID.Name {
			owned[ep.ID.Name] = ep
		}
	}

	desired := make(map[string]bool, svc.Service.Replicas)
	for i := 0; i < svc.Service.Replicas; i++ {
		desired[endpointName(svc.ID.Name, i)] = true
	}

	var drift bool
	for name := range desired {
		ep, exists := owned[name]
		if !exists {
			if err := e.createEndpoint(svc, name); err != nil {
				if IsClass(err, ClassConflict) {
					return e.conflictRetry(svc, err, "创建派生对象 "+name)
				}
				return Result{}, err
			}
			drift = true
			continue
		}
		// 已有对象：若 spec 漂移则纠正（CAS 保护，冲突则以最新版本重来）。
		if !ep.DeletionMarked &&
			(ep.Endpoint.Image != "default" || ep.Endpoint.Capacity != 100) {
			ep.Endpoint.Image = "default"
			ep.Endpoint.Capacity = 100
			if _, err := e.store.UpdateSpec(ep); err != nil {
				return e.conflictRetry(svc, err, "纠正派生对象 "+name+" 的 spec")
			}
			drift = true
		}
	}
	// 多余的派生对象：级联标记删除，由 Endpoint 协调路径完成清理。
	for name, ep := range owned {
		if !desired[name] && !ep.DeletionMarked {
			if _, err := e.store.MarkDeleting(ep.ID); err != nil {
				return e.conflictRetry(svc, err, "级联删除多余派生对象 "+name)
			}
			drift = true
		}
	}

	// 4) 统计就绪数；只有所有期望派生对象 Applied 后 Service 才 Applied。
	//
	// “就绪”只取决于外部副作用是否曾确认成功：Provisioned 且相位为 Applied
	// （非 Terminal、未删除）。不把 ObservedGen 短暂落后（spec 扰动后等待
	// 幂等再确认）或瞬时可恢复故障计入掉就绪——这保证“保留上次成功状态”
	// 对属主同样成立：一个副本临时失败不会让已 Applied 的 Service 抖动。
	ready := 0
	for name := range desired {
		if ep, ok := owned[name]; ok &&
			!ep.DeletionMarked &&
			ep.EndpointStatus != nil &&
			ep.EndpointStatus.Provisioned &&
			ep.EndpointStatus.Phase == PhaseApplied {
			ready++
		}
	}

	wantStatus := &ServiceStatus{
		Phase:         PhaseApplied,
		ObservedGen:   svc.Generation,
		ReadyReplicas: ready,
	}
	if ready < svc.Service.Replicas {
		wantStatus.Phase = PhasePending
	}

	if drift {
		// 刚发生过写入，派生对象的状态事件会重新唤醒本 Service。
		e.log.Logf("[service/%s] 判定=存在派生对象漂移，已发起纠正，等待派生对象就绪", svc.ID.Name)
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}

	if !serviceStatusEqual(svc.ServiceStatus, wantStatus) {
		updated, err := e.store.UpdateStatus(withServiceStatus(svc, wantStatus))
		if err != nil {
			return e.conflictRetry(svc, err, "回写 Service 状态")
		}
		e.log.Logf("[service/%s] 判定=状态更新 phase=%s ready=%d/%d gen=%d -> rv=%d",
			svc.ID.Name, wantStatus.Phase, ready, svc.Service.Replicas,
			svc.Generation, updated.ResourceVersion)
		if wantStatus.Phase == PhasePending {
			return Result{Requeue: true, Delay: resyncDelay}, nil
		}
		return Result{}, nil
	}

	if wantStatus.Phase == PhasePending {
		e.log.Logf("[service/%s] 判定=等待派生对象就绪 ready=%d/%d",
			svc.ID.Name, ready, svc.Service.Replicas)
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}

	e.log.Logf("[service/%s] 判定=已收敛 phase=Applied ready=%d/%d，无写入（幂等）",
		svc.ID.Name, ready, svc.Service.Replicas)
	return Result{}, nil
}

// deleteService 处理 Service 删除：派生对象必须全部确认清理后才允许自身回收。
func (e *Engine) deleteService(svc *Object) (Result, error) {
	remaining := 0
	var leftover string
	for _, ep := range e.store.List(KindEndpoint) {
		if ep.Endpoint.Owner.Name != svc.ID.Name {
			continue
		}
		remaining++
		leftover = ep.ID.Name
		if !ep.DeletionMarked {
			// 删除与更新并发：删除胜出，任何残留派生对象立即标记删除。
			if _, err := e.store.MarkDeleting(ep.ID); err != nil {
				return e.conflictRetry(svc, err, "删除流程中标记派生对象 "+ep.ID.Name)
			}
			e.log.Logf("[service/%s] 判定=删除中，补标记残留派生对象 %s", svc.ID.Name, ep.ID.Name)
		}
	}

	// 双保险：确认 Store 中已无派生对象，且外部世界没有任何属于本 Service
	// 命名前缀的残留资源（防止物理删除记录丢失但外部资源仍在）。
	externalLeak := false
	prefix := svc.ID.Name + "-"
	for _, epID := range e.external.ResourceIDs() {
		if epID.Kind == KindEndpoint && strings.HasPrefix(epID.Name, prefix) {
			externalLeak = true
			break
		}
	}

	if remaining > 0 || externalLeak {
		e.log.Logf("[service/%s] 判定=删除未完成 store残留=%d(如%s) 外部残留=%t -> 延迟复查",
			svc.ID.Name, remaining, leftover, externalLeak)
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}

	updated, err := e.store.RemoveFinalizer(svc.ID, svc.ResourceVersion, FinalizerReconcile)
	if err != nil {
		return e.conflictRetry(svc, err, "移除 Service finalizer")
	}
	e.log.Logf("[service/%s] 判定=派生对象已全部确认清理，移除finalizer -> rv=%d",
		svc.ID.Name, updated.ResourceVersion)

	if err := e.store.Delete(svc.ID); err != nil {
		// 物理删除失败（仍有 finalizer / 冲突）：以最新版本重来。
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}
	e.log.Logf("[service/%s] 判定=已物理删除，回收完成", svc.ID.Name)
	return Result{}, nil
}

func (e *Engine) terminateService(svc *Object, msg string) (Result, error) {
	want := &ServiceStatus{
		Phase:       PhaseTerminal,
		ObservedGen: svc.Generation,
		Message:     msg,
	}
	if !serviceStatusEqual(svc.ServiceStatus, want) {
		updated, err := e.store.UpdateStatus(withServiceStatus(svc, want))
		if err != nil {
			return e.conflictRetry(svc, err, "写入 Terminal 状态")
		}
		e.log.Logf("[service/%s] 判定=不可恢复配置错误，写入Terminal原因=%q -> rv=%d",
			svc.ID.Name, msg, updated.ResourceVersion)
	} else {
		e.log.Logf("[service/%s] 判定=Terminal状态已存在，原因=%q，无写入（幂等）",
			svc.ID.Name, msg)
	}
	return Result{}, terminalf("%s", msg)
}

func (e *Engine) createEndpoint(svc *Object, name string) error {
	ep := &Object{
		ID:         ObjectID{Kind: KindEndpoint, Name: name},
		Finalizers: []string{FinalizerReconcile},
		Endpoint: &EndpointSpec{
			Owner:    OwnerRef{Name: svc.ID.Name},
			Image:    "default",
			Capacity: 100,
		},
		EndpointStatus: &EndpointStatus{Phase: PhasePending},
	}
	if _, err := e.store.Create(ep); err != nil {
		e.log.Logf("[service/%s] 判定=需要派生对象 %s 但创建被拒：%v", svc.ID.Name, name, err)
		return err
	}
	e.log.Logf("[service/%s] 判定=创建缺失派生对象 %s（Store 孤儿防护已检查属主未删除）",
		svc.ID.Name, name)
	return nil
}

// ---- Endpoint ----

func (e *Engine) reconcileEndpoint(ep *Object) (Result, error) {
	if !ep.DeletionMarked && !hasString(ep.Finalizers, FinalizerReconcile) {
		ep.Finalizers = append(ep.Finalizers, FinalizerReconcile)
		updated, err := e.store.UpdateSpec(ep)
		if err != nil {
			if IsClass(err, ClassConflict) {
				return Result{Requeue: true, Delay: 0}, nil
			}
			return Result{}, err
		}
		e.log.Logf("[endpoint/%s] 判定=缺少finalizer，已注册 -> rv=%d", ep.ID.Name, updated.ResourceVersion)
		return e.reconcileEndpoint(updated)
	}

	if ep.DeletionMarked {
		return e.deleteEndpoint(ep)
	}

	owner, ownerExists := e.store.Get(ObjectID{Kind: KindService, Name: ep.Endpoint.Owner.Name})
	if !ownerExists || owner.DeletionMarked {
		// 属主消失或正在删除：派生对象不得继续存活，直接进入级联删除。
		// 正常流程下 Store 的 Create 准入已经阻止“删除中属主新建派生对象”，
		// 这里覆盖属主被直接删除等存量对象的回收路径。
		e.log.Logf("[endpoint/%s] 判定=属主 %s 存在=%t 删除中=%t -> 级联清理派生对象",
			ep.ID.Name, ep.Endpoint.Owner.Name, ownerExists, ownerExists && owner.DeletionMarked)
		if _, err := e.store.MarkDeleting(ep.ID); err != nil {
			if IsClass(err, ClassConflict) {
				return Result{Requeue: true, Delay: 0}, nil
			}
			return Result{}, err
		}
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}

	// 不可恢复的配置错误。
	if ep.Endpoint.Capacity <= 0 {
		return e.terminateEndpoint(ep, fmt.Sprintf(
			"invalid endpoint config: capacity=%d (must be > 0)", ep.Endpoint.Capacity))
	}

	// 幂等外部副作用：已存在时 created=false 且不产生任何副作用计数。
	created, err := e.external.Provision(ep)
	if err != nil {
		// 可恢复失败：不覆盖状态，原样返回供调度器退避重试，
		// 上次成功写入的 Applied 状态因此得以保留。
		e.log.Logf("[endpoint/%s] 判定=外部资源协调可恢复失败：%v（保留现有状态，等待退避重试）",
			ep.ID.Name, err)
		return Result{}, transientf("provision %s: %v", ep.ID.Name, err)
	}

	want := &EndpointStatus{
		Phase:       PhaseApplied,
		ObservedGen: ep.Generation,
		Provisioned: true,
	}
	if !endpointStatusEqual(ep.EndpointStatus, want) {
		updated, err := e.store.UpdateStatus(withEndpointStatus(ep, want))
		if err != nil {
			if IsClass(err, ClassConflict) {
				return Result{Requeue: true, Delay: 0}, nil
			}
			return Result{}, err
		}
		e.log.Logf("[endpoint/%s] 判定=外部资源 created=%t，回写Applied -> rv=%d",
			ep.ID.Name, created, updated.ResourceVersion)
		return Result{}, nil
	}
	e.log.Logf("[endpoint/%s] 判定=已收敛 created=%t 外部资源已存在，无写入（幂等）",
		ep.ID.Name, created)
	return Result{}, nil
}

// deleteEndpoint 处理 Endpoint 删除：外部资源未回收干净前绝不结束删除。
func (e *Engine) deleteEndpoint(ep *Object) (Result, error) {
	deleted, err := e.external.Delete(ep)
	if err != nil {
		e.log.Logf("[endpoint/%s] 判定=外部资源回收可恢复失败：%v（保留finalizer，退避重试）",
			ep.ID.Name, err)
		return Result{}, transientf("delete %s: %v", ep.ID.Name, err)
	}

	// 删除与更新并发的竞争窗口：回收后、finalizer 移除前，若用户更新令
	// DeletionMarked 消失（若存储支持复活）或 spec 再次变更，仍以重新读取
	// 到的最新版本为准；此处 RemoveFinalizer 的 CAS 会在 rv 过期时失败。
	updated, err := e.store.RemoveFinalizer(ep.ID, ep.ResourceVersion, FinalizerReconcile)
	if err != nil {
		if IsClass(err, ClassConflict) {
			return Result{Requeue: true, Delay: 0}, nil
		}
		return Result{}, err
	}

	if err := e.store.Delete(ep.ID); err != nil {
		e.log.Logf("[endpoint/%s] 判定=物理删除未完成：%v -> 延迟复查", ep.ID.Name, err)
		return Result{Requeue: true, Delay: resyncDelay}, nil
	}
	e.log.Logf("[endpoint/%s] 判定=外部资源 deleted=%t，finalizer已移除(rv=%d)，物理删除完成",
		ep.ID.Name, deleted, updated.ResourceVersion)
	return Result{}, nil
}

func (e *Engine) terminateEndpoint(ep *Object, msg string) (Result, error) {
	want := &EndpointStatus{
		Phase:       PhaseTerminal,
		ObservedGen: ep.Generation,
		Message:     msg,
	}
	if !endpointStatusEqual(ep.EndpointStatus, want) {
		updated, err := e.store.UpdateStatus(withEndpointStatus(ep, want))
		if err != nil {
			if IsClass(err, ClassConflict) {
				return Result{Requeue: true, Delay: 0}, nil
			}
			return Result{}, err
		}
		e.log.Logf("[endpoint/%s] 判定=不可恢复配置错误，写入Terminal原因=%q -> rv=%d",
			ep.ID.Name, msg, updated.ResourceVersion)
	} else {
		e.log.Logf("[endpoint/%s] 判定=Terminal状态已存在，原因=%q，无写入（幂等）",
			ep.ID.Name, msg)
	}
	return Result{}, terminalf("%s", msg)
}

// ---- 辅助 ----

// conflictRetry 把 CAS 冲突转换为“立即以最新版本重试”的结论：
// 本轮绝不基于过期快照继续写，也不按失败退避。
func (e *Engine) conflictRetry(base *Object, err error, action string) (Result, error) {
	if IsClass(err, ClassConflict) {
		e.log.Logf("[%s/%s] 判定=%s 时遇到版本冲突（持有rv=%d）-> 立即以最新版本重新判断",
			base.ID.Kind, base.ID.Name, action, base.ResourceVersion)
		return Result{Requeue: true, Delay: 0}, nil
	}
	return Result{}, err
}

func endpointName(serviceName string, idx int) string {
	return serviceName + "-" + strconv.Itoa(idx)
}

func withServiceStatus(o *Object, st *ServiceStatus) *Object {
	c := o.Clone()
	c.ServiceStatus = st.Clone()
	c.EndpointStatus = nil
	return c
}

func withEndpointStatus(o *Object, st *EndpointStatus) *Object {
	c := o.Clone()
	c.EndpointStatus = st.Clone()
	c.ServiceStatus = nil
	return c
}

func serviceStatusEqual(a, b *ServiceStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func endpointStatusEqual(a, b *EndpointStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
