package reconcile

import (
	"fmt"
	"reflect"
	"sync"
)

// FinalizerReconcile 是协调器注册的 finalizer；非空时 Store 拒绝物理删除。
const FinalizerReconcile = "reconcile.engine/finalizer"

// Store 是带版本号与变更订阅的对象存储。
//
// 所有写入方法都要求调用方持有读取时观测到的 ResourceVersion（CAS）：
// 若期间对象已被其他协调过程改写，写入返回 ClassConflict，调用方必须
// 重新 Get 最新版本再判断，严禁用过期快照覆盖较新的状态。
type Store interface {
	// Get 返回对象的最新快照（深拷贝）；不存在时第二个返回值为 false。
	Get(id ObjectID) (*Object, bool)
	// List 按 Kind 列出全部对象的最新快照。
	List(kind Kind) []*Object

	// Create 创建新对象。若同名对象已存在返回错误；若属主处于
	// DeletionMarked 状态（孤儿防护）也返回错误。
	Create(obj *Object) (*Object, error)
	// UpdateSpec 以 CAS 方式替换期望状态（Spec / Finalizers / 删除标记），
	// 内容变化后 Generation 与 ResourceVersion 均递增；Status 保持不变。
	// 内容与当前完全一致时为空操作（不递增版本、不计数）。
	UpdateSpec(obj *Object) (*Object, error)
	// UpdateStatus 以 CAS 方式仅替换状态，内容变化后只递增 ResourceVersion。
	UpdateStatus(obj *Object) (*Object, error)
	// RemoveFinalizer 以 CAS 方式移除指定 finalizer；不存在时视为空操作成功。
	RemoveFinalizer(id ObjectID, rv int64, finalizer string) (*Object, error)
	// MarkDeleting 模拟用户删除：置 DeletionMarked 并递增 Generation。
	MarkDeleting(id ObjectID) (*Object, error)

	// Delete 在无 finalizer 时物理删除对象；否则返回错误。
	Delete(id ObjectID) error

	// Subscribe 返回变更事件通道与取消订阅函数。
	Subscribe() (<-chan Event, func())

	// WriteCount 返回某对象内容被成功物理写入的次数
	// （Create + 内容真正变化的 Update），用于断言“已收敛对象不得被反复改写”。
	WriteCount(id ObjectID) int
}

// MemoryStore 是 Store 的内存实现。
type MemoryStore struct {
	mu     sync.RWMutex
	data   map[ObjectID]*Object
	rver   int64
	writes map[ObjectID]int
	subs   []chan Event
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:   make(map[ObjectID]*Object),
		writes: make(map[ObjectID]int),
	}
}

func (s *MemoryStore) Get(id ObjectID) (*Object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.data[id]
	return obj.Clone(), ok
}

func (s *MemoryStore) List(kind Kind) []*Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Object
	for _, obj := range s.data {
		if obj.ID.Kind == kind {
			out = append(out, obj.Clone())
		}
	}
	return out
}

func (s *MemoryStore) Create(obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[obj.ID]; exists {
		return nil, fmt.Errorf("object %s/%s already exists", obj.ID.Kind, obj.ID.Name)
	}
	if err := s.checkOwnerAdmissionLocked(obj); err != nil {
		return nil, err
	}

	c := obj.Clone()
	c.Generation = 1
	c.ResourceVersion = s.nextRVLocked()
	s.initStatusLocked(c)

	s.data[c.ID] = c
	s.writes[c.ID]++
	s.publishUpsertLocked(c)
	return c.Clone(), nil
}

func (s *MemoryStore) UpdateSpec(obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[obj.ID]
	if !ok {
		return nil, fmt.Errorf("object %s/%s not found", obj.ID.Kind, obj.ID.Name)
	}
	if cur.ResourceVersion != obj.ResourceVersion {
		return nil, errConflict
	}

	specEqual := reflect.DeepEqual(cur.Service, obj.Service) &&
		reflect.DeepEqual(cur.Endpoint, obj.Endpoint)
	finEqual := reflect.DeepEqual(cur.Finalizers, obj.Finalizers)
	// 删除标记单调不可逆：一旦置位，Spec 更新不能清除（等价于
	// Kubernetes deletionTimestamp 不可变）。删除与更新并发时删除意图必胜，
	// 过期快照里的 DeletionMarked=false 不得复活对象。
	nextMark := cur.DeletionMarked || obj.DeletionMarked
	markEqual := cur.DeletionMarked == nextMark
	if specEqual && finEqual && markEqual {
		return cur.Clone(), nil // 空操作：已收敛对象不得被反复改写
	}

	next := cur.Clone()
	next.Service = obj.Service.Clone()
	next.Endpoint = obj.Endpoint.Clone()
	next.Finalizers = append([]string(nil), obj.Finalizers...)
	next.DeletionMarked = nextMark
	next.Generation = cur.Generation + 1
	next.ResourceVersion = s.nextRVLocked()
	// 注意：Status（含 ObservedGen）原样保留。期望状态变化后旧状态自然过期，
	// 由协调器以最新 Generation 重新判定后回写；可恢复失败期间也保留
	// 上次成功写入的状态，不在这里清空。

	s.data[next.ID] = next
	s.writes[next.ID]++
	s.publishUpsertLocked(next)
	return next.Clone(), nil
}

func (s *MemoryStore) UpdateStatus(obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[obj.ID]
	if !ok {
		return nil, fmt.Errorf("object %s/%s not found", obj.ID.Kind, obj.ID.Name)
	}
	if cur.ResourceVersion != obj.ResourceVersion {
		return nil, errConflict
	}
	if reflect.DeepEqual(cur.ServiceStatus, obj.ServiceStatus) &&
		reflect.DeepEqual(cur.EndpointStatus, obj.EndpointStatus) {
		return cur.Clone(), nil // 状态未变：不产生重复写入
	}

	next := cur.Clone()
	next.ServiceStatus = obj.ServiceStatus.Clone()
	next.EndpointStatus = obj.EndpointStatus.Clone()
	next.ResourceVersion = s.nextRVLocked()

	s.data[next.ID] = next
	s.writes[next.ID]++
	s.publishUpsertLocked(next)
	return next.Clone(), nil
}

func (s *MemoryStore) RemoveFinalizer(id ObjectID, rv int64, finalizer string) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[id]
	if !ok {
		return nil, fmt.Errorf("object %s/%s not found", id.Kind, id.Name)
	}
	if cur.ResourceVersion != rv {
		return nil, errConflict
	}
	if !hasString(cur.Finalizers, finalizer) {
		return cur.Clone(), nil // 幂等：finalizer 已不在
	}

	next := cur.Clone()
	next.Finalizers = removeString(next.Finalizers, finalizer)
	next.Generation = cur.Generation + 1
	next.ResourceVersion = s.nextRVLocked()

	s.data[next.ID] = next
	s.writes[next.ID]++
	s.publishUpsertLocked(next)
	return next.Clone(), nil
}

func (s *MemoryStore) MarkDeleting(id ObjectID) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[id]
	if !ok {
		return nil, fmt.Errorf("object %s/%s not found", id.Kind, id.Name)
	}
	if cur.DeletionMarked {
		return cur.Clone(), nil
	}

	next := cur.Clone()
	next.DeletionMarked = true
	next.Generation = cur.Generation + 1
	next.ResourceVersion = s.nextRVLocked()
	if st := next.ServiceStatus; st != nil {
		st.Phase = PhaseDeleting
	}
	if st := next.EndpointStatus; st != nil {
		st.Phase = PhaseDeleting
	}

	s.data[next.ID] = next
	s.writes[next.ID]++
	s.publishUpsertLocked(next)
	return next.Clone(), nil
}

func (s *MemoryStore) Delete(id ObjectID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.data[id]
	if !ok {
		return nil // 幂等：目标已不存在
	}
	if len(cur.Finalizers) > 0 {
		return fmt.Errorf("object %s/%s still has finalizers %v", id.Kind, id.Name, cur.Finalizers)
	}
	delete(s.data, id)
	s.publishLocked(Event{Type: EventDelete, ID: id})
	return nil
}

func (s *MemoryStore) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 1024)
	s.mu.Lock()
	s.subs = append(s.subs, ch)
	s.mu.Unlock()

	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, sub := range s.subs {
			if sub == ch {
				s.subs = append(s.subs[:i], s.subs[i+1:]...)
				close(ch)
				return
			}
		}
	}
	return ch, cancel
}

func (s *MemoryStore) WriteCount(id ObjectID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writes[id]
}

// ---- 内部辅助（调用方持锁） ----

func (s *MemoryStore) nextRVLocked() int64 {
	s.rver++
	return s.rver
}

// checkOwnerAdmissionLocked 实施孤儿防护：属主正在删除时不允许创建派生对象，
// 避免“删除与新建竞争”留下无法回收的残留对象。
func (s *MemoryStore) checkOwnerAdmissionLocked(obj *Object) error {
	if obj.Endpoint == nil || obj.Endpoint.Owner.Name == "" {
		return nil
	}
	owner, ok := s.data[ObjectID{Kind: KindService, Name: obj.Endpoint.Owner.Name}]
	if !ok {
		return fmt.Errorf("%w: endpoint %s references nonexistent service %s",
			errConflict, obj.ID.Name, obj.Endpoint.Owner.Name)
	}
	if owner.DeletionMarked {
		return fmt.Errorf("%w: service %s is being deleted, endpoint %s is rejected",
			errConflict, owner.ID.Name, obj.ID.Name)
	}
	return nil
}

func (s *MemoryStore) initStatusLocked(o *Object) {
	if o.Service != nil && o.ServiceStatus == nil {
		o.ServiceStatus = &ServiceStatus{Phase: PhasePending}
	}
	if o.Endpoint != nil && o.EndpointStatus == nil {
		o.EndpointStatus = &EndpointStatus{Phase: PhasePending}
	}
}

func (s *MemoryStore) publishUpsertLocked(o *Object) {
	ev := Event{Type: EventUpsert, ID: o.ID}
	if o.Endpoint != nil {
		ev.Owner = o.Endpoint.Owner.Name
	}
	s.publishLocked(ev)
}

func (s *MemoryStore) publishLocked(ev Event) {
	for _, ch := range s.subs {
		select {
		case ch <- ev:
		default:
			// 订阅者缓冲已满时丢弃事件；Controller 以安全重扫兜底，不阻塞写入。
		}
	}
}

func hasString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func removeString(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

// 规格 / 状态的深拷贝辅助。
func (s *ServiceSpec) Clone() *ServiceSpec {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

func (e *EndpointSpec) Clone() *EndpointSpec {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

func (s *ServiceStatus) Clone() *ServiceStatus {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

func (e *EndpointStatus) Clone() *EndpointStatus {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}
