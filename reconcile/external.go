package reconcile

import (
	"fmt"
	"sync"
)

// ExternalSystem 模拟对象背后的外部世界（如云资源）。
//
// 所有操作都必须是幂等的：同一对象重复协调时，只有第一次真正产生
// 外部状态变更，后续调用命中已存在状态，Mutations 不增长。
type ExternalSystem interface {
	// Provision 创建外部资源，幂等。返回 true 表示本次确实发生了创建（副作用+1），
	// false 表示资源此前已存在（纯幂等命中，无副作用）。
	Provision(ep *Object) (created bool, err error)
	// Delete 回收外部资源，幂等。返回 true 表示本次确实发生了删除（副作用+1）。
	Delete(ep *Object) (deleted bool, err error)
	// IsProvisioned 查询外部资源是否存在（不产生副作用）。
	IsProvisioned(id ObjectID) bool
	// ResourceIDs 返回外部世界当前持有的全部资源 ID，用于级联删除的残留校验。
	ResourceIDs() []ObjectID
	// Mutations 返回外部世界累计真实变更次数（创建+删除），用于断言副作用次数。
	Mutations() int
}

// FakeExternal 是 ExternalSystem 的内存模拟，支持注入可恢复故障。
type FakeExternal struct {
	mu sync.Mutex

	resources map[ObjectID]struct{}
	mutations int

	// provisionFail：资源尚不存在时，随后 n 次 Provision 返回可恢复错误。
	provisionFail int
	// ensureFail：资源已存在后，随后 n 次 Provision（幂等确认路径）
	// 仍返回可恢复错误，用于验证“保留上次成功写入的状态”。
	ensureFail int
	// deleteFail：随后 n 次 Delete（资源仍存在时）返回可恢复错误。
	deleteFail int
}

// NewFakeExternal 创建外部模拟器。
func NewFakeExternal() *FakeExternal {
	return &FakeExternal{resources: make(map[ObjectID]struct{})}
}

// SetProvisionFail 令随后 n 次“真正创建”返回可恢复错误。
func (f *FakeExternal) SetProvisionFail(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisionFail = n
}

// SetEnsureFail 令随后 n 次“对已存在资源的幂等确认”返回可恢复错误。
func (f *FakeExternal) SetEnsureFail(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureFail = n
}

// SetDeleteFail 令随后 n 次 Delete 返回可恢复错误。
func (f *FakeExternal) SetDeleteFail(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteFail = n
}

func (f *FakeExternal) Provision(ep *Object) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.resources[ep.ID]; ok {
		// 幂等命中路径：资源已存在，正常情况下不产生副作用。
		if f.ensureFail > 0 {
			f.ensureFail--
			return false, fmt.Errorf("external backend temporarily unavailable while confirming %s", ep.ID.Name)
		}
		return false, nil
	}
	if f.provisionFail > 0 {
		f.provisionFail--
		return false, fmt.Errorf("external backend temporarily unavailable while creating %s", ep.ID.Name)
	}
	f.resources[ep.ID] = struct{}{}
	f.mutations++
	return true, nil
}

func (f *FakeExternal) Delete(ep *Object) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.resources[ep.ID]; !ok {
		return false, nil // 幂等：已回收
	}
	if f.deleteFail > 0 {
		f.deleteFail--
		return false, fmt.Errorf("external backend temporarily unavailable while deleting %s", ep.ID.Name)
	}
	delete(f.resources, ep.ID)
	f.mutations++
	return true, nil
}

func (f *FakeExternal) IsProvisioned(id ObjectID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.resources[id]
	return ok
}

func (f *FakeExternal) ResourceIDs() []ObjectID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ObjectID, 0, len(f.resources))
	for id := range f.resources {
		out = append(out, id)
	}
	return out
}

func (f *FakeExternal) Mutations() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mutations
}
