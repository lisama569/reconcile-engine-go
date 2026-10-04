// Package object 定义协调引擎处理的领域对象：带版本号、软删除标记、
// 终结器（finalizer）、期望配置（Spec）与观测状态（Status）。
package object

// Key 唯一标识一个对象。
type Key string

// Phase 表示对象当前所处的协调阶段。
type Phase string

// SideEffectKind 描述期望触发的外部副作用类型。
type SideEffectKind string

const (
	// PhasePending 尚未完成收敛。
	PhasePending Phase = "Pending"
	// PhaseReady 已按期望收敛。
	PhaseReady Phase = "Ready"
	// PhaseFailed 失败（可重试或不可恢复）。
	PhaseFailed Phase = "Failed"
	// PhaseDeleting 正在级联删除。
	PhaseDeleting Phase = "Deleting"
)

const (
	// EffectProvision 创建/更新外部资源。
	EffectProvision SideEffectKind = "Provision"
	// EffectCleanup 回收外部资源。
	EffectCleanup SideEffectKind = "Cleanup"
)

// FinalizerOwnerCleanup 是所有者对象上的清理终结器：
// 只要派生对象（children）尚未全部回收，该终结器就不能摘除。
const FinalizerOwnerCleanup = "owner-cleanup"

// Spec 是对象的期望配置。
type Spec struct {
	// Generation 由外部写入方在"期望配置发生实质变化"时递增。
	// 协调器用它判断失败结论是否针对当前配置：配置未变时，
	// 不可恢复失败结论保持有效（不再重试、不再改写）。
	Generation int64
	// DesiredItems 期望外部系统中存在的副作用项（幂等键 -> 描述）。
	DesiredItems map[string]string
	// FailConfigured 为 true 时表示配置本身非法（不可恢复失败）。
	FailConfigured bool
	// RetryBudget 是该对象允许的最大连续可恢复失败次数：
	// 超过后进入"已放弃、需人工介入"状态，不再自动重试。
	// 0 表示使用引擎默认预算；负数表示不限。
	RetryBudget int
}

// Status 是最近一次成功协调观测到的状态。
// 协调失败时不得清空已成功写入的内容。
type Status struct {
	Phase      Phase
	ReadyItems map[string]string // 已确认存在的副作用项（上次成功写入的状态）
	Reason     string            // 失败原因（成功时为空）
	// FailureKind 是最近一次失败的机器可分类别
	// （"Transient"/"InvalidConfig"/"BudgetExhausted"，成功时为空），
	// 供收敛视图直接区分可恢复失败、配置性问题与预算耗尽。
	FailureKind string
	Retriable   bool   // 当前失败是否可重试
	Attempts    int    // 已尝试次数
	UpdatedAt   string // 最近一次成功写入时间（测试可读）
	// ObservedGeneration 是当前状态结论所针对的 Spec.Generation。
	ObservedGeneration int64
}

// Object 是协调的基本单位。
type Object struct {
	Key             Key
	ResourceVersion int64 // 乐观锁版本：每次写入 +1
	Owner           Key   // 非空表示派生对象（child）
	Finalizers      []string
	// DeletionTimestamp 非空表示被请求删除（软删除）。
	DeletionTimestamp string
	Spec              Spec
	Status            Status
}

// CloneMap 复制 map（nil 安全）。
func CloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// CloneSlice 复制切片。
func CloneSlice(s []string) []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

// DeepCopy 返回深拷贝，避免协调逻辑直接修改存储中的快照。
func (o *Object) DeepCopy() *Object {
	if o == nil {
		return nil
	}
	cp := *o
	cp.Finalizers = CloneSlice(o.Finalizers)
	cp.Spec.DesiredItems = CloneMap(o.Spec.DesiredItems)
	cp.Status.ReadyItems = CloneMap(o.Status.ReadyItems)
	return &cp
}

// HasFinalizer 判断是否带有指定终结器。
func (o *Object) HasFinalizer(name string) bool {
	for _, f := range o.Finalizers {
		if f == name {
			return true
		}
	}
	return false
}

// AddFinalizer 添加终结器（去重）。
func (o *Object) AddFinalizer(name string) {
	if o.HasFinalizer(name) {
		return
	}
	o.Finalizers = append(o.Finalizers, name)
}

// RemoveFinalizer 移除终结器（不存在时无操作）。
func (o *Object) RemoveFinalizer(name string) {
	out := o.Finalizers[:0]
	for _, f := range o.Finalizers {
		if f != name {
			out = append(out, f)
		}
	}
	o.Finalizers = out
}

// MarkDeleted 标记软删除（幂等）。
func (o *Object) MarkDeleted(now string) {
	if o.DeletionTimestamp == "" {
		o.DeletionTimestamp = now
	}
}

// Deleting 返回对象是否正被删除。
func (o *Object) Deleting() bool { return o.DeletionTimestamp != "" }
