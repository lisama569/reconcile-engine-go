// Package reconcile 实现一个本地运行的期望状态协调引擎。
//
// 核心概念：
//   - [Object]：被管理的对象，分 Owner（Service）与 Derived（Endpoint）两类；
//   - [Store]：带版本号的对象存储，所有写入都必须通过 CAS 校验资源版本；
//   - [ExternalSystem]：对象外部副作用的执行器（幂等）；
//   - [Reconciler]：单对象的期望状态 -> 实际状态判定与执行器；
//   - [Controller]：多对象调度器，负责按 key 串行、跨 key 并行、退避重试与事件唤醒。
package reconcile

import "time"

// Kind 标识对象的资源类别。
type Kind string

const (
	// KindService 属主对象，其期望状态声明需要几个派生对象。
	KindService Kind = "Service"
	// KindEndpoint 派生对象，由 Service 派生并持有 OwnerRef。
	KindEndpoint Kind = "Endpoint"
)

// Phase 是对象状态所处的阶段（Status 字段）。
type Phase string

const (
	// PhasePending 尚未完成任何一次成功协调。
	PhasePending Phase = "Pending"
	// PhaseApplied 期望状态已成功落地（外部副作用已就位）。
	PhaseApplied Phase = "Applied"
	// PhaseTerminal 遇到不可恢复的配置错误，永久终止重试。
	PhaseTerminal Phase = "Terminal"
	// PhaseDeleting 对象正在被删除，等待协调器完成级联清理并移除 finalizer。
	PhaseDeleting Phase = "Deleting"
)

// ObjectID 是对象在 Store 中的复合主键。
type ObjectID struct {
	Kind Kind
	Name string
}

// OwnerRef 指向派生对象的属主。
type OwnerRef struct {
	Name string
}

// ServiceSpec 是 Service 对象的期望状态。
type ServiceSpec struct {
	// Replicas 期望派生的 Endpoint 数量。
	Replicas int
	// Invalid 为 true 时表示不可恢复的配置错误。
	Invalid bool
}

// EndpointSpec 是 Endpoint 对象的期望状态。
type EndpointSpec struct {
	Owner    OwnerRef
	Image    string
	Capacity int
}

// ServiceStatus 记录 Service 的观测状态。
type ServiceStatus struct {
	Phase       Phase
	ObservedGen int64
	// ReadyReplicas 已确认外部副作用就位的派生对象数量。
	ReadyReplicas int
	// Message 在 Terminal 阶段给出可区分的失败原因。
	Message string
}

// EndpointStatus 记录 Endpoint 的观测状态。
type EndpointStatus struct {
	Phase       Phase
	ObservedGen int64
	// Provisioned 外部副作用（外部资源）是否已创建。
	Provisioned bool
	Message     string
}

// Object 是 Store 中保存的对象快照。
type Object struct {
	ID ObjectID

	// Generation 每次期望状态（Spec / finalizer / 删除标记）变更时由 Store 递增。
	Generation int64
	// ResourceVersion 每次对象内容（含 Status）变化时由 Store 递增，用于 CAS。
	ResourceVersion int64

	// DeletionMarked 表示用户已发起删除，协调器只能做清理，不能再创建副作用。
	DeletionMarked bool
	// Finalizers 非空时 Store 拒绝物理删除；协调器清理完后移除。
	Finalizers []string

	Service  *ServiceSpec
	Endpoint *EndpointSpec

	ServiceStatus  *ServiceStatus
	EndpointStatus *EndpointStatus
}

// Clone 返回对象的深拷贝，避免调用方与存储共享底层切片。
func (o *Object) Clone() *Object {
	if o == nil {
		return nil
	}
	c := &Object{
		ID:              o.ID,
		Generation:      o.Generation,
		ResourceVersion: o.ResourceVersion,
		DeletionMarked:  o.DeletionMarked,
		Finalizers:      append([]string(nil), o.Finalizers...),
	}
	if o.Service != nil {
		v := *o.Service
		c.Service = &v
	}
	if o.Endpoint != nil {
		v := *o.Endpoint
		c.Endpoint = &v
	}
	if o.ServiceStatus != nil {
		v := *o.ServiceStatus
		c.ServiceStatus = &v
	}
	if o.EndpointStatus != nil {
		v := *o.EndpointStatus
		c.EndpointStatus = &v
	}
	return c
}

// EventType 标识 Store 变更事件的类型。
type EventType string

const (
	EventUpsert EventType = "Upsert"
	EventDelete EventType = "Delete"
)

// Event 表示一次 Store 变更。
type Event struct {
	Type EventType
	ID   ObjectID
	// Owner 为派生对象所属属主的名字（仅 Upsert 可填充）；
	// 用于在派生对象变更时立即唤醒属主协调。Delete 事件对象已消失，
	// 属主由其自身的安全重扫兜底发现派生对象已消失。
	Owner string
}

// Result 是单次协调的判定结论。
type Result struct {
	// Requeue 为 true 时即使没有新事件也需要在 Delay 后再次协调（退避重试 / 安全重扫）。
	Requeue bool
	Delay   Duration
}

// Duration 是本包使用的时间长度类型。
type Duration = time.Duration

// Logger 是单测可观察的判定日志接口。
type Logger interface {
	Logf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Logf(string, ...any) {}
