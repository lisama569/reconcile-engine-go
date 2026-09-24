package reconcile

import (
	"context"
	"sync"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// ApplyRequest 描述一次副作用调用。
// Token 是该副作用项的幂等键（即 Spec.DesiredItems 的键）。
// 副作用的幂等性分两层保证：
//  1. 协调器只对 Status.ReadyItems 之外的 token 下发 Apply
//     （"已收敛不重复"，重复协调不会重复调用）；
//  2. 外部执行器自身也应以 Token 去重，防御重试导致的重复创建。
type ApplyRequest struct {
	Object object.Key
	Kind   object.SideEffectKind
	Token  string
	Detail string
}

// Applier 模拟外部系统（真实实现可能是云 API）。
// 返回的 error 会被 Classify 归类：
//   - 包装为不可恢复错误（InvalidConfig）-> 终止重试；
//   - 其他错误 -> 可恢复，退避重试，且不回滚已成功项。
type Applier interface {
	Apply(ctx context.Context, req ApplyRequest) error
}

// ApplierFunc 便于用函数实现 Applier。
type ApplierFunc func(ctx context.Context, req ApplyRequest) error

// Apply 实现 Applier。
func (f ApplierFunc) Apply(ctx context.Context, req ApplyRequest) error {
	return f(ctx, req)
}

// CountingApplier 统计真实副作用调用次数，供测试断言"无重复副作用"。
// 它同时按 (Object, Kind, Token) 做第二层幂等去重：
// 同一副作用一旦成功完成，后续重复调用直接返回成功、不再下发到内层。
type CountingApplier struct {
	mu    sync.Mutex
	Calls []ApplyRequest
	Inner Applier
	done  map[string]bool
}

// NewCountingApplier 包装一个底层 Applier。
func NewCountingApplier(inner Applier) *CountingApplier {
	return &CountingApplier{
		Calls: []ApplyRequest{},
		Inner: inner,
		done:  map[string]bool{},
	}
}

func id(req ApplyRequest) string {
	return string(req.Object) + "|" + string(req.Kind) + "|" + req.Token
}

// Apply 记录调用；已成功完成过的同幂等键直接短路。
func (a *CountingApplier) Apply(ctx context.Context, req ApplyRequest) error {
	a.mu.Lock()
	a.Calls = append(a.Calls, req)
	completed := a.done[id(req)]
	a.mu.Unlock()

	// 第二层幂等：模拟外部系统按幂等键去重。
	if completed {
		return nil
	}
	if err := a.Inner.Apply(ctx, req); err != nil {
		return err
	}
	a.mu.Lock()
	a.done[id(req)] = true
	a.mu.Unlock()
	return nil
}

// CallCount 返回某个 token 被实际下发（含去重前）的次数。
func (a *CountingApplier) CallCount(token string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, c := range a.Calls {
		if c.Token == token {
			n++
		}
	}
	return n
}

// TotalCalls 返回总调用次数。
func (a *CountingApplier) TotalCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.Calls)
}

// Snapshot 返回调用记录的副本。
func (a *CountingApplier) Snapshot() []ApplyRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ApplyRequest, len(a.Calls))
	copy(out, a.Calls)
	return out
}
