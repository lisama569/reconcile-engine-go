package reconcile

import (
	"fmt"
	"testing"
	"time"
)

// testLogger 把每次判定的输入、观测版本与依据打印到单测日志
// （go test -v 可见），满足“可解释”的要求。
type testLogger struct{ t *testing.T }

func (l testLogger) Logf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("%s", fmt.Sprintf(format, args...))
}

func newEngine(t *testing.T) (Store, *FakeExternal, *Engine) {
	t.Helper()
	store := NewMemoryStore()
	ext := NewFakeExternal()
	return store, ext, NewReconciler(store, ext, testLogger{t})
}

func mustCreate(t *testing.T, store Store, obj *Object) *Object {
	t.Helper()
	out, err := store.Create(obj)
	if err != nil {
		t.Fatalf("Create %s/%s: %v", obj.ID.Kind, obj.ID.Name, err)
	}
	return out
}

func newService(name string, replicas int) *Object {
	return &Object{
		ID:            ObjectID{Kind: KindService, Name: name},
		Finalizers:    []string{FinalizerReconcile},
		Service:       &ServiceSpec{Replicas: replicas},
		ServiceStatus: &ServiceStatus{Phase: PhasePending},
	}
}

func newEndpoint(name, owner string, capacity int) *Object {
	if capacity == 0 {
		capacity = 100
	}
	return &Object{
		ID:         ObjectID{Kind: KindEndpoint, Name: name},
		Finalizers: []string{FinalizerReconcile},
		Endpoint: &EndpointSpec{
			Owner:    OwnerRef{Name: owner},
			Image:    "default",
			Capacity: capacity,
		},
		EndpointStatus: &EndpointStatus{Phase: PhasePending},
	}
}

// eventually 轮询断言直到成功或超时（测试时钟放大时可调整）。
func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
