package store

import (
	"errors"
	"testing"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

func obj(key object.Key) *object.Object {
	return &object.Object{Key: key, Spec: object.Spec{DesiredItems: map[string]string{"x": "1"}}}
}

func TestNewAssignsVersions(t *testing.T) {
	s := New(obj("a"), obj("b"))
	a, err := s.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := s.Get("b")
	if a.ResourceVersion != 1 || b.ResourceVersion != 2 {
		t.Fatalf("seed versions want 1,2 got %d,%d", a.ResourceVersion, b.ResourceVersion)
	}
}

func TestCASConflictOnStaleVersion(t *testing.T) {
	s := New(obj("a"))
	first, _ := s.Get("a")

	// 其他人先用最新版本完成一次写入。
	fresh := first.DeepCopy()
	fresh.Status.Reason = "writer-1"
	if _, err := s.CAS(fresh, first.ResourceVersion); err != nil {
		t.Fatalf("first CAS should succeed: %v", err)
	}

	// 旧快照再写：必须冲突，绝不允许覆盖较新状态。
	stale := first.DeepCopy()
	stale.Status.Reason = "stale-writer"
	_, err := s.CAS(stale, first.ResourceVersion)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS must be ErrConflict, got %v", err)
	}
	cur, _ := s.Get("a")
	if cur.Status.Reason != "writer-1" {
		t.Fatalf("newer state must not be overwritten, got %q", cur.Status.Reason)
	}
}

func TestCASSuccessBumpsVersion(t *testing.T) {
	s := New(obj("a"))
	o, _ := s.Get("a")
	o.Status.Reason = "v2"
	stored, err := s.CAS(o, o.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ResourceVersion != 2 {
		t.Fatalf("want new RV 2, got %d", stored.ResourceVersion)
	}
	if s.Version("a") != 2 {
		t.Fatalf("Version() want 2, got %d", s.Version("a"))
	}
}

func TestCreateDuplicateConflict(t *testing.T) {
	s := New()
	if _, err := s.Create(obj("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(obj("a")); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate create must conflict, got %v", err)
	}
}

func TestDeleteVersioned(t *testing.T) {
	s := New(obj("a"))
	o, _ := s.Get("a")
	if err := s.Delete("a", o.ResourceVersion-1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete must conflict, got %v", err)
	}
	if err := s.Delete("a", o.ResourceVersion); err != nil {
		t.Fatalf("fresh delete should succeed, got %v", err)
	}
	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
}

func TestListChildrenAndNotify(t *testing.T) {
	parent := &object.Object{Key: "p"}
	child1 := &object.Object{Key: "c1", Owner: "p"}
	child2 := &object.Object{Key: "c2", Owner: "p"}
	other := &object.Object{Key: "o", Owner: "someone"}
	s := New(parent, child1, child2, other)

	kids := s.ListChildren("p")
	if len(kids) != 2 {
		t.Fatalf("want 2 children, got %d", len(kids))
	}
	if got := len(s.List()); got != 4 {
		t.Fatalf("List want 4, got %d", got)
	}

	n := s.Notify()
	s.Create(&object.Object{Key: "c3", Owner: "p"})
	select {
	case k := <-n:
		if k != "c3" {
			t.Fatalf("notify want c3, got %s", k)
		}
	default:
		t.Fatal("expected notify event after create")
	}
}
