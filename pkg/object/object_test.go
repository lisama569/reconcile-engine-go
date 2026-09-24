package object

import "testing"

func TestDeepCopyIsIndependent(t *testing.T) {
	o := &Object{
		Key:        "obj-1",
		Finalizers: []string{FinalizerOwnerCleanup},
		Spec:       Spec{DesiredItems: map[string]string{"a": "1"}},
		Status:     Status{ReadyItems: map[string]string{"a": "1"}},
	}
	cp := o.DeepCopy()
	cp.Finalizers[0] = "mutated"
	cp.Spec.DesiredItems["a"] = "2"
	cp.Spec.DesiredItems["b"] = "3"
	cp.Status.ReadyItems["a"] = "9"

	if o.Finalizers[0] != FinalizerOwnerCleanup {
		t.Fatalf("original finalizer mutated: %v", o.Finalizers)
	}
	if o.Spec.DesiredItems["a"] != "1" || len(o.Spec.DesiredItems) != 1 {
		t.Fatalf("original spec map mutated: %v", o.Spec.DesiredItems)
	}
	if o.Status.ReadyItems["a"] != "1" {
		t.Fatalf("original status map mutated: %v", o.Status.ReadyItems)
	}
}

func TestFinalizers(t *testing.T) {
	o := &Object{}
	if o.HasFinalizer(FinalizerOwnerCleanup) {
		t.Fatal("empty object should not have finalizer")
	}
	o.AddFinalizer(FinalizerOwnerCleanup)
	o.AddFinalizer(FinalizerOwnerCleanup) // 去重
	if len(o.Finalizers) != 1 {
		t.Fatalf("add finalizer should dedup, got %v", o.Finalizers)
	}
	o.RemoveFinalizer(FinalizerOwnerCleanup)
	o.RemoveFinalizer(FinalizerOwnerCleanup) // 无操作
	if len(o.Finalizers) != 0 {
		t.Fatalf("remove finalizer failed, got %v", o.Finalizers)
	}
}

func TestMarkDeletedIdempotent(t *testing.T) {
	o := &Object{}
	if o.Deleting() {
		t.Fatal("new object should not be deleting")
	}
	o.MarkDeleted("2026-09-24T00:00:00Z")
	o.MarkDeleted("2026-09-24T01:00:00Z") // 幂等，不覆盖
	if !o.Deleting() {
		t.Fatal("object should be deleting")
	}
	if o.DeletionTimestamp != "2026-09-24T00:00:00Z" {
		t.Fatalf("MarkDeleted should be idempotent, got %q", o.DeletionTimestamp)
	}
}

func TestNilDeepCopy(t *testing.T) {
	var o *Object
	if o.DeepCopy() != nil {
		t.Fatal("nil deep copy should be nil")
	}
}
