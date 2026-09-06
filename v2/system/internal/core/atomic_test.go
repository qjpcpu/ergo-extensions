package core

import "testing"

func TestAtomicValueDefaultAndStore(t *testing.T) {
	value := NewAtomicValue[int]()
	if got := value.Load(); got != 0 {
		t.Fatalf("expected zero default, got %d", got)
	}
	if stored := value.Store(42); stored != 42 {
		t.Fatalf("store returned %d", stored)
	}
	if got := value.Load(); got != 42 {
		t.Fatalf("expected stored value, got %d", got)
	}

	withInitial := NewAtomicValue("initial")
	if got := withInitial.Load(); got != "initial" {
		t.Fatalf("expected initial value, got %q", got)
	}
}
