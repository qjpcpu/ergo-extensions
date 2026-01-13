package system

import (
	"testing"
)

func TestImmutableList_GetAllReturnsCopy(t *testing.T) {
	l := NewImmutableList([]int{1, 2})
	all := l.GetAll()
	all[0] = 9

	if got, ok := l.Get(0); !ok || got != 1 {
		t.Fatalf("Get(0) got %v %v, want 1 true", got, ok)
	}
}

func TestImmutableList_NilVsEmpty(t *testing.T) {
	nilList := NewImmutableList[int](nil)
	if nilList.Len() != 0 {
		t.Fatalf("nilList.Len() got %d, want 0", nilList.Len())
	}
	if nilList.GetAll() != nil {
		t.Fatalf("nilList.GetAll() should be nil")
	}

	empty := NewImmutableList([]int{})
	if empty.Len() != 0 {
		t.Fatalf("empty.Len() got %d, want 0", empty.Len())
	}
	if got := empty.GetAll(); got == nil {
		t.Fatalf("empty.GetAll() should be non-nil empty slice")
	}
}
