package core

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

func TestImmutableListExistAndRange(t *testing.T) {
	list := NewImmutableList([]int{1, 2, 3})
	if !list.Exist(2) {
		t.Fatal("expected element to exist")
	}
	if list.Exist(4) {
		t.Fatal("unexpected element")
	}

	var visited []int
	list.Range(func(v int) bool {
		visited = append(visited, v)
		return v != 2
	})
	if len(visited) != 2 || visited[0] != 1 || visited[1] != 2 {
		t.Fatalf("unexpected range visit: %+v", visited)
	}
}
