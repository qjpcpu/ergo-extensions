package app

import (
	"testing"
)

func TestQueue(t *testing.T) {
	q := NewQueue[int]()
	if q.Len() != 0 {
		t.Errorf("expected length 0, got %d", q.Len())
	}
	if v, ok := q.Pop(); ok || v != 0 {
		t.Errorf("expected empty pop, got %v, %v", v, ok)
	}
	if v, ok := q.Peek(); ok || v != 0 {
		t.Errorf("expected empty peek, got %v, %v", v, ok)
	}

	q.Push(1)
	q.Push(2)
	q.Push(3)

	if q.Len() != 3 {
		t.Errorf("expected length 3, got %d", q.Len())
	}

	if v, ok := q.Peek(); !ok || v != 1 {
		t.Errorf("expected peek 1, got %v, %v", v, ok)
	}

	var sum int
	q.Range(func(v int) bool {
		sum += v
		return true
	})
	if sum != 6 {
		t.Errorf("expected sum 6, got %d", sum)
	}

	var first int
	q.Range(func(v int) bool {
		first = v
		return false
	})
	if first != 1 {
		t.Errorf("expected early range to see 1, got %d", first)
	}

	if v, ok := q.Pop(); !ok || v != 1 {
		t.Errorf("expected pop 1, got %v, %v", v, ok)
	}
	if q.Len() != 2 {
		t.Errorf("expected length 2, got %d", q.Len())
	}
}

func TestLimitQueue(t *testing.T) {
	q := NewLimitQueue[int](2)
	if !q.Push(1) {
		t.Error("push 1 failed")
	}
	if !q.Push(2) {
		t.Error("push 2 failed")
	}
	if q.Push(3) {
		t.Error("push 3 should fail")
	}
	if q.Len() != 2 {
		t.Errorf("expected length 2, got %d", q.Len())
	}
}
