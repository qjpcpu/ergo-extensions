package core

import "sync/atomic"

type AtomicValue[T any] struct {
	value atomic.Value
}

func NewAtomicValue[T any](v ...T) *AtomicValue[T] {
	av := &AtomicValue[T]{}
	if len(v) > 0 {
		av.value.Store(v[0])
	} else {
		var vv T
		av.value.Store(vv)
	}
	return av
}

func (self *AtomicValue[T]) Store(v T) T {
	self.value.Store(v)
	return v
}

func (self *AtomicValue[T]) Load() T {
	return self.value.Load().(T)
}
