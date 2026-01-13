package system

import (
	"sync"

	"ergo.services/ergo/gen"
)

type NodeList = ImmutableList[gen.Atom]

func NewNodeList(list ...gen.Atom) *NodeList {
	return NewImmutableList(list)
}

type ImmutableList[T comparable] struct {
	list []T
	once sync.Once
	keys map[T]struct{}
}

// The caller must not modify list after passing it to NewImmutableList.
func NewImmutableList[T comparable](list []T) *ImmutableList[T] {
	return &ImmutableList[T]{list: list}
}

func (self *ImmutableList[T]) ensureKeys() {
	self.once.Do(func() {
		if len(self.list) == 0 {
			return
		}
		keys := make(map[T]struct{}, len(self.list))
		for _, elem := range self.list {
			keys[elem] = struct{}{}
		}
		self.keys = keys
	})
}

func (self *ImmutableList[T]) Exist(elem T) bool {
	if len(self.list) == 0 {
		return false
	}
	self.ensureKeys()
	_, ok := self.keys[elem]
	return ok
}

func (self *ImmutableList[T]) Len() int {
	return len(self.list)
}

func (self *ImmutableList[T]) Get(i int) (elem T, ok bool) {
	if i >= 0 && i < len(self.list) {
		elem = self.list[i]
		ok = true
	}
	return
}

func (self *ImmutableList[T]) GetAll() (elems []T) {
	if self.list == nil {
		return nil
	}
	elems = make([]T, len(self.list))
	copy(elems, self.list)
	return
}

func (self *ImmutableList[T]) Range(fn func(T) bool) {
	for _, elem := range self.list {
		if !fn(elem) {
			break
		}
	}
}
