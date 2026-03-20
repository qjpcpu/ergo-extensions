package app

import (
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/lib"
)

type Queue[T any] interface {
	Push(T) bool
	Pop() (T, bool)
	Peek() (T, bool)
	Range(fn func(T) bool)
	Len() int
}

func NewQueue[T any]() Queue[T] {
	return &simpleQueue[T]{impl: lib.NewQueueMPSC()}
}

func NewLimitQueue[T any](size int64) Queue[T] {
	return &simpleQueue[T]{impl: lib.NewQueueLimitMPSC(size, false)}
}

type simpleQueue[T any] struct {
	impl lib.QueueMPSC
}

func (q *simpleQueue[T]) Push(msg T) bool {
	return q.impl.Push(msg)
}

func (q *simpleQueue[T]) Pop() (msg T, ok bool) {
	var v any
	v, ok = q.impl.Pop()
	if !ok {
		return
	}
	return v.(T), true
}

func (q *simpleQueue[T]) Peek() (msg T, ok bool) {
	if item := q.impl.Item(); item != nil {
		return item.Value().(T), true
	}
	return
}

func (q *simpleQueue[T]) Range(fn func(T) bool) {
	item := q.impl.Item()
	for item != nil {
		if !fn(item.Value().(T)) {
			return
		}
		item = item.Next()
	}
}

func (q *simpleQueue[T]) Len() int {
	return int(q.impl.Len())
}

type IActor interface {
	gen.Process
	act.ActorBehavior
	Init(args ...any) error
	HandleCall(from gen.PID, ref gen.Ref, request any) (any, error)
	HandleMessage(from gen.PID, message any) error
	HandleInspect(from gen.PID, item ...string) map[string]string
	HandleLog(message gen.MessageLog) error
	HandleEvent(message gen.MessageEvent) error
	Terminate(reason error)
}

type IMetaActor interface {
	gen.MetaProcess
	Init(meta gen.MetaProcess) error
	Start() error
	HandleMessage(from gen.PID, message any) error
	HandleCall(from gen.PID, ref gen.Ref, request any) (any, error)
	Terminate(reason error)
	HandleInspect(from gen.PID, item ...string) map[string]string
}
