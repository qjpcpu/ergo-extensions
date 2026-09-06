package app

import (
	"fmt"
	"runtime/debug"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

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

func ProtectActor(a IActor) IActor { return &panicAsErrorActor{IActor: a} }

func ProtectMetaActor(a IMetaActor) IMetaActor { return &panicAsErrorMetaActor{IMetaActor: a} }

type PanicError struct {
	r     any
	stack string
}

func newPanicError(r any, stack string) PanicError {
	return PanicError{r: r, stack: stack}
}

func (e PanicError) Error() string {
	return fmt.Sprintf("panic:%v stack: %s", e.r, e.stack)
}

type panicAsErrorMetaActor struct {
	IMetaActor
}

func (a *panicAsErrorMetaActor) Init(meta gen.MetaProcess) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IMetaActor.Init(meta)
}

func (a *panicAsErrorMetaActor) Start() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IMetaActor.Start()
}

func (a *panicAsErrorMetaActor) HandleMessage(from gen.PID, message any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IMetaActor.HandleMessage(from, message)
}

func (a *panicAsErrorMetaActor) HandleCall(from gen.PID, ref gen.Ref, request any) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IMetaActor.HandleCall(from, ref, request)
}

func (a *panicAsErrorMetaActor) HandleInspect(from gen.PID, item ...string) (result map[string]string) {
	defer func() {
		if r := recover(); r != nil {
			result = map[string]string{
				"panic": fmt.Sprint(r),
				"stack": string(debug.Stack()),
			}
		}
	}()
	return a.IMetaActor.HandleInspect(from, item...)
}

type panicAsErrorActor struct {
	IActor
}

func (a *panicAsErrorActor) Init(args ...any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IActor.Init(args...)
}

func (a *panicAsErrorActor) HandleCall(from gen.PID, ref gen.Ref, request any) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IActor.HandleCall(from, ref, request)
}

func (a *panicAsErrorActor) HandleMessage(from gen.PID, message any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IActor.HandleMessage(from, message)
}

func (a *panicAsErrorActor) HandleInspect(from gen.PID, item ...string) (result map[string]string) {
	defer func() {
		if r := recover(); r != nil {
			result = map[string]string{
				"panic": fmt.Sprint(r),
				"stack": string(debug.Stack()),
			}
		}
	}()
	return a.IActor.HandleInspect(from, item...)
}

func (a *panicAsErrorActor) HandleLog(message gen.MessageLog) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IActor.HandleLog(message)
}

func (a *panicAsErrorActor) HandleEvent(message gen.MessageEvent) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r, string(debug.Stack()))
		}
	}()
	return a.IActor.HandleEvent(message)
}
