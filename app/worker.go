package app

import (
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type myPool struct {
	act.Pool
	size int64
	fac  gen.ProcessFactory
}

// CreatePool creates a process factory for a worker pool with the specified size and worker factory.
func CreatePool(workerFactory gen.ProcessFactory, size int64) gen.ProcessFactory {
	return func() gen.ProcessBehavior { return &myPool{size: size, fac: workerFactory} }
}

func (p *myPool) Init(args ...any) (act.PoolOptions, error) {
	if p.size == 0 {
		p.size = 3
	}
	opts := act.PoolOptions{
		WorkerFactory: p.fac,
		PoolSize:      p.size,
	}

	return opts, nil
}

// tempActor is a short-lived actor that executes a function and exits.
type tempActor struct {
	act.Actor
	book system.IAddressBook
	fn   func(*tempActor)
}

func (w *tempActor) Init(args ...any) error {
	w.Send(w.PID(), "start")
	return nil
}

func (w *tempActor) HandleMessage(from gen.PID, message any) error {
	if message == "start" {
		w.fn(w)
		return gen.TerminateReasonNormal
	}
	return nil
}

// monitorActor monitors a PID and reports its exit via a channel.
type monitorActor struct {
	act.Actor
	setup func(w *monitorActor) error
	ch    chan error
	pid   gen.PID
}

func (w *monitorActor) Init(args ...any) error {
	w.Send(w.PID(), "start")
	return nil
}

func (w *monitorActor) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case string:
		if e == "start" {
			if err := w.setup(w); err != nil {
				w.ch <- err
				return gen.TerminateReasonNormal
			}
		}
	case gen.MessageDownPID:
		if e.PID == w.pid {
			if e.Reason == gen.TerminateReasonNormal {
				w.ch <- nil
			} else {
				w.ch <- e.Reason
			}
			return gen.TerminateReasonNormal
		}
	}
	return nil
}

type nodeResult struct {
	response any
	err      error
}

func NewCaller(process gen.Process) *Caller {
	return &Caller{process: process}
}

type Caller struct {
	process gen.Process
}

func (caller *Caller) Send(to gen.Atom, msg any) error {
	res, err := caller.process.Call(system.WhereIsProcess, system.MessageLocate{Name: to})
	if err != nil {
		return err
	}
	node, ok := res.(gen.Atom)
	if !ok || node == "" {
		return gen.ErrProcessUnknown
	}
	if node == caller.process.Node().Name() {
		return caller.process.Send(to, msg)
	}
	return caller.process.SendImportant(gen.ProcessID{Node: node, Name: to}, msg)
}

func (caller *Caller) Call(to gen.Atom, msg any) (any, error) {
	res, err := caller.process.Call(system.WhereIsProcess, system.MessageLocate{Name: to})
	if err != nil {
		return nil, err
	}
	node, ok := res.(gen.Atom)
	if !ok || node == "" {
		return nil, gen.ErrProcessUnknown
	}
	if node == caller.process.Node().Name() {
		return caller.process.Call(to, msg)
	}
	return caller.process.Call(gen.ProcessID{Node: node, Name: to}, msg)
}
