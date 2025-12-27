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

type myworker struct {
	act.Actor
	monitorPID map[gen.PID]chan error
	book       system.IAddressBook
}

func (w *myworker) Init(args ...any) error {
	return nil
}

type nodeResult struct {
	response any
	err      error
}

type messageNodeSend struct {
	to  string
	msg any
	ch  chan nodeResult
}

type messageNodeCall struct {
	to  string
	msg any
	ch  chan nodeResult
}

type messageWaitProcess struct {
	PID gen.PID
	Ch  chan error
}

func (w *myworker) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageNodeSend:
		if p, ok := w.book.Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			e.ch <- nodeResult{err: w.Send(gen.Atom(e.to), e.msg)}
		} else {
			e.ch <- nodeResult{err: w.Send(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)}
		}
	case messageNodeCall:
		if p, ok := w.book.Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			res, err := w.Call(gen.Atom(e.to), e.msg)
			e.ch <- nodeResult{response: res, err: err}
		} else {
			res, err := w.Call(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)
			e.ch <- nodeResult{response: res, err: err}
		}
	case messageWaitProcess:
		if err := w.MonitorPID(e.PID); err != nil {
			e.Ch <- err
			return nil
		} else {
			w.monitorPID[e.PID] = e.Ch
		}
	case gen.MessageDownPID:
		if ch, ok := w.monitorPID[e.PID]; ok {
			delete(w.monitorPID, e.PID)
			if e.Reason == gen.TerminateReasonNormal {
				ch <- nil
			} else {
				ch <- e.Reason
			}
			w.Log().Info("PID:%s exit with reason %v", e.PID, e.Reason)
			w.DemonitorPID(e.PID)
		}
	}
	return nil
}
