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

type messageNodeCallLocal struct {
	to  string
	msg any
	ch  chan nodeResult
}

type messageWaitProcess struct {
	PID gen.PID
	Ch  chan error
}

type messageSpawnProcess struct {
	Name    gen.Atom // Optional
	Factory gen.ProcessFactory
	Options gen.ProcessOptions // Optional
	Args    []any              // Optional
	Ch      chan error         // Optional
}

func (w *myworker) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageNodeSend:
		p, err := w.book.QueryBy(w, system.QueryOption{}).Locate(gen.Atom(e.to))
		if err != nil || p == "" || w.Node().Name() == p {
			e.ch <- nodeResult{err: w.Send(gen.Atom(e.to), e.msg)}
		} else {
			e.ch <- nodeResult{err: w.SendImportant(gen.ProcessID{Node: p, Name: gen.Atom(e.to)}, e.msg)}
		}
	case messageNodeCall:
		p, err := w.book.QueryBy(w, system.QueryOption{}).Locate(gen.Atom(e.to))
		if err != nil || p == "" || w.Node().Name() == p {
			res, err := w.Call(gen.Atom(e.to), e.msg)
			e.ch <- nodeResult{response: res, err: err}
		} else {
			res, err := w.CallImportant(gen.ProcessID{Node: p, Name: gen.Atom(e.to)}, e.msg)
			e.ch <- nodeResult{response: res, err: err}
		}
	case messageNodeCallLocal:
		res, err := w.Call(gen.Atom(e.to), e.msg)
		e.ch <- nodeResult{response: res, err: err}
	case messageSpawnProcess:
		sendResp := func(err error) {
			if e.Ch != nil {
				e.Ch <- err
			}
		}
		var pid gen.PID
		var err error
		if e.Name != "" {
			pid, err = w.SpawnRegister(e.Name, e.Factory, e.Options, e.Args...)
		} else {
			pid, err = w.Spawn(e.Factory, e.Options, e.Args...)
		}
		if err != nil {
			sendResp(err)
			return nil
		}
		if e.Ch != nil {
			err = w.MonitorPID(pid)
			if err != nil {
				w.Node().Kill(pid)
				sendResp(err)
				return nil
			}
			w.monitorPID[pid] = e.Ch
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
