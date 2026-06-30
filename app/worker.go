package app

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

const routeProcessName = gen.Atom("app_routes")

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
		// Use a wide default pool so hot forwarding does not serialize on slow
		// directory lookups or remote delivery checks.
		p.size = 128
	}
	opts := act.PoolOptions{
		WorkerFactory: p.fac,
		PoolSize:      p.size,
	}

	return opts, nil
}

type messageNodeSend struct {
	to        string
	toNode    gen.Atom
	msg       any
	important bool
	ch        chan nodeResult
}

type messageNodeCall struct {
	to        string
	toNode    gen.Atom
	msg       any
	timeout   int
	important bool
	ch        chan nodeResult
}

type messagePIDSend struct {
	to        gen.PID
	msg       any
	important bool
	ch        chan nodeResult
}

type messagePIDCall struct {
	to        gen.PID
	msg       any
	timeout   int
	important bool
	ch        chan nodeResult
}

type messageWaitProcess struct {
	PID gen.PID
	Ch  chan error
}

type messageSpawnProcess struct {
	Name    gen.Atom
	Factory gen.ProcessFactory
	Options gen.ProcessOptions
	Args    []any
	Ch      chan nodeResult
}

type routeActor struct {
	act.Actor
	monitorPID map[gen.PID]chan error
	book       system.IAddressBook
	hints      *routeHintCache
}

func newRouteActor(book system.IAddressBook, hints *routeHintCache) *routeActor {
	return &routeActor{
		monitorPID: make(map[gen.PID]chan error),
		book:       book,
		hints:      hints,
	}
}

func (w *routeActor) Init(args ...any) error {
	return nil
}

func (w *routeActor) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageNodeSend:
		e.ch <- nodeResult{err: w.forwardSend(e.to, e.toNode, e.msg, e.important)}
	case messagePIDSend:
		e.ch <- nodeResult{err: w.sendToPID(e.to, e.msg, e.important)}
	case messageNodeCall:
		var res any
		var err error
		var p gen.Atom
		if e.toNode != "" {
			p = e.toNode
		} else {
			p, err = w.book.QueryBy(w, system.QueryOption{Timeout: e.timeout}).Locate(gen.Atom(e.to))
		}
		if err != nil || p == "" || w.Node().Name() == p {
			if e.important {
				res, err = w.CallImportant(gen.Atom(e.to), e.msg)
			} else if e.timeout > 0 {
				res, err = w.CallWithTimeout(gen.Atom(e.to), e.msg, e.timeout)
			} else {
				res, err = w.Call(gen.Atom(e.to), e.msg)
			}
		} else {
			if e.important {
				res, err = w.CallImportant(gen.ProcessID{Node: p, Name: gen.Atom(e.to)}, e.msg)
			} else if e.timeout > 0 {
				res, err = w.CallWithTimeout(gen.ProcessID{Node: p, Name: gen.Atom(e.to)}, e.msg, e.timeout)
			} else {
				res, err = w.Call(gen.ProcessID{Node: p, Name: gen.Atom(e.to)}, e.msg)
			}
		}
		e.ch <- nodeResult{response: res, err: err}
	case messagePIDCall:
		res, err := w.callPID(e.to, e.msg, e.timeout, e.important)
		e.ch <- nodeResult{response: res, err: err}
	case messageWaitProcess:
		if err := w.MonitorPID(e.PID); err != nil {
			e.Ch <- err
			return nil
		}
		w.monitorPID[e.PID] = e.Ch
	case messageSpawnProcess:
		sendResp := func(pid gen.PID, err error) {
			if e.Ch != nil {
				e.Ch <- nodeResult{response: pid, err: err}
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
			sendResp(gen.PID{}, err)
			return nil
		}
		if e.Name != "" {
			w.Send(system.WhereIsProcess, system.MessageRegisterLocalProcess{
				Name: e.Name,
				PID:  pid,
			})
		}
		sendResp(pid, nil)
	case gen.MessageDownPID:
		if ch, ok := w.monitorPID[e.PID]; ok {
			delete(w.monitorPID, e.PID)
			if e.Reason == gen.TerminateReasonNormal {
				ch <- nil
			} else {
				ch <- e.Reason
			}
			w.DemonitorPID(e.PID)
		}
	}
	return nil
}

func (w *routeActor) forwardSend(to string, node gen.Atom, msg any, important bool) error {
	if node != "" {
		return w.sendToNode(to, node, msg, important)
	}
	process := gen.Atom(to)
	now := time.Now()
	if cachedNode, ok := w.hints.get(process, now); ok {
		if err := w.sendToNode(to, cachedNode, msg, important); err == nil {
			w.hints.touch(process, cachedNode, now)
			return nil
		}
		w.hints.invalidate(process)
	}
	resolvedNode, err := w.book.QueryBy(w, system.QueryOption{}).Locate(process)
	if err != nil {
		return err
	}
	if resolvedNode == "" {
		return gen.ErrProcessUnknown
	}
	if err := w.sendToNode(to, resolvedNode, msg, important); err != nil {
		w.hints.invalidate(process)
		return err
	}
	w.hints.set(process, resolvedNode, now)
	return nil
}

func (w *routeActor) sendToNode(to string, node gen.Atom, msg any, important bool) error {
	process := gen.Atom(to)
	if important {
		if node == w.Node().Name() {
			return w.SendImportant(process, msg)
		}
		return w.SendImportant(gen.ProcessID{Node: node, Name: process}, msg)
	}
	if node == w.Node().Name() {
		return w.Send(process, msg)
	}
	return w.Send(gen.ProcessID{Node: node, Name: process}, msg)
}

func (w *routeActor) sendToPID(to gen.PID, msg any, important bool) error {
	if important {
		return w.SendImportant(to, msg)
	}
	if to.Node == w.Node().Name() {
		return w.Send(to, msg)
	}
	return w.Send(to, msg)
}

func (w *routeActor) callPID(to gen.PID, msg any, timeout int, important bool) (any, error) {
	if important {
		return w.CallImportant(to, msg)
	}
	return w.CallPID(to, msg, timeout)
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
