package app

import (
	"context"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
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

type actorLocator interface {
	Locate(context.Context, gen.Atom) (gen.PID, bool, error)
}

type forwardRequest struct {
	ctx     context.Context
	message any
	ch      chan nodeResult
}

type routeActor struct {
	act.Actor
	monitorPID map[gen.PID]chan error
	locator    actorLocator
	request    *forwardRequest
}

func newRouteActor(locator actorLocator) *routeActor {
	return &routeActor{
		monitorPID: make(map[gen.PID]chan error),
		locator:    locator,
	}
}

func (w *routeActor) Init(args ...any) error {
	return nil
}

func (w *routeActor) HandleMessage(from gen.PID, message any) (reason error) {
	if request, ok := message.(forwardRequest); ok {
		if err := request.ctx.Err(); err != nil {
			request.ch <- nodeResult{err: forwardContextError(err)}
			return nil
		}
		w.request = &request
		defer func() {
			if recovered := recover(); recovered != nil {
				select {
				case request.ch <- nodeResult{err: gen.TerminateReasonPanic}:
				default:
				}
				reason = gen.TerminateReasonPanic
			}
			w.request = nil
		}()
		message = request.message
	}

	switch e := message.(type) {
	case messageNodeSend:
		e.ch <- nodeResult{err: w.forwardSend(e.to, e.toNode, e.msg, e.important)}
	case messagePIDSend:
		e.ch <- nodeResult{err: w.sendToPID(e.to, e.msg, e.important)}
	case messageNodeCall:
		var res any
		var err error
		e.timeout = w.remainingTimeout(e.timeout)
		if e.toNode != "" {
			if e.important {
				res, err = w.callImportantWithTimeout(gen.ProcessID{Node: e.toNode, Name: gen.Atom(e.to)}, e.msg, e.timeout)
			} else if e.timeout > 0 {
				res, err = w.CallWithTimeout(gen.ProcessID{Node: e.toNode, Name: gen.Atom(e.to)}, e.msg, e.timeout)
			} else {
				res, err = w.Call(gen.ProcessID{Node: e.toNode, Name: gen.Atom(e.to)}, e.msg)
			}
		} else {
			pid, found, locateErr := w.locate(gen.Atom(e.to), e.timeout)
			if locateErr != nil {
				err = locateErr
			} else if !found {
				err = gen.ErrProcessUnknown
			} else {
				res, err = w.callPID(pid, e.msg, e.timeout, e.important)
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
	pid, found, err := w.locate(process, 0)
	if err != nil {
		return err
	}
	if !found {
		return gen.ErrProcessUnknown
	}
	return w.sendToPID(pid, msg, important)
}

func (w *routeActor) sendToNode(to string, node gen.Atom, msg any, important bool) error {
	if err := w.requestContext().Err(); err != nil {
		return forwardContextError(err)
	}
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
	if err := w.requestContext().Err(); err != nil {
		return forwardContextError(err)
	}
	if important {
		return w.SendImportant(to, msg)
	}
	if to.Node == w.Node().Name() {
		return w.Send(to, msg)
	}
	return w.Send(to, msg)
}

func (w *routeActor) callPID(to gen.PID, msg any, timeout int, important bool) (any, error) {
	if err := w.requestContext().Err(); err != nil {
		return nil, forwardContextError(err)
	}
	timeout = w.remainingTimeout(timeout)
	if important {
		return w.callImportantWithTimeout(to, msg, timeout)
	}
	return w.CallPID(to, msg, timeout)
}

func (w *routeActor) locate(key gen.Atom, timeout int) (gen.PID, bool, error) {
	if w.locator == nil {
		return gen.PID{}, false, system.ErrActorRouterUnbound
	}
	ctx := w.requestContext()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	pid, found, err := w.locator.Locate(ctx, key)
	if err == nil {
		err = ctx.Err()
	}
	return pid, found, forwardContextError(err)
}

func (w *routeActor) callImportantWithTimeout(to any, msg any, timeout int) (any, error) {
	prev := w.ImportantDelivery()
	if err := w.SetImportantDelivery(true); err != nil {
		return nil, err
	}
	defer func() {
		_ = w.SetImportantDelivery(prev)
	}()
	if timeout > 0 {
		return w.CallWithTimeout(to, msg, timeout)
	}
	return w.Call(to, msg)
}

type nodeResult struct {
	response any
	err      error
}

func NewCaller(process gen.Process, locator actorLocator) *Caller {
	return &Caller{process: process, locator: locator}
}

type Caller struct {
	process gen.Process
	locator actorLocator
}

// Send resolves the global actor name and sends using the process delivery settings.
// Without important delivery, remote success does not confirm mailbox receipt.
func (caller *Caller) Send(to gen.Atom, msg any) error {
	pid, found, err := caller.locator.Locate(context.Background(), to)
	if err != nil {
		return err
	}
	if !found {
		return gen.ErrProcessUnknown
	}
	return caller.process.Send(pid, msg)
}

// SendImportant resolves the global actor name and confirms mailbox delivery,
// not completion of business processing.
func (caller *Caller) SendImportant(to gen.Atom, msg any) error {
	pid, found, err := caller.locator.Locate(context.Background(), to)
	if err != nil {
		return err
	}
	if !found {
		return gen.ErrProcessUnknown
	}
	return caller.process.SendImportant(pid, msg)
}

func (caller *Caller) Call(to gen.Atom, msg any) (any, error) {
	pid, found, err := caller.locator.Locate(context.Background(), to)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, gen.ErrProcessUnknown
	}
	return caller.process.Call(pid, msg)
}

func (w *routeActor) requestContext() context.Context {
	if w.request != nil {
		return w.request.ctx
	}
	return context.Background()
}
func (w *routeActor) remainingTimeout(fallback int) int {
	if deadline, ok := w.requestContext().Deadline(); ok {
		remaining := time.Until(deadline)
		seconds := int((remaining + time.Second - 1) / time.Second)
		if seconds < 1 {
			return 1
		}
		return seconds
	}
	return fallback
}
func (w *routeActor) Terminate(reason error) {
	if w.request != nil {
		select {
		case w.request.ch <- nodeResult{err: reason}:
		default:
		}
	}
	for _, ch := range w.monitorPID {
		select {
		case ch <- reason:
		default:
		}
	}
}
