package daemon

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"errors"
	"fmt"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

const daemonIOWorkers = 16

type messageRetry struct {
	Name  gen.Atom
	Epoch int64
}
type messageIO struct {
	key        gen.Atom
	state      daemonLaunchState
	owner      gen.Atom
	message    any
	recoverAll bool
	scanPage   *scanPageRequest
	watch      *peerWatch
}
type scanPageRequest struct {
	scan     *recoveryScan
	iterator core.DaemonIterator
	factory  core.DaemonIteratorFactory
}

type messageIOResult struct {
	key     gen.Atom
	epoch   int64
	exited  gen.PID
	running bool
	err     error
}
type messageReplyFinished struct{}

func (w *daemon) dispatchIO(job messageIO) error {
	if w.ioPool == (gen.PID{}) {
		book, release, parent := w.book, w.release, w.PID()
		pid, err := w.Spawn(func() gen.ProcessBehavior { return &daemonIOPool{book: book, release: release, parent: parent} }, gen.ProcessOptions{LinkParent: true})
		if err != nil {
			return err
		}
		if err := w.MonitorPID(pid); err != nil {
			w.Node().Kill(pid)
			return err
		}
		w.ioPool = pid
	}
	return w.Send(w.ioPool, job)
}

type daemonIOPool struct {
	act.Pool
	book    core.IAddressBook
	release func(context.Context, gen.Atom, gen.PID) error
	parent  gen.PID
}

func (p *daemonIOPool) Init(...any) (act.PoolOptions, error) {
	return act.PoolOptions{PoolSize: daemonIOWorkers, WorkerFactory: func() gen.ProcessBehavior {
		return &daemonIOWorker{book: p.book, release: p.release, parent: p.parent}
	}}, nil
}

type daemonIOWorker struct {
	act.Actor
	watches map[gen.ProcessID]*peerWatch
	book    core.IAddressBook
	release func(context.Context, gen.Atom, gen.PID) error
	parent  gen.PID
}

func (w *daemonIOWorker) Init(...any) error { return nil }
func (w *daemonIOWorker) HandleMessage(_ gen.PID, message any) error {
	if down, ok := message.(gen.MessageDownProcessID); ok {
		if watch := w.watches[down.ProcessID]; watch != nil {
			delete(w.watches, down.ProcessID)
			w.Send(w.parent, messagePeerDown{watch: watch})
		}
		return nil
	}
	job, ok := message.(messageIO)
	if !ok {
		return nil
	}
	if job.scanPage != nil {
		w.fetchScanPage(job.scanPage)
		return nil
	}
	result := messageIOResult{key: job.key, epoch: job.state.Epoch, exited: job.state.Exited}
	defer func() {
		if v := recover(); v != nil {
			result.err = fmt.Errorf("daemon I/O panic: %v", v)
		}
		if job.watch != nil {
			w.Send(w.parent, messagePeerMonitor{watch: job.watch, worker: w.PID(), ready: true, err: result.err})
			return
		}
		if job.message != nil {
			w.Send(w.parent, messageDeliveryFinished{node: job.owner, message: job.message, err: result.err})
			return
		}
		if job.recoverAll {
			w.Send(w.parent, messageReplyFinished{})
			return
		}
		w.Send(w.parent, result)
	}()
	if job.watch != nil {
		w.Send(w.parent, messagePeerMonitor{watch: job.watch, worker: w.PID()})
		target := gen.ProcessID{Name: ProcessName, Node: job.watch.node}
		result.err = w.MonitorProcessID(target)
		if result.err == nil {
			if w.watches == nil {
				w.watches = make(map[gen.ProcessID]*peerWatch)
			}
			w.watches[target] = job.watch
		}
		return nil
	}
	if job.recoverAll {
		w.Send(gen.ProcessID{Name: ProcessName, Node: job.owner}, core.MessageLaunchAllDaemon{})
		return nil
	}
	if job.message != nil {
		result.err = w.Send(gen.ProcessID{Name: ProcessName, Node: job.owner}, job.message)
		return nil
	}
	if job.state.Exited != (gen.PID{}) && w.release != nil {
		if result.err = w.release(context.Background(), job.key, job.state.Exited); result.err != nil {
			return nil
		}
	}
	pid, found, err := w.book.Locate(context.Background(), job.key)
	if err != nil {
		result.err = err
		return nil
	}
	if found && pid.Node == w.Node().Name() {
		_, err = w.Node().ProcessState(pid)
		if errors.Is(err, gen.ErrProcessUnknown) {
			found = false
		} else if err != nil {
			result.err = err
			return nil
		}
	}
	if found {
		result.running = true
		return nil
	}
	return nil
}

func (w *daemonIOWorker) fetchScanPage(request *scanPageRequest) {
	result := messageScanPage{scan: request.scan, request: request, iterator: request.iterator}
	defer func() {
		if v := recover(); v != nil {
			result.err = fmt.Errorf("scanner panic: %v", v)
		}
		w.Send(w.parent, result)
	}()
	if result.iterator == nil {
		result.iterator = request.factory()
	}
	result.page, result.more, result.err = result.iterator()
}
