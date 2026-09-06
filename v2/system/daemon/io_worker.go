package daemon

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"errors"
	"fmt"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

const daemonIOWorkers = 8

type messageRetry struct {
	Name  gen.Atom
	Epoch int64
}
type messageIO struct {
	key        gen.Atom
	state      daemonLaunchState
	owner      gen.Atom
	reply      *core.MessageDaemonLaunchResult
	recoverAll bool
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
	book    core.IAddressBook
	release func(context.Context, gen.Atom, gen.PID) error
	parent  gen.PID
}

func (w *daemonIOWorker) Init(...any) error { return nil }
func (w *daemonIOWorker) HandleMessage(_ gen.PID, message any) error {
	job, ok := message.(messageIO)
	if !ok {
		return nil
	}
	result := messageIOResult{key: job.key, epoch: job.state.Epoch, exited: job.state.Exited}
	defer func() {
		if v := recover(); v != nil {
			result.err = fmt.Errorf("daemon I/O panic: %v", v)
		}
		if job.reply != nil || job.recoverAll {
			w.Send(w.parent, messageReplyFinished{})
			return
		}
		w.Send(w.parent, result)
	}()
	if job.recoverAll {
		w.Send(gen.ProcessID{Name: ProcessName, Node: job.owner}, core.MessageLaunchAllDaemon{})
		return nil
	}
	if job.reply != nil {
		if err := w.Send(gen.ProcessID{Name: ProcessName, Node: job.owner}, *job.reply); err != nil {
			w.Log().Warning("daemon launch result delivery failed: %v", err)
		}
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
	result.err = w.Send(gen.ProcessID{Name: ProcessName, Node: job.state.TargetNode}, core.MessageLaunchOneDaemon{
		Launcher: job.state.Launcher, Process: job.state.Process, Owner: w.Node().Name(), Epoch: job.state.Epoch,
	})
	return nil
}
