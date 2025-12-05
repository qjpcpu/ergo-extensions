package system

import (
	"fmt"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

const DaemonMonitorProcess = gen.Atom("daemon_monitor")

type DaemonProcess struct {
	ProcessName  gen.Atom
	LauncherName gen.Atom
	Args         []any
}

type Launcher struct {
	Factory gen.ProcessFactory
	Option  gen.ProcessOptions
}

type DaemonIteratorFactory func() DaemonIterator

type DaemonIterator func() ([]DaemonProcess, bool, error)

type daemon struct {
	act.Actor
	book            *AddressBook
	iteratorFac     DaemonIteratorFactory
	launchers       map[gen.Atom]Launcher
	registrar       gen.Registrar
	isLeader        bool
	planLaunchAllAt int64
}

func factory_daemon(fac DaemonIteratorFactory, launchers map[gen.Atom]Launcher, book *AddressBook) gen.ProcessFactory {
	l := make(map[gen.Atom]Launcher)
	for k, v := range launchers {
		l[k] = v
	}
	return func() gen.ProcessBehavior { return &daemon{iteratorFac: fac, book: book, launchers: l} }
}

func (w *daemon) Init(args ...any) error {
	w.SendAfter(w.PID(), start_init{}, time.Second*1)
	return nil
}

func (w *daemon) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case start_init:
		if err := w.setupRegistrarMonitoring(); err != nil {
			w.SendAfter(w.PID(), start_init{}, time.Second*1)
		} else {
			w.launchAllAfter(time.Second * 10)
		}
	case MessageLaunchAllDaemon:
		if now := time.Now().Unix(); w.planLaunchAllAt > now {
			return nil
		}
		if err := w.leaderShouldLaunchDaemon(); err != nil {
			w.launchAllAfter(time.Second * 10)
		}
	case MessageLaunchOneDaemon:
		w.launchDaemonOnNode(w.Node().Name(), e.Process)
	}
	return nil
}

func (w *daemon) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
	case zk.EventNodeSwitchedToLeader:
		if e.Name == w.Node().Name() {
			w.isLeader = true
			w.launchAllAfter(time.Second * 1)
			return nil
		}
	case zk.EventNodeSwitchedToFollower:
		if e.Name == w.Node().Name() {
			w.isLeader = false
			return nil
		}
	case zk.EventNodeLeft:
		w.launchAllAfter(time.Second * 5)
	}
	return nil
}

func (w *daemon) launchAllAfter(duration time.Duration) {
	if duration <= 0 {
		w.Send(w.PID(), MessageLaunchAllDaemon{})
	} else {
		w.planLaunchAllAt = max(w.planLaunchAllAt, time.Now().Add(duration).Unix())
		w.SendAfter(w.PID(), MessageLaunchAllDaemon{}, duration)
	}
}

func (w *daemon) setupRegistrarMonitoring() error {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return err
		} else {
			w.registrar = registrar
		}
		event, err := registrar.Event()
		if err != nil {
			return err
		}
		if events, err := w.MonitorEvent(event); err != nil {
			return err
		} else {
			for _, evt := range events {
				w.HandleEvent(evt)
			}
		}
	}
	return nil
}

func (w *daemon) leaderShouldLaunchDaemon() error {
	if !w.isLeader {
		return nil
	}
	return w.launchDaemon()
}

func (w *daemon) launchDaemon() error {
	if w.iteratorFac == nil {
		return nil
	}
	w.Log().Info("start check daemon process existence")
	next := w.iteratorFac()
	var retErr error
	for {
		processList, hasMore, err := next()
		if err != nil {
			return err
		}
		for _, proc := range processList {
			node := w.book.PickNode(proc.ProcessName)
			if err = w.launchDaemonOnNode(node, proc); err != nil {
				retErr = err
			}
		}
		if !hasMore {
			break
		}
	}
	return retErr
}

func (w *daemon) launchDaemonOnNode(node gen.Atom, proc DaemonProcess) error {
	launcher, ok := w.launchers[proc.LauncherName]
	if !ok {
		return fmt.Errorf("can't found launcher by name %s", proc.LauncherName)
	}
	if _, ok := w.book.Locate(proc.ProcessName); ok {
		return nil
	}
	if node == w.Node().Name() {
		_, err := w.SpawnRegister(proc.ProcessName, launcher.Factory, launcher.Option, proc.Args...)
		if err != nil {
			if err == gen.ErrTaken {
				w.Log().Info("launch daemon process %s on %s OK", proc.ProcessName, node)
				return nil
			}
			w.Log().Error("launch daemon process %s on %s fail %v", proc.ProcessName, node, err)
			return err
		} else {
			w.Log().Info("launch daemon process %s on %s OK", proc.ProcessName, node)
		}
	} else {
		if err := w.SendImportant(gen.ProcessID{Name: DaemonMonitorProcess, Node: node}, MessageLaunchOneDaemon{Process: proc}); err != nil {
			w.Log().Error("spawn remote daemon process %s on %s fail %v", proc.ProcessName, node, err)
			return err
		} else {
			w.Log().Info("spawn remote daemon process %s on %s OK", proc.ProcessName, node)
		}
	}
	return nil
}
