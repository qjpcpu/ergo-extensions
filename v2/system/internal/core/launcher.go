package core

import (
	"errors"
	"fmt"
	"sync"

	"ergo.services/ergo/gen"
)

var launchers sync.Map

// RegisterLauncher registers a launcher with the given name.
func RegisterLauncher(name gen.Atom, launcher Launcher) error {
	if launcher.Factory == nil {
		return fmt.Errorf("invalid launcher %s", name)
	}
	launcher.Name = name
	launchers.Store(name, launcher)
	return nil
}

// GetLauncher retrieves a launcher by its name.
func GetLauncher(name gen.Atom) (Launcher, bool) {
	if val, ok := launchers.Load(name); ok {
		return val.(Launcher), true
	}
	return Launcher{}, false
}

// UnregisterLauncher unregisters a launcher by its name.
func UnregisterLauncher(name gen.Atom) {
	launchers.Delete(name)
}

func RangeLaunchers(fn func(name gen.Atom, launcher Launcher) bool) {
	launchers.Range(func(key, value any) bool {
		return fn(key.(gen.Atom), value.(Launcher))
	})
}

type RouteDecorator func(key gen.Atom, factory gen.ProcessFactory) gen.ProcessFactory

func NewSpawner(parent gen.Process, decorate RouteDecorator, launcher gen.Atom) Spawner {
	return Spawner{parent: parent, decorate: decorate, launcher: launcher}
}

type Spawner struct {
	parent   gen.Process
	decorate RouteDecorator
	launcher gen.Atom
}

func (p Spawner) SpawnRegister(processName gen.Atom, args ...any) (pid gen.PID, err error) {
	launcher, ok := GetLauncher(p.launcher)
	if !ok {
		err = fmt.Errorf("no such launcher %s", p.launcher)
		return
	}
	if p.decorate == nil {
		return gen.PID{}, errors.New("actor router is required")
	}
	factory := WithDaemonRecovery(p.decorate(processName, launcher.Factory), p.launcher, DaemonProcess{ProcessName: processName, Args: args})
	return p.parent.SpawnRegister(processName, factory, launcher.Option, args...)
}

type DaemonProcess struct {
	// ProcessName is the name of the process.
	ProcessName gen.Atom
	// Args are the arguments to start the process.
	Args []any
}

type Launcher struct {
	// Factory is a function that creates a new process.
	Factory gen.ProcessFactory
	// Option provides options for configuring the process.
	Option gen.ProcessOptions

	// RecoveryScanner is an optional function that scans for daemons to recover.
	RecoveryScanner DaemonIteratorFactory // optional

	Name gen.Atom
}

type DaemonIteratorFactory func() DaemonIterator

type DaemonIterator func() ([]DaemonProcess, bool, error)

func SingletonDaemon(name gen.Atom, args []any) DaemonIteratorFactory {
	return func() DaemonIterator {
		return func() ([]DaemonProcess, bool, error) {
			return []DaemonProcess{{ProcessName: name, Args: args}}, false, nil
		}
	}
}

// WithDaemonRecovery schedules recovery after the routed behavior finishes cleanup.
func WithDaemonRecovery(factory gen.ProcessFactory, launcher gen.Atom, process DaemonProcess) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		behavior := factory()
		if behavior == nil {
			return nil
		}
		return &daemonRecoveryBehavior{ProcessBehavior: behavior, launcher: launcher, process: process}
	}
}

type daemonRecoveryBehavior struct {
	gen.ProcessBehavior
	launcher    gen.Atom
	process     DaemonProcess
	node        gen.Node
	pid         gen.PID
	initialized bool
}
type daemonRecoveryProcess struct {
	gen.Process
	behavior gen.ProcessBehavior
}

func (p daemonRecoveryProcess) Behavior() gen.ProcessBehavior { return p.behavior }
func (b *daemonRecoveryBehavior) ProcessInit(p gen.Process, args ...any) error {
	b.node = p.Node()
	b.pid = p.PID()
	err := b.ProcessBehavior.ProcessInit(daemonRecoveryProcess{p, b.ProcessBehavior}, args...)
	b.initialized = err == nil
	return err
}
func (b *daemonRecoveryBehavior) ProcessTerminate(reason error) {
	defer func() {
		if b.initialized && !errors.Is(reason, gen.TerminateReasonShutdown) {
			_ = b.node.Send(gen.Atom("extensions_daemon"), MessageDaemonExited{Ensure: MessageEnsureDaemon{Launcher: b.launcher, Process: b.process}, PID: b.pid})
		}
	}()
	b.ProcessBehavior.ProcessTerminate(reason)
}
