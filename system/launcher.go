package system

import (
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
	launcher.name = name
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

func NewSpawner(parent gen.Process, launcher gen.Atom) Spawner {
	return Spawner{parent: parent, launcher: launcher}
}

type Spawner struct {
	parent   gen.Process
	launcher gen.Atom
}

func (p Spawner) SpawnRegister(processName gen.Atom, args ...any) (pid gen.PID, err error) {
	launcher, ok := GetLauncher(p.launcher)
	if !ok {
		err = fmt.Errorf("no such launcher %s", p.launcher)
		return
	}
	return p.parent.SpawnRegister(processName, launcher.Factory, launcher.Option, args...)
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

	name gen.Atom
}

type DaemonIteratorFactory func() DaemonIterator

type DaemonIterator func() ([]DaemonProcess, bool, error)
