package system

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system/daemon"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"github.com/qjpcpu/ergo-extensions/v2/system/membership"
)

const DaemonMonitorProcess = daemon.ProcessName

var ErrNoAvailableNodes = core.ErrNoAvailableNodes

type (
	IAddressBook                = core.IAddressBook
	AddressBook                 = core.AddressBook
	NodeList                    = core.NodeList
	ImmutableList[T comparable] = core.ImmutableList[T]
	AtomicValue[T any]          = core.AtomicValue[T]
	Spawner                     = core.Spawner
	DaemonProcess               = core.DaemonProcess
	Launcher                    = core.Launcher
	MembershipOptions           = membership.Options
	DaemonOptions               = daemon.Options
	DaemonIteratorFactory       = core.DaemonIteratorFactory
	DaemonIterator              = core.DaemonIterator
)

func NewAtomicValue[T any]() *AtomicValue[T] {
	return core.NewAtomicValue[T]()
}

func NewImmutableList[T comparable](list []T) *ImmutableList[T] {
	return core.NewImmutableList(list)
}

func NewNodeList(list ...gen.Atom) *NodeList {
	return core.NewNodeList(list...)
}

func NewAddressBook() *AddressBook {
	return core.NewAddressBook()
}

func DefaultMembershipOptions() MembershipOptions {
	return membership.DefaultOptions()
}

func DefaultDaemonOptions() DaemonOptions {
	return daemon.DefaultOptions()
}

func RegisterLauncher(name gen.Atom, launcher Launcher) error {
	return core.RegisterLauncher(name, launcher)
}

func GetLauncher(name gen.Atom) (Launcher, bool) {
	return core.GetLauncher(name)
}

func UnregisterLauncher(name gen.Atom) {
	core.UnregisterLauncher(name)
}

func NewSpawner(parent gen.Process, router *ActorRouter, launcher gen.Atom) Spawner {
	if router == nil {
		return core.NewSpawner(parent, nil, launcher)
	}
	return core.NewSpawner(parent, router.routeFactory, launcher)
}

func SingletonDaemon(name gen.Atom, args []any) DaemonIteratorFactory {
	return core.SingletonDaemon(name, args)
}
