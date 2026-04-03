package system

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system/daemon"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
	"github.com/qjpcpu/ergo-extensions/system/whereis"
)

const (
	WhereIsProcess       = whereis.ProcessName
	DaemonMonitorProcess = daemon.ProcessName
)

var ErrNoAvailableNodes = daemon.ErrNoAvailableNodes

type (
	QueryOption                 = core.QueryOption
	IAddressBookQuery           = core.IAddressBookQuery
	IAddressBook                = core.IAddressBook
	AddressBook                 = core.AddressBook
	NodeList                    = core.NodeList
	ImmutableList[T comparable] = core.ImmutableList[T]
	AtomicValue[T any]          = core.AtomicValue[T]
	Spawner                     = core.Spawner
	DaemonProcess               = core.DaemonProcess
	Launcher                    = core.Launcher
	DaemonIteratorFactory       = core.DaemonIteratorFactory
	DaemonIterator              = core.DaemonIterator
	ProcessVersion              = core.ProcessVersion
	ProcessInfo                 = core.ProcessInfo
	ProcessInfoList             = core.ProcessInfoList
	MessageLocate               = core.MessageLocate
	MessageLocateResult         = core.MessageLocateResult
	MessageForwardLocate        = core.MessageForwardLocate
	MessageProcessChanged       = core.MessageProcessChanged
	MessageGetAddressBook       = core.MessageGetAddressBook
	MessageAddressBook          = core.MessageAddressBook
	MessageLaunchAllDaemon      = core.MessageLaunchAllDaemon
	MessageEnsureDaemon         = core.MessageEnsureDaemon
	MessageLaunchOneDaemon      = core.MessageLaunchOneDaemon
	MessageDaemonLaunchResult   = core.MessageDaemonLaunchResult
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

func RegisterLauncher(name gen.Atom, launcher Launcher) error {
	return core.RegisterLauncher(name, launcher)
}

func GetLauncher(name gen.Atom) (Launcher, bool) {
	return core.GetLauncher(name)
}

func UnregisterLauncher(name gen.Atom) {
	core.UnregisterLauncher(name)
}

func NewSpawner(parent gen.Process, launcher gen.Atom) Spawner {
	return core.NewSpawner(parent, launcher)
}

func SingletonDaemon(name gen.Atom, args []any) DaemonIteratorFactory {
	return core.SingletonDaemon(name, args)
}

func NewVersion() ProcessVersion {
	return core.NewVersion()
}
