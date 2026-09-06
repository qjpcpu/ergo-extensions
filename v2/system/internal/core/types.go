package core

import (
	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	MessageLaunchAllDaemon struct{}
	MessageTopologyUpdated struct{}
	MessageDaemonExited    struct {
		Ensure MessageEnsureDaemon
		PID    gen.PID
	}
	MessageEnsureDaemon struct {
		Launcher gen.Atom
		Process  DaemonProcess
		Attempt  int
	}
	MessageLaunchOneDaemon struct {
		Launcher gen.Atom
		Process  DaemonProcess
		Owner    gen.Atom
		Epoch    int64
	}
	MessageDaemonLaunchResult struct {
		Name  gen.Atom
		Node  gen.Atom
		Epoch int64
		State gen.Atom
		Err   string
	}
)

func init() {
	types := []any{
		MessageLaunchAllDaemon{},
		DaemonProcess{},
		MessageEnsureDaemon{},
		MessageLaunchOneDaemon{},
		MessageDaemonLaunchResult{},
	}
	for _, value := range types {
		err := edf.RegisterTypeOf(value)
		if err == nil || err == gen.ErrTaken {
			continue
		}
		panic(err)
	}
}
