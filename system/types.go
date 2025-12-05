package system

import (
	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	start_init    struct{}
	rebroadcast   struct{}
	MessageLocate struct {
		Name gen.Atom
	}
	MessageProcessChanged struct {
		Node        gen.Atom
		UpProcess   []ProcessInfo
		DownProcess []ProcessInfo
	}
	MessageProcesses struct {
		Node        gen.Atom
		ProcessList []ProcessInfo
	}
	ProcessInfo struct {
		Node gen.Atom
		// PID process ID
		PID gen.PID
		// Name registered associated name with this process
		Name gen.Atom
		// Application application name if this process started under application umbrella
		Application gen.Atom
		// State shows current state of the process
		State gen.ProcessState
		// Uptime of the process in seconds
		Uptime int64
	}
	ProcessInfoList    []ProcessInfo
	MessageProcessList struct {
		Node        gen.Atom
		ProcessList ProcessInfoList
	}
	MessageGetAddressBook struct{}
	MessageAddressBook    struct{ Book IAddressBook }

	MessageLaunchAllDaemon struct{}
	MessageLaunchOneDaemon struct {
		Process DaemonProcess
	}
)

func init() {
	types := []any{
		ProcessInfo{},
		ProcessInfoList{},
		MessageProcessList{},
		MessageLocate{},
		MessageProcessChanged{},
		MessageProcesses{},
		MessageLaunchAllDaemon{},
		DaemonProcess{},
		MessageLaunchOneDaemon{},
	}

	for _, t := range types {
		err := edf.RegisterTypeOf(t)
		if err == nil || err == gen.ErrTaken {
			continue
		}
		panic(err)
	}
}
