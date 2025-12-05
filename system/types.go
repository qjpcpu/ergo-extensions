package system

import (
	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	start_init           struct{}
	inspect_process_list struct{}
	rebroadcast          struct{}
	MessageLocate        struct {
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
		Launcher gen.Atom
		Process  DaemonProcess
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
