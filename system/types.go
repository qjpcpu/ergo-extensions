package system

import (
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	start_init           struct{}
	inspect_process_list struct{}
	rebroadcast          struct{ seq uint64 }
	MessageLocate        struct {
		Name gen.Atom
	}
	MessageProcessChanged struct {
		Node        gen.Atom
		UpProcess   []ProcessInfo
		DownProcess []ProcessInfo
		Version     ProcessVersion
	}
	ProcessVersion          [2]int64
	MessageFetchProcessList struct {
		Version ProcessVersion
	}
	MessageProcesses struct {
		Node        gen.Atom
		ProcessList []ProcessInfo
		Version     ProcessVersion
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
		Version     ProcessVersion
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
		ProcessVersion{},
		MessageFetchProcessList{},
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

func (v ProcessVersion) GreaterThan(v2 ProcessVersion) bool {
	return v[0] > v2[0] || v[0] == v2[0] && v[1] > v2[1]
}

func (v ProcessVersion) Incr() ProcessVersion {
	return [2]int64{v[0], v[1] + 1}
}

func NewVersion() ProcessVersion {
	return [2]int64{time.Now().UnixNano(), 0}
}
func NewEmptyVersion() ProcessVersion {
	return [2]int64{0, 0}
}
