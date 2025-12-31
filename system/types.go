package system

import (
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	start_init           struct{}
	inspect_process_list struct{}
	MessageLocate        struct {
		Name gen.Atom
	}
	ProcessVersion [2]int64
	ProcessInfo    struct {
		// Node is the node name hosting this process.
		Node gen.Atom
		// PID is the process identifier.
		PID gen.PID
		// Name is the registered name associated with this process.
		Name gen.Atom
		// BirthAt is the Unix timestamp (seconds) when the process was started.
		BirthAt int64
	}
	MessageProcessChanged struct {
		Node        gen.Atom
		UpProcess   []ProcessInfo
		DownProcess []ProcessInfo
		Version     ProcessVersion
	}
	MessageFetchProcessList struct {
		Version ProcessVersion
	}
	MessageProcesses struct {
		Node        gen.Atom
		ProcessList []ProcessInfo
		Version     ProcessVersion
	}
	ProcessInfoList    []ProcessInfo
	MessageProcessList struct {
		Node        gen.Atom
		ProcessList ProcessInfoList
		Version     ProcessVersion
	}
	MessageGetAddressBook  struct{}
	MessageAddressBook     struct{ Book IAddressBook }
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
