package system

import (
	"fmt"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	start_init           struct{}
	inspect_process_list struct{}
	schedule_cronjob     struct{}
	MessageLocate        struct {
		Name gen.Atom
	}
	MessageFlushProcess struct {
		PID gen.PID
	}
	MessageFetchProcessReply struct {
		Origin  gen.Atom
		FetchID uint64
		Kind    uint8
		Base    ProcessVersion
		Changed MessageProcessChanged
		Full    MessageProcesses
	}
	ProcessVersion     [2]int64
	NodeProcessVersion struct {
		Node    gen.Atom
		Version ProcessVersion
	}
	VersionSet  []NodeProcessVersion
	ProcessInfo struct {
		// Node is the node name hosting this process.
		Node gen.Atom
		// PID is the process identifier.
		PID gen.PID
		// Name is the registered name associated with this process.
		Name gen.Atom
		// BirthAt is the Unix timestamp (seconds) when the process was started.
		BirthAt int64
		// used in persist book
		Version int
	}
	MessageProcessChanged struct {
		Node        gen.Atom
		UpProcess   []ProcessInfo
		DownProcess []ProcessInfo
		Version     ProcessVersion
	}
	MessageFetchProcessList struct {
		Node       gen.Atom
		FetchID    uint64
		VersionSet VersionSet
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
	MessageGetAddressBook struct{}
	MessageAddressBook    struct {
		Owner gen.PID
		Book  IAddressBook
	}
	MessageLaunchAllDaemon struct{}
	MessageLaunchOneDaemon struct {
		Launcher gen.Atom
		Process  DaemonProcess
	}
)

func init() {
	types := []any{
		MessageFlushProcess{},
		ProcessVersion{},
		NodeProcessVersion{},
		VersionSet{},
		ProcessInfo{},
		ProcessInfoList{},
		MessageProcessChanged{},
		MessageProcesses{},
		MessageFetchProcessList{},
		MessageFetchProcessReply{},
		MessageProcessList{},
		MessageLocate{},
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

func (v ProcessVersion) GreaterThanOrEq(v2 ProcessVersion) bool {
	return v.GreaterThan(v2) || v.Equal(v2)
}

func (v ProcessVersion) GreaterThan(v2 ProcessVersion) bool {
	return v[0] > v2[0] || v[0] == v2[0] && v[1] > v2[1]
}

func (v ProcessVersion) Equal(v2 ProcessVersion) bool {
	return v[0] == v2[0] && v[1] == v2[1]
}

func (v ProcessVersion) Incr() ProcessVersion {
	return [2]int64{v[0], v[1] + 1}
}

func (v ProcessVersion) String() string {
	return fmt.Sprintf("%d.%d", v[0], v[1])
}

func NewVersion() ProcessVersion {
	return [2]int64{time.Now().UnixNano(), 0}
}
func NewEmptyVersion() ProcessVersion {
	return [2]int64{0, 0}
}

func (vs VersionSet) Size() int {
	return len(vs)
}

func (vs VersionSet) NextNode() (node gen.Atom) {
	return vs[len(vs)-1].Node
}

func (vs VersionSet) Next() NodeProcessVersion {
	return vs[len(vs)-1]
}

func (vs VersionSet) Drop() VersionSet {
	ret := vs[:len(vs)-1]
	return ret
}

func (vs VersionSet) MoveNodeToNext(node gen.Atom) VersionSet {
	for i, item := range vs {
		if item.Node == node && i < len(vs)-1 {
			set := make(VersionSet, 0, len(vs))
			for j := i + 1; j < len(vs); j++ {
				set = append(set, vs[j])
			}
			for j := 0; j <= i; j++ {
				set = append(set, vs[j])
			}
			return set
		}
	}
	return vs
}
