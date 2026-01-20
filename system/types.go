package system

import (
	"fmt"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

type (
	messageInit           struct{}
	messageInspectProcess struct{}
	messageTopologyChange struct {
		ID int64
	}
	messageScheduleCron struct{}
	MessageLocate       struct {
		Name gen.Atom
	}
	MessageForwardLocate struct {
		Name gen.Atom
		From gen.PID
		Ref  gen.Ref
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
		// used in persist book
		Version int
	}
	MessageProcessChanged struct {
		Node        gen.Atom
		UpProcess   []ProcessInfo
		DownProcess []ProcessInfo
		Version     ProcessVersion
		FullSync    bool
	}
	ProcessInfoList       []ProcessInfo
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
		ProcessVersion{},
		ProcessInfo{},
		ProcessInfoList{},
		MessageProcessChanged{},
		MessageLocate{},
		MessageForwardLocate{},
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
