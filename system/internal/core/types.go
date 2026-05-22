package core

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
	messageDaemonLaunchTimeout struct {
		Name  gen.Atom
		Epoch int64
	}
	messageScheduleCron struct{}
	MessageLocate       struct {
		Name gen.Atom
	}
	MessageLocateResult struct {
		Name gen.Atom
		Node gen.Atom
	}
	MonitorPlacement struct {
		Name gen.Atom
	}
	DuplicatePlacement struct {
		Name gen.Atom
		Node gen.Atom
	}
	MessageForwardLocate struct {
		Name gen.Atom
		From gen.PID
		Ref  gen.Ref
		Hops uint8
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
		FullSync    bool
	}
	MessageRegisterLocalProcess struct {
		Name    gen.Atom
		PID     gen.PID
		BirthAt int64
	}
	ProcessInfoList       []ProcessInfo
	MessageGetAddressBook struct{}
	MessageAddressBook    struct {
		Owner gen.PID
		Book  IAddressBook
	}
	MessageLaunchAllDaemon struct{}
	MessageEnsureDaemon    struct {
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
		ProcessVersion{},
		ProcessInfo{},
		ProcessInfoList{},
		MessageProcessChanged{},
		MessageRegisterLocalProcess{},
		MessageLocate{},
		MessageLocateResult{},
		MessageForwardLocate{},
		MessageLaunchAllDaemon{},
		DaemonProcess{},
		MessageEnsureDaemon{},
		MessageLaunchOneDaemon{},
		MessageDaemonLaunchResult{},
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
