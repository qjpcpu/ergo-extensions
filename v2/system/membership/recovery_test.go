package membership

import (
	"errors"
	"testing"

	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

type recoveryNetwork struct {
	gen.Network
	registrar gen.Registrar
	err       error
}

func (n *recoveryNetwork) Registrar() (gen.Registrar, error) { return n.registrar, n.err }

type recoveryNode struct {
	gen.Node
	network gen.Network
}

func (n recoveryNode) Network() gen.Network { return n.network }

type recoveryRegistrar struct {
	failingRegistrar
	event    gen.Event
	eventErr error
}

func (r *recoveryRegistrar) Event() (gen.Event, error) { return r.event, r.eventErr }

func TestMembershipReconnectsAfterSetupFailure(t *testing.T) {
	for _, stage := range []string{"registrar", "event", "monitor"} {
		t.Run(stage, func(t *testing.T) {
			book := core.NewAddressBook()
			actor := spawnMembership(t, book, Options{})
			m := actor.Behavior().(*membership)
			m.registrar = nil
			failure := errors.New("discovery unavailable")
			reg := &recoveryRegistrar{event: gen.Event{Name: "membership", Node: actor.Node().Name()}}
			reg.nodes = []gen.Atom{"peer@localhost"}
			network := &recoveryNetwork{registrar: reg}
			node := recoveryNode{Node: m.Node(), network: network}
			actor.OnNode(func() gen.Node { return node })
			monitor := actor.OnMonitorEvent(reg.event)
			switch stage {
			case "registrar":
				network.err = failure
			case "event":
				reg.eventErr = failure
			case "monitor":
				monitor.Fail(failure)
			}
			actor.SendMessage(gen.PID{}, messageInit{})
			if !errors.Is(m.lastError, failure) || m.retry != 1 || m.registrar != nil || book.GetAvailableNodes().Len() != 0 {
				t.Fatalf("partial setup published state: %+v", m)
			}
			actor.ShouldSendAfter().Message(messageRefresh{ID: m.refreshID}).Once().Assert()
			network.err = nil
			reg.eventErr = nil
			monitor.Fail(nil)
			actor.SendMessage(gen.PID{}, messageRefresh{ID: m.refreshID})
			if m.lastError != nil || m.retry != 0 || m.event != reg.event || !book.GetAvailableNodes().Exist("peer@localhost") {
				t.Fatalf("refresh did not recover: %+v", m)
			}
			m.Terminate(gen.TerminateReasonShutdown)
			if m.cancelRefresh != nil {
				t.Fatal("refresh timer survived termination")
			}
		})
	}
}

func TestMembershipRetriesFailedTopologyNotification(t *testing.T) {
	book := core.NewAddressBook()
	actor := spawnMembership(t, book, Options{})
	m := actor.Behavior().(*membership)
	notification := actor.OnSend(gen.Atom("extensions_daemon"))
	failure := errors.New("daemon mailbox full")
	notification.Fail(failure)
	m.topologyDirty = true
	actor.SendMessage(gen.PID{}, messageInit{})
	if !m.topologyDirty || !errors.Is(m.lastError, failure) || m.notifiedVersion == book.NodesVersion() {
		t.Fatal("failed notification was acknowledged")
	}
	notification.Fail(nil)
	actor.SendMessage(gen.PID{}, messageTopologyChanged{ID: m.topologyID})
	if m.topologyDirty || m.lastError != nil || m.notifiedVersion != book.NodesVersion() {
		t.Fatal("topology was not delivered after recovery")
	}
}
