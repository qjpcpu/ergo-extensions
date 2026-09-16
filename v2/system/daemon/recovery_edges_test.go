package daemon

import (
	"errors"
	"testing"

	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

type reconnectRegistrar struct {
	daemonTestRegistrar
	event               gen.Event
	eventErr, leaderErr error
	leader              any
}

func (r *reconnectRegistrar) Event() (gen.Event, error)      { return r.event, r.eventErr }
func (r *reconnectRegistrar) ConfigItem(string) (any, error) { return r.leader, r.leaderErr }

type reconnectNetwork struct {
	gen.Network
	reg gen.Registrar
	err error
}

func (n *reconnectNetwork) Registrar() (gen.Registrar, error) { return n.reg, n.err }

type reconnectNode struct {
	gen.Node
	network gen.Network
}

func (n reconnectNode) Network() gen.Network { return n.network }

func TestDaemonSetupRecoversFromDiscoveryFailure(t *testing.T) {
	for _, stage := range []string{"registrar", "event", "monitor", "leader"} {
		t.Run(stage, func(t *testing.T) {
			actor := spawnDaemonUnit(t, core.NewAddressBook(), "source@localhost")
			w := actor.Behavior().(*daemon)
			failure := errors.New("discovery unavailable")
			reg := &reconnectRegistrar{event: gen.Event{Name: "membership", Node: w.Node().Name()}, leader: w.Node().Name()}
			network := &reconnectNetwork{reg: reg}
			node := reconnectNode{Node: w.Node(), network: network}
			actor.OnNode(func() gen.Node { return node })
			monitor := actor.OnMonitorEvent(reg.event)
			switch stage {
			case "registrar":
				network.err = failure
			case "event":
				reg.eventErr = failure
			case "monitor":
				monitor.Fail(failure)
			case "leader":
				reg.leaderErr = failure
			}
			mark := actor.Mark()
			actor.SendMessage(gen.PID{}, messageInit{})
			if w.registrar != nil || w.isLeader {
				t.Fatal("partial registration enabled recovery")
			}
			actor.ShouldSendAfter().Message(messageInit{}).Since(mark).Once().Assert()
			network.err = nil
			reg.eventErr = nil
			reg.leaderErr = nil
			monitor.Fail(nil)
			actor.SendMessage(gen.PID{}, messageInit{})
			if w.registrar != reg || !w.isLeader || w.cancelLaunchAll == nil {
				t.Fatal("leader recovery did not resume")
			}
		})
	}
}

func TestDaemonDeliveryFailureRetriesCurrentAttempt(t *testing.T) {
	for _, phase := range []daemonLaunchPhase{daemonLaunchPhaseOffering, daemonLaunchPhaseLaunching} {
		name := "offer"
		if phase == daemonLaunchPhaseLaunching {
			name = "launch"
		}
		t.Run(name, func(t *testing.T) {
			book := core.NewAddressBook()
			book.SetAvailableNodes(core.NewNodeList("target@localhost"))
			actor := spawnDaemonUnit(t, book, "source@localhost")
			w := actor.Behavior().(*daemon)
			actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
			state := w.launching["task"]
			state.Phase = phase
			w.launching["task"] = state
			var msg any = core.MessageDaemonLaunchOffer{Name: "task", Owner: w.Node().Name(), Epoch: state.Epoch}
			if phase == daemonLaunchPhaseLaunching {
				msg = core.MessageLaunchOneDaemon{Process: state.Process, Epoch: state.Epoch}
			}
			w.pendingReplies = 1
			actor.SendMessage(gen.PID{}, messageDeliveryFinished{node: state.TargetNode, message: msg, err: gen.ErrNoConnection})
			next := w.launching["task"]
			if next.Phase != daemonLaunchPhaseRetrying || next.Attempt != state.Attempt+1 || len(w.retries) != 1 || w.pendingReplies != 1 {
				t.Fatal("failed delivery lost its retry", next)
			}
			actor.SendMessage(gen.PID{}, messageRetry{Name: "task", Epoch: next.Epoch})
			current := w.launching["task"]
			if current.Phase != daemonLaunchPhaseChecking || current.Epoch == state.Epoch {
				t.Fatal("retry did not start a fresh check")
			}
			actor.SendMessage(gen.PID{}, messageDeliveryFinished{node: state.TargetNode, message: msg, err: gen.ErrNoConnection})
			if w.launching["task"].Epoch != current.Epoch || len(w.retries) != 0 {
				t.Fatal("late failure affected the new attempt")
			}
		})
	}
}

func TestDaemonPullFailureReturnsReservedCapacity(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	offer := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source@localhost", Epoch: 3}
	acceptTestLaunchOffer(w, offer)
	entry := w.pendingLaunch[offer.Name]
	if entry == nil || entry.worker == (gen.PID{}) {
		t.Fatal("task was not reserved")
	}
	actor.SendMessage(gen.PID{}, messageDeliveryFinished{node: offer.Owner, message: core.MessageDaemonLaunchPull{Name: offer.Name, Epoch: offer.Epoch}, err: gen.ErrNoConnection})
	if len(w.pendingLaunch) != 0 || len(w.idleWorkers) != daemonLaunchWorkers {
		t.Fatal("failed pull retained worker capacity")
	}
	replies := protocolMessages[core.MessageDaemonLaunchResult](actor, 0)
	if len(replies) == 0 || replies[len(replies)-1].State != daemonLaunchFailed {
		t.Fatal("pull failure was not reported")
	}
}

func TestDaemonFollowerRecoveryWaitsForLeaderAndCapacity(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "follower@localhost")
	w := actor.Behavior().(*daemon)
	reg := &reconnectRegistrar{leaderErr: gen.ErrNoConnection}
	w.registrar = reg
	w.requestRecovery()
	if !w.wantRecovery || w.pendingReplies != 0 {
		t.Fatal("failed leader lookup discarded recovery")
	}
	reg.leaderErr = nil
	reg.leader = "not an atom"
	w.requestPendingRecovery()
	if !w.wantRecovery {
		t.Fatal("invalid leader discarded recovery")
	}
	reg.leader = gen.Atom("")
	w.requestPendingRecovery()
	if !w.wantRecovery {
		t.Fatal("empty leader discarded recovery")
	}
	reg.leader = gen.Atom("leader@localhost")
	w.options.MaxInFlight = 1
	w.launching["busy"] = daemonLaunchState{}
	w.requestPendingRecovery()
	if !w.wantRecovery {
		t.Fatal("full admission discarded recovery")
	}
	delete(w.launching, "busy")
	actor.SendMessage(gen.PID{}, messageReplyFinished{})
	if w.wantRecovery || w.pendingReplies != 1 {
		t.Fatal("free capacity did not forward recovery")
	}
	actor.SendMessage(gen.PID{}, messageReplyFinished{})
	if w.pendingReplies != 0 {
		t.Fatal("recovery reply accounting leaked")
	}
}
