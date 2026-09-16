package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

type unavailableWorkerProcess struct {
	gen.Process
	pid                  gen.PID
	spawnErr, monitorErr error
}

func (p unavailableWorkerProcess) Spawn(gen.ProcessFactory, gen.ProcessOptions, ...any) (gen.PID, error) {
	return p.pid, p.spawnErr
}
func (p unavailableWorkerProcess) MonitorPID(gen.PID) error { return p.monitorErr }

func TestDaemonWorkerCreationFailureCanRecover(t *testing.T) {
	for _, kind := range []string{"io", "launch"} {
		for _, stage := range []string{"spawn", "monitor"} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				book := core.NewAddressBook()
				book.SetAvailableNodes(core.NewNodeList("source@localhost"))
				actor := spawnDaemonUnit(t, book, "source@localhost")
				w := actor.Behavior().(*daemon)
				base := w.Process
				child := gen.PID{Node: w.Node().Name(), ID: 900}
				failure := errors.New("worker unavailable")
				faulty := unavailableWorkerProcess{Process: base, pid: child}
				if stage == "spawn" {
					faulty.spawnErr = failure
				} else {
					faulty.monitorErr = failure
				}
				var killed gen.PID
				actor.Node().OnKill(func(pid gen.PID) error { killed = pid; return nil })
				w.Process = faulty
				if kind == "io" {
					actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
					state := w.launching["task"]
					if state.Phase != daemonLaunchPhaseRetrying || w.ioPool != (gen.PID{}) {
						t.Fatal("failed pool left a stuck check")
					}
					w.Process = base
					actor.SendMessage(gen.PID{}, messageRetry{Name: "task", Epoch: state.Epoch})
					if w.ioPool == (gen.PID{}) || w.launching["task"].Phase != daemonLaunchPhaseChecking {
						t.Fatal("retry did not create a working pool")
					}
				} else {
					actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchOffer{Name: "task", Owner: w.Node().Name(), Epoch: 1})
					results := protocolMessages[core.MessageDaemonLaunchResult](actor, 0)
					if len(results) != 1 || results[0].State != daemonLaunchFailed || len(w.launchWorkers) != 0 {
						t.Fatal("failed launch worker did not fail offer")
					}
					w.Process = base
					actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchOffer{Name: "next", Owner: w.Node().Name(), Epoch: 2})
					if len(w.launchWorkers) != daemonLaunchWorkers || w.pendingLaunch["next"] == nil {
						t.Fatal("subsequent offer did not recover capacity")
					}
				}
				if stage == "monitor" && killed != child {
					t.Fatal("unmonitored worker leaked")
				}
			})
		}
	}
}

func TestDaemonIOWorkerHandlesStaleLocalRoutesAndCleanupPanic(t *testing.T) {
	for _, stage := range []string{"alive", "gone", "state error", "cleanup panic"} {
		t.Run(stage, func(t *testing.T) {
			book := core.NewAddressBook()
			node := unit.StartNode(t, "local@localhost", gen.NodeOptions{})
			pid := gen.PID{Node: "local@localhost", ID: 900}
			lookups, cleanups := 0, 0
			failure := errors.New("state unavailable")
			panicCleanup := stage == "cleanup panic"
			if err := book.BindLocator("local@localhost", func(context.Context, gen.Atom) (gen.PID, bool, error) { lookups++; return pid, true, nil }); err != nil {
				t.Fatal(err)
			}
			release := func(context.Context, gen.Atom, gen.PID) error {
				cleanups++
				if panicCleanup {
					panic("cleanup unavailable")
				}
				return nil
			}
			pool := &daemonIOPool{book: book, release: release, parent: gen.PID{Node: "local@localhost", ID: 1}}
			options, err := pool.Init()
			if err != nil || options.PoolSize != daemonIOWorkers {
				t.Fatal("invalid I/O pool", options, err)
			}
			actor, err := node.Spawn(options.WorkerFactory, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			node.OnProcessState(func(gen.PID) (gen.ProcessState, error) {
				if stage == "gone" {
					return gen.ProcessStateTerminated, gen.ErrProcessUnknown
				}
				if stage == "state error" {
					return 0, failure
				}
				return gen.ProcessStateRunning, nil
			})
			job := messageIO{key: "task", state: daemonLaunchState{Epoch: 4, Exited: pid}}
			actor.SendMessage(gen.PID{}, job)
			results := protocolMessages[messageIOResult](actor, 0)
			if len(results) != 1 || results[0].epoch != 4 || results[0].exited != pid || cleanups != 1 {
				t.Fatal("lost cleanup result", results)
			}
			result := results[0]
			switch stage {
			case "alive":
				if !result.running || result.err != nil {
					t.Fatal("live actor was not recognized", result)
				}
			case "gone":
				if result.running || result.err != nil {
					t.Fatal("stale local route blocked recovery", result)
				}
			case "state error":
				if !errors.Is(result.err, failure) {
					t.Fatal("state failure was hidden", result)
				}
			case "cleanup panic":
				if result.err == nil || !strings.Contains(result.err.Error(), "panic") || lookups != 0 {
					t.Fatal("cleanup panic allowed lookup", result)
				}
				panicCleanup = false
				mark := actor.Mark()
				actor.SendMessage(gen.PID{}, job)
				results = protocolMessages[messageIOResult](actor, mark)
				if len(results) != 1 || results[0].err != nil || !results[0].running {
					t.Fatal("worker did not recover after panic")
				}
			}
			mark := actor.Mark()
			actor.SendMessage(gen.PID{}, messageIO{owner: "leader@localhost", recoverAll: true})
			if len(protocolMessages[messageReplyFinished](actor, mark)) != 1 {
				t.Fatal("recovery forwarding did not release pending reply")
			}
		})
	}
}

func TestDaemonExitBurstKeepsAdmissionBoundedAndRechecksLateLookup(t *testing.T) {
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList("source@localhost"))
	actor := spawnDaemonUnit(t, book, "source@localhost")
	w := actor.Behavior().(*daemon)
	w.options.MaxInFlight = 8
	w.registrar = nil
	for i := 0; i < 512; i++ {
		actor.SendMessage(gen.PID{}, core.MessageDaemonExited{Ensure: core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: gen.Atom(fmt.Sprintf("task-%d", i))}}, PID: gen.PID{Node: w.Node().Name(), ID: uint64(i + 100)}})
	}
	if len(w.launching) != 8 || len(w.retries) != 0 || !w.wantRecovery {
		t.Fatal("exit burst exceeded admission or lost overflow recovery")
	}
	key := gen.Atom("task-0")
	old := w.launching[key]
	nextPID := gen.PID{Node: w.Node().Name(), ID: 9999}
	actor.SendMessage(gen.PID{}, core.MessageDaemonExited{Ensure: core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: key}}, PID: nextPID})
	actor.SendMessage(gen.PID{}, messageIOResult{key: key, epoch: old.Epoch, exited: old.Exited, running: true})
	next := w.launching[key]
	if next.Exited != nextPID || next.Epoch == old.Epoch || next.Phase != daemonLaunchPhaseChecking {
		t.Fatal("late lookup erased a newer exit")
	}
	actor.SendMessage(gen.PID{}, messageIOResult{key: key, epoch: old.Epoch, exited: old.Exited, running: true})
	if w.launching[key].Epoch != next.Epoch {
		t.Fatal("obsolete lookup replaced the current check")
	}
}
