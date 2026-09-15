package daemon

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

type blockingPeerMonitor struct {
	gen.Process
	entered chan struct{}
	finish  chan struct{}
	err     error
}

func (p blockingPeerMonitor) MonitorProcessID(gen.ProcessID) error {
	close(p.entered)
	<-p.finish
	return p.err
}

type controlPeerMonitor struct {
	gen.Process
	t *testing.T
}

func (p controlPeerMonitor) MonitorProcessID(gen.ProcessID) error {
	p.t.Error("control callback attempted remote monitor establishment")
	return gen.ErrNotAllowed
}

func TestSlowPeerMonitorLeavesDaemonResponsive(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			book := core.NewAddressBook()
			remote := gen.Atom("slow@localhost")
			book.SetAvailableNodes(core.NewNodeList(remote))
			actor := spawnDaemonUnit(t, book, "source@localhost")
			w := actor.Behavior().(*daemon)
			w.Process = controlPeerMonitor{Process: w.Process, t: t}
			for _, key := range []gen.Atom{"first", "second"} {
				w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: key}})
				w.handleIOResult(messageIOResult{key: key, epoch: w.launching[key].Epoch})
			}
			incoming := core.MessageDaemonLaunchOffer{Name: "incoming", Owner: remote, Epoch: 7}
			w.handleLaunchOffer(incoming)
			w.handleLaunchOffer(incoming)
			watch := w.peerWatches[remote]
			if watch == nil || len(watch.offers) != 1 {
				t.Fatal("monitor requests were not coalesced")
			}
			if len(peerMonitorJobs(actor)) != 1 {
				t.Fatal("multiple monitor jobs for one peer")
			}
			if len(protocolMessages[core.MessageDaemonLaunchOffer](actor, 0)) != 0 {
				t.Fatal("offered before monitor completion")
			}
			workerActor, err := actor.Node().Spawn(func() gen.ProcessBehavior { return &daemonIOWorker{parent: actor.PID()} }, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			worker := workerActor.Behavior().(*daemonIOWorker)
			entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(finish) })
			var monitorErr error
			if failure {
				monitorErr = errors.New("peer unreachable")
			}
			worker.Process = blockingPeerMonitor{Process: worker.Process, entered: entered, finish: finish, err: monitorErr}
			go func() { workerActor.SendMessage(actor.PID(), messageIO{watch: watch}); close(done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("worker did not start monitoring")
			}
			progress := protocolMessages[messagePeerMonitor](workerActor, 0)
			if len(progress) != 1 || progress[0].ready {
				t.Fatal("worker did not announce monitor ownership")
			}
			w.HandleMessage(workerActor.PID(), progress[0])
			// The remote call is still blocked. A local launch can complete, and a
			// withdrawal must remove an inbound offer before monitor completion.
			local := core.MessageDaemonLaunchOffer{Name: "local", Owner: w.Node().Name(), Epoch: 1}
			w.HandleMessage(actor.PID(), local)
			entry := startPulledTask(w, "local", pullTestLauncher(t))
			finishPulledTask(w, entry)
			if w.pendingLaunch["local"] != nil {
				t.Fatal("unrelated launch did not finish")
			}
			w.HandleMessage(actor.PID(), core.MessageDaemonLaunchWithdraw{Name: incoming.Name, Owner: remote, Epoch: incoming.Epoch})
			if len(watch.offers) != 0 {
				t.Fatal("withdrawal did not cancel waiting offer")
			}
			once.Do(func() { close(finish) })
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("worker did not return")
			}
			progress = protocolMessages[messagePeerMonitor](workerActor, 0)
			if len(progress) != 2 || !progress[1].ready {
				t.Fatal("monitor completion missing")
			}
			w.HandleMessage(workerActor.PID(), progress[1])
			if w.pendingLaunch[incoming.Name] != nil {
				t.Fatal("withdrawn offer resumed")
			}
			if failure {
				if w.peers[remote] || len(w.retries) != 2 {
					t.Fatal("failed monitoring did not retry source tasks")
				}
				return
			}
			if !w.peers[remote] || len(protocolMessages[core.MessageDaemonLaunchOffer](actor, 0)) != 2 {
				t.Fatal("successful monitoring did not resume both offers")
			}
			mark := workerActor.Mark()
			workerActor.SendMessage(gen.PID{}, gen.MessageDownProcessID{ProcessID: gen.ProcessID{Name: ProcessName, Node: remote}, Reason: gen.TerminateReasonKill})
			downs := protocolMessages[messagePeerDown](workerActor, mark)
			if len(downs) != 1 {
				t.Fatal("worker did not forward peer exit")
			}
			w.HandleMessage(workerActor.PID(), downs[0])
			if w.peers[remote] || len(w.retries) != 2 {
				t.Fatal("forwarded peer exit did not retry source tasks")
			}
		})
	}
}

func peerMonitorJobs(actor *unit.Subject) []*peerWatch {
	var jobs []*peerWatch
	// Inspect envelopes directly: protocolMessages unwraps protocol payloads.
	for _, record := range actor.Records() {
		if send, ok := record.(check.Send); ok {
			if job, ok := send.Message.(messageIO); ok && job.watch != nil {
				jobs = append(jobs, job.watch)
			}
		}
	}
	return jobs
}

func TestPeerMonitorWorkerExitRetriesAndIgnoresLateMessages(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			book := core.NewAddressBook()
			remote := gen.Atom("peer@localhost")
			book.SetAvailableNodes(core.NewNodeList(remote))
			actor := spawnDaemonUnit(t, book, "source@localhost")
			w := actor.Behavior().(*daemon)
			w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
			w.handleIOResult(messageIOResult{key: "task", epoch: w.launching["task"].Epoch})
			old := w.peerWatches[remote]
			worker := gen.PID{Node: w.Node().Name(), ID: 999, Creation: 1}
			w.handlePeerMonitor(messagePeerMonitor{watch: old, worker: worker})
			if ready {
				w.handlePeerMonitor(messagePeerMonitor{watch: old, worker: worker, ready: true})
			}
			w.HandleMessage(worker, gen.MessageDownPID{PID: worker, Reason: gen.TerminateReasonKill})
			if w.peers[remote] || w.launching["task"].Phase != daemonLaunchPhaseRetrying {
				t.Fatal("worker exit left task waiting on a lost monitor")
			}
			w.HandleMessage(actor.PID(), messageRetry{Name: "task", Epoch: w.launching["task"].Epoch})
			w.handleIOResult(messageIOResult{key: "task", epoch: w.launching["task"].Epoch})
			next := w.peerWatches[remote]
			if next == nil || next == old {
				t.Fatal("retry did not establish a fresh watch")
			}
			w.HandleMessage(worker, messagePeerMonitor{watch: old, worker: worker, ready: true})
			w.HandleMessage(worker, messagePeerDown{watch: old})
			if w.peers[remote] || w.peerWatches[remote] != next || w.launching["task"].Phase != daemonLaunchPhaseMonitoring {
				t.Fatal("late monitor messages affected retry")
			}
			w.handlePeerMonitor(messagePeerMonitor{watch: next, ready: true})
			if w.launching["task"].Phase != daemonLaunchPhaseOffering {
				t.Fatal("fresh monitor did not resume task")
			}
		})
	}
}

func TestTopologyChangeDuringPeerMonitoringUsesNewTarget(t *testing.T) {
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList("old@localhost"))
	actor := spawnDaemonUnit(t, book, "source@localhost")
	w := actor.Behavior().(*daemon)
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
	w.handleIOResult(messageIOResult{key: "task", epoch: w.launching["task"].Epoch})
	old := w.peerWatches["old@localhost"]
	book.SetAvailableNodes(core.NewNodeList("new@localhost"))
	w.HandleMessage(actor.PID(), core.MessageTopologyUpdated{})
	w.handleIOResult(messageIOResult{key: "task", epoch: w.launching["task"].Epoch})
	w.handlePeerMonitor(messagePeerMonitor{watch: old, ready: true})
	if len(protocolMessages[core.MessageDaemonLaunchOffer](actor, 0)) != 0 {
		t.Fatal("late monitor completion offered task to old target")
	}
	w.handlePeerMonitor(messagePeerMonitor{watch: w.peerWatches["new@localhost"], ready: true})
	offers := protocolMessages[core.MessageDaemonLaunchOffer](actor, 0)
	if len(offers) != 1 || w.launching["task"].TargetNode != "new@localhost" {
		t.Fatal("new target did not receive the task")
	}
}
