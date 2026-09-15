package daemon

import (
	"fmt"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

func protocolMessages[T any](actor *unit.Subject, after int) []T {
	var messages []T
	for _, record := range actor.Records()[after:] {
		if send, ok := record.(check.Send); ok {
			message := send.Message
			if job, ok := message.(messageIO); ok {
				message = job.message
			}
			if msg, ok := message.(T); ok {
				messages = append(messages, msg)
			}
		}
	}
	return messages
}

func pullTestLauncher(t *testing.T) gen.Atom {
	t.Helper()
	name := gen.Atom(t.Name())
	if err := core.RegisterLauncher(name, core.Launcher{Factory: func() gen.ProcessBehavior { return &daemonTestProc{} }}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { core.UnregisterLauncher(name) })
	return name
}

func startPulledTask(w *daemon, name, launcher gen.Atom) *launchEntry {
	entry := w.pendingLaunch[name]
	w.handleLaunchOneDaemon(core.MessageLaunchOneDaemon{Launcher: launcher, Process: core.DaemonProcess{ProcessName: name}, Owner: entry.selected.Owner, Epoch: entry.selected.Epoch})
	return entry
}

func finishPulledTask(w *daemon, entry *launchEntry) {
	w.finishLaunch(entry.worker, messageLaunchFinished{owner: entry.selected.Owner, result: core.MessageDaemonLaunchResult{
		Name: entry.name, Node: w.Node().Name(), Epoch: entry.selected.Epoch, State: daemonLaunchStarted,
	}})
}

func TestIdleWorkerPullsNextTaskWhileOtherWorkersStayBusy(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	launcher := pullTestLauncher(t)
	const total = 24
	for i := 0; i < total; i++ {
		acceptTestLaunchOffer(w, core.MessageDaemonLaunchOffer{Name: gen.Atom(fmt.Sprint(i)), Owner: "source@localhost", Epoch: int64(i + 1)})
	}
	if got := len(protocolMessages[core.MessageDaemonLaunchPull](actor, 0)); got != 8 {
		t.Fatalf("initial pulls = %d, want 8", got)
	}
	for i := 0; i < 8; i++ {
		startPulledTask(w, gen.Atom(fmt.Sprint(i)), launcher)
	}
	fast := w.pendingLaunch["0"]
	worker := fast.worker
	for i := 8; i < total; i++ {
		mark := actor.Mark()
		finishPulledTask(w, fast)
		pulls := protocolMessages[core.MessageDaemonLaunchPull](actor, mark)
		if len(pulls) != 1 || pulls[0].Name != gen.Atom(fmt.Sprint(i)) {
			t.Fatalf("completion did not pull next task: %+v", pulls)
		}
		fast = startPulledTask(w, pulls[0].Name, launcher)
		if fast.worker != worker || !fast.running {
			t.Fatal("next task did not use the worker that just finished")
		}
	}
	finishPulledTask(w, fast)
	for i := 1; i < 8; i++ {
		finishPulledTask(w, w.pendingLaunch[gen.Atom(fmt.Sprint(i))])
	}
	if len(w.pendingLaunch) != 0 || len(w.idleWorkers) != 8 {
		t.Fatal("workers did not return to idle after draining tasks")
	}
}

func TestWaitingTaskSurvivesTimeoutAndPullBeforeConfirmation(t *testing.T) {
	for _, pullFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(pullFirst), func(t *testing.T) {
			book := core.NewAddressBook()
			book.SetAvailableNodes(core.NewNodeList("target@localhost"))
			actor := spawnDaemonUnit(t, book, "source@localhost")
			w := actor.Behavior().(*daemon)
			w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
			state := w.launching["task"]
			acceptTestIOResult(w, messageIOResult{key: "task", epoch: state.Epoch})
			offerTimeout := messageDaemonLaunchTimeout{Name: "task", Epoch: state.Epoch, Phase: daemonLaunchPhaseOffering}
			mark := actor.Mark()
			w.handleDaemonLaunchTimeout(offerTimeout)
			if got := protocolMessages[core.MessageDaemonLaunchOffer](actor, mark); len(got) != 1 || got[0].Epoch != state.Epoch {
				t.Fatal("unconfirmed notice did not resend with the same identity")
			}
			pull := core.MessageDaemonLaunchPull{Name: "task", Node: state.TargetNode, Epoch: state.Epoch}
			if pullFirst {
				w.handleLaunchPull(pull)
			}
			w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "task", Node: state.TargetNode, Epoch: state.Epoch, State: daemonLaunchQueued})
			w.handleDaemonLaunchTimeout(offerTimeout)
			if !pullFirst {
				waiting := w.launching["task"]
				if waiting.Phase != daemonLaunchPhaseWaiting || waiting.Cancel != nil || len(w.retries) != 0 {
					t.Fatal("acknowledged task should wait for capacity without a launch timer")
				}
				w.handleLaunchPull(pull)
			}
			started := w.launching["task"]
			if started.Phase != daemonLaunchPhaseLaunching || started.Cancel == nil || len(w.retries) != 0 {
				t.Fatal("pull should start launch deadline despite a late offer timeout or confirmation")
			}
			launches := protocolMessages[core.MessageLaunchOneDaemon](actor, 0)
			if len(launches) != 1 || launches[0].Process.ProcessName != "task" {
				t.Fatalf("unexpected task delivery: %+v", launches)
			}
		})
	}
}

func TestPullCoalescesOwnersAndReportsEveryRequest(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	launcher := pullTestLauncher(t)
	a := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-a@localhost", Epoch: 1}
	b := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-b@localhost", Epoch: 2}
	acceptTestLaunchOffer(w, a)
	acceptTestLaunchOffer(w, a)
	acceptTestLaunchOffer(w, b)
	if len(w.pendingLaunch["task"].waiters) != 2 || len(w.idleWorkers) != 7 {
		t.Fatal("duplicate task did not share one worker")
	}
	entry := startPulledTask(w, "task", launcher)
	mark := actor.Mark()
	finishPulledTask(w, entry)
	results := protocolMessages[core.MessageDaemonLaunchResult](actor, mark)
	if len(results) != 2 || results[0].Epoch != 1 || results[1].Epoch != 2 || results[0].State != daemonLaunchStarted || results[1].State != daemonLaunchStarted {
		t.Fatalf("completion did not report to both requests: %+v", results)
	}
}

func TestWithdrawalReassignsReservationAndKeepsRunningWorkerBusy(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	launcher := pullTestLauncher(t)
	a := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-a@localhost", Epoch: 1}
	b := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-b@localhost", Epoch: 2}
	acceptTestLaunchOffer(w, a)
	acceptTestLaunchOffer(w, b)
	w.withdrawLaunch(core.MessageDaemonLaunchWithdraw{Name: a.Name, Owner: a.Owner, Epoch: a.Epoch})
	if w.pendingLaunch["task"].selected != b || len(w.idleWorkers) != 7 {
		t.Fatal("remaining owner did not receive a reservation")
	}
	w.handleLaunchOneDaemon(core.MessageLaunchOneDaemon{Launcher: launcher, Process: core.DaemonProcess{ProcessName: a.Name}, Owner: a.Owner, Epoch: a.Epoch})
	if w.pendingLaunch["task"].running {
		t.Fatal("withdrawn handoff started a task")
	}
	entry := startPulledTask(w, "task", launcher)
	w.withdrawLaunch(core.MessageDaemonLaunchWithdraw{Name: b.Name, Owner: b.Owner, Epoch: b.Epoch})
	if len(w.idleWorkers) != 7 || w.pendingLaunch["task"] != entry {
		t.Fatal("running Init released its worker before returning")
	}
	finishPulledTask(w, entry)
	if len(w.idleWorkers) != 8 || len(w.pendingLaunch) != 0 {
		t.Fatal("completion did not release the withdrawn task")
	}
}

func TestReservationTimeoutAndWorkerExitContinuePulling(t *testing.T) {
	for _, workerExit := range []bool{false, true} {
		t.Run(fmt.Sprint(workerExit), func(t *testing.T) {
			actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
			w := actor.Behavior().(*daemon)
			for i := 0; i < 9; i++ {
				acceptTestLaunchOffer(w, core.MessageDaemonLaunchOffer{Name: gen.Atom(fmt.Sprint(i)), Owner: "source@localhost", Epoch: int64(i + 1)})
			}
			entry := w.pendingLaunch["0"]
			mark := actor.Mark()
			if workerExit {
				w.launchWorkerDown(entry.worker, gen.TerminateReasonKill)
			} else {
				w.reservationTimeout(messageLaunchReservationTimeout{entry: entry, offer: entry.selected})
			}
			pulls := protocolMessages[core.MessageDaemonLaunchPull](actor, mark)
			if len(pulls) != 1 || pulls[0].Name != "8" || len(w.launchWorkers) != 8 {
				t.Fatalf("failure stalled next task: %+v", pulls)
			}
			results := protocolMessages[core.MessageDaemonLaunchResult](actor, mark)
			if len(results) != 1 || results[0].Name != "0" || results[0].State != daemonLaunchFailed {
				t.Fatalf("failure was not reported: %+v", results)
			}
		})
	}
}

func TestPeerExitReleasesOffersAndRetriesInitiatedTasks(t *testing.T) {
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList("peer@localhost"))
	actor := spawnDaemonUnit(t, book, "self@localhost")
	w := actor.Behavior().(*daemon)
	for i := 0; i < 12; i++ {
		acceptTestLaunchOffer(w, core.MessageDaemonLaunchOffer{Name: gen.Atom(fmt.Sprint(i)), Owner: "peer@localhost", Epoch: int64(i + 1)})
	}
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "outgoing"}})
	state := w.launching["outgoing"]
	acceptTestIOResult(w, messageIOResult{key: "outgoing", epoch: state.Epoch})
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "outgoing", Node: state.TargetNode, Epoch: state.Epoch, State: daemonLaunchQueued})
	w.peerDown("peer@localhost")
	if len(w.pendingLaunch) != 0 || len(w.idleWorkers) != 8 || len(w.launchQueue) != 0 {
		t.Fatal("peer exit retained target reservations")
	}
	if len(w.retries) != 1 || w.launching["outgoing"].Phase != daemonLaunchPhaseRetrying {
		t.Fatal("peer exit did not retry task waiting for a pull")
	}
}

func TestTopologyRetargetsWaitingTask(t *testing.T) {
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList("old@localhost"))
	actor := spawnDaemonUnit(t, book, "source@localhost")
	w := actor.Behavior().(*daemon)
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: "task"}})
	old := w.launching["task"]
	acceptTestIOResult(w, messageIOResult{key: "task", epoch: old.Epoch})
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "task", Node: old.TargetNode, Epoch: old.Epoch, State: daemonLaunchQueued})
	book.SetAvailableNodes(core.NewNodeList("new@localhost"))
	w.retargetTasks()
	next := w.launching["task"]
	if next.TargetNode != "new@localhost" || next.Epoch == old.Epoch || next.Phase != daemonLaunchPhaseChecking {
		t.Fatal("topology change did not restart lookup for the new target")
	}
	withdrawals := protocolMessages[core.MessageDaemonLaunchWithdraw](actor, 0)
	if len(withdrawals) != 1 || withdrawals[0].Epoch != old.Epoch {
		t.Fatal("old target's notice was not withdrawn")
	}
}

func TestCoalescedCompletionRetriesUntilAcknowledged(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	launcher := pullTestLauncher(t)
	a := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-a@localhost", Epoch: 1}
	b := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source-b@localhost", Epoch: 2}
	acceptTestLaunchOffer(w, a)
	acceptTestLaunchOffer(w, b)
	finishPulledTask(w, startPulledTask(w, "task", launcher))
	w.withdrawLaunch(core.MessageDaemonLaunchWithdraw{Name: a.Name, Owner: a.Owner, Epoch: a.Epoch})
	if len(w.launchReplies) != 1 || w.launchReplies[b] == nil || len(w.idleWorkers) != 8 {
		t.Fatal("completion acknowledgement should leave only the other owner's reply pending")
	}
	mark := actor.Mark()
	w.retryLaunchResult(messageLaunchResultTimeout{offer: b, reply: w.launchReplies[b]})
	results := protocolMessages[core.MessageDaemonLaunchResult](actor, mark)
	if len(results) != 1 || results[0].Epoch != b.Epoch || results[0].State != daemonLaunchStarted {
		t.Fatalf("unacknowledged coalesced result was not retransmitted: %+v", results)
	}
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList("target@localhost"))
	sourceActor := spawnDaemonUnit(t, book, b.Owner)
	source := sourceActor.Behavior().(*daemon)
	source.launching[b.Name] = daemonLaunchState{Epoch: b.Epoch, TargetNode: "target@localhost", Phase: daemonLaunchPhaseWaiting}
	source.handleDaemonLaunchResult(results[0])
	source.handleDaemonLaunchResult(results[0])
	acks := protocolMessages[core.MessageDaemonLaunchWithdraw](sourceActor, 0)
	if len(acks) != 2 || acks[0] != acks[1] || len(source.launching) != 0 {
		t.Fatal("both first completion and duplicate completion should be acknowledged")
	}
	w.withdrawLaunch(acks[1])
	if len(w.launchReplies) != 0 {
		t.Fatal("acknowledgement did not release the retained result")
	}
}

func TestLateOfferReceivesRetainedCompletion(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "target@localhost")
	w := actor.Behavior().(*daemon)
	launcher := pullTestLauncher(t)
	offer := core.MessageDaemonLaunchOffer{Name: "task", Owner: "source@localhost", Epoch: 1}
	acceptTestLaunchOffer(w, offer)
	finishPulledTask(w, startPulledTask(w, "task", launcher))
	mark := actor.Mark()
	acceptTestLaunchOffer(w, offer)
	results := protocolMessages[core.MessageDaemonLaunchResult](actor, mark)
	if len(results) != 1 || results[0].State != daemonLaunchStarted || len(w.idleWorkers) != 8 {
		t.Fatal("late notification did not receive the completed result")
	}
	w.peerDown(offer.Owner)
	if len(w.launchReplies) != 0 {
		t.Fatal("peer exit did not release undelivered results")
	}
}

// Pull protocol tests complete the separately tested monitor handshake explicitly.
func completeTestPeerWatches(w *daemon) {
	for _, watch := range w.peerWatches {
		if !w.peers[watch.node] {
			w.handlePeerMonitor(messagePeerMonitor{watch: watch, ready: true})
		}
	}
}
func acceptTestLaunchOffer(w *daemon, msg core.MessageDaemonLaunchOffer) {
	w.handleLaunchOffer(msg)
	completeTestPeerWatches(w)
}
func acceptTestIOResult(w *daemon, msg messageIOResult) {
	w.handleIOResult(msg)
	completeTestPeerWatches(w)
}
