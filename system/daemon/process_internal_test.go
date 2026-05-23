package daemon

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
	"github.com/qjpcpu/registrar/events"
)

type daemonTestProc struct{ act.Actor }

type daemonTestRegistrar struct{}

func (r *daemonTestRegistrar) Register(gen.NodeRegistrar, gen.RegisterRoutes) (gen.StaticRoutes, error) {
	return gen.StaticRoutes{}, nil
}
func (r *daemonTestRegistrar) Resolver() gen.Resolver         { return nil }
func (r *daemonTestRegistrar) RegisterProxy(gen.Atom) error   { return gen.ErrUnsupported }
func (r *daemonTestRegistrar) UnregisterProxy(gen.Atom) error { return gen.ErrUnsupported }
func (r *daemonTestRegistrar) RegisterApplicationRoute(gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}
func (r *daemonTestRegistrar) UnregisterApplicationRoute(gen.Atom) error { return gen.ErrUnsupported }
func (r *daemonTestRegistrar) Nodes() ([]gen.Atom, error)                { return nil, nil }
func (r *daemonTestRegistrar) Config(...string) (map[string]any, error) {
	return nil, gen.ErrUnsupported
}
func (r *daemonTestRegistrar) ConfigItem(string) (any, error) {
	return gen.Atom("node-a@127.0.0.1"), nil
}
func (r *daemonTestRegistrar) Event() (gen.Event, error) { return gen.Event{}, nil }
func (r *daemonTestRegistrar) Info() gen.RegistrarInfo   { return gen.RegistrarInfo{} }
func (r *daemonTestRegistrar) Terminate()                {}
func (r *daemonTestRegistrar) Version() gen.Version      { return gen.Version{} }

func spawnDaemonUnit(t *testing.T, book core.IAddressBook, self gen.Atom) *unit.TestActor {
	t.Helper()
	actor, err := unit.Spawn(t, FactoryWithOptions(book, Options{
		InitialRecoveryDelay:  time.Millisecond,
		LeaderRecoveryDelay:   time.Millisecond,
		NodeLeftRecoveryDelay: time.Millisecond,
		FullRecoveryInterval:  time.Hour,
		LaunchTimeout:         time.Millisecond,
		RunningGrace:          time.Millisecond,
		RetryInitialDelay:     time.Millisecond,
		RetryMaxDelay:         time.Millisecond,
		RecoveryJitterMax:     -1,
		RetryJitterMax:        -1,
	}), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn daemon actor: %v", err)
	}
	actor.ClearEvents()
	return actor
}

func findDaemonName(t *testing.T, book *core.AddressBook, owner gen.Atom, target gen.Atom) gen.Atom {
	t.Helper()
	return findDaemonNameWithPrefix(t, book, owner, target, "daemon")
}

func findDaemonNameWithPrefix(t *testing.T, book *core.AddressBook, owner gen.Atom, target gen.Atom, prefix string) gen.Atom {
	t.Helper()
	for i := 0; i < 2048; i++ {
		name := gen.Atom(fmt.Sprintf("%s-%s-%d", prefix, strings.ReplaceAll(string(owner), "@", "-"), i))
		if book.PickDirectoryNode(name) == owner && (target == "" || book.PickNode(name) == target) {
			return name
		}
	}
	t.Fatalf("failed to find daemon name for owner=%s target=%s", owner, target)
	return ""
}

func hasImportantSend(actor *unit.TestActor, to gen.ProcessID, match func(any) bool) bool {
	for _, event := range actor.Events() {
		send, ok := event.(unit.SendEvent)
		if !ok || !send.Important || send.To != to {
			continue
		}
		if match == nil || match(send.Message) {
			return true
		}
	}
	return false
}

func TestDaemonInitAndLaunchAllAfterScheduleMessages(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor, err := unit.Spawn(t, FactoryWithOptions(book, Options{
		RecoveryJitterMax: -1,
		RetryJitterMax:    -1,
	}), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn daemon actor: %v", err)
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageInit{}).
		Once().
		Assert()

	actor.ClearEvents()
	w := actor.Behavior().(*daemon)
	w.launchAllAfter(0)
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()

	actor.ClearEvents()
	w.launchAllAfter(time.Second)
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()
}

func TestDaemonHandleEventsUpdateLeadershipAndScheduleRecovery(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)

	if err := w.HandleEvent(gen.MessageEvent{Message: events.EventNodeSwitchedToLeader{Name: self}}); err != nil {
		t.Fatalf("leader event: %v", err)
	}
	if !w.isLeader {
		t.Fatal("expected daemon to become leader")
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()

	actor.ClearEvents()
	if err := w.HandleEvent(gen.MessageEvent{Message: events.EventNodeLeft{}}); err != nil {
		t.Fatalf("node left event: %v", err)
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()

	if err := w.HandleEvent(gen.MessageEvent{Message: events.EventNodeSwitchedToFollower{Name: self}}); err != nil {
		t.Fatalf("follower event: %v", err)
	}
	if w.isLeader {
		t.Fatal("expected daemon to become follower")
	}
}

func TestDaemonHandleEnsureDaemonForwardsAndStartsLaunch(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)

	remoteOwnerName := findDaemonName(t, book, remote, "")
	actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{
		Launcher: gen.Atom("launcher"),
		Process:  core.DaemonProcess{ProcessName: remoteOwnerName},
	})
	if !hasImportantSend(actor, gen.ProcessID{Name: ProcessName, Node: remote}, func(message any) bool {
		msg, ok := message.(core.MessageEnsureDaemon)
		return ok && msg.Process.ProcessName == remoteOwnerName
	}) {
		t.Fatalf("expected ensure daemon to be forwarded to owner %s", remote)
	}

	actor.ClearEvents()
	runningName := findDaemonNameWithPrefix(t, book, self, "", "running-daemon")
	if err := book.AddProcess(self, core.ProcessInfo{Name: runningName, Node: remote}); err != nil {
		t.Fatalf("add running daemon: %v", err)
	}
	actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{
		Launcher: gen.Atom("launcher"),
		Process:  core.DaemonProcess{ProcessName: runningName},
	})
	if len(w.launching) != 0 {
		t.Fatalf("running daemon should not be relaunched: %#v", w.launching)
	}

	launcherName := gen.Atom("ensure-launcher")
	if err := core.RegisterLauncher(launcherName, core.Launcher{
		Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
	}); err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() { core.UnregisterLauncher(launcherName) })

	actor.ClearEvents()
	localOwnerRemoteTarget := findDaemonNameWithPrefix(t, book, self, self, "launch-daemon")
	actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{
		Launcher: launcherName,
		Process:  core.DaemonProcess{ProcessName: localOwnerRemoteTarget},
	})
	if _, ok := w.launching[localOwnerRemoteTarget]; !ok {
		t.Fatalf("expected daemon launch state for %s", localOwnerRemoteTarget)
	}
	actor.ShouldSpawn().Once().Assert()
}

func TestDaemonEnsureDaemonReturnsNoAvailableNodes(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)

	err := w.handleEnsureDaemon(core.MessageEnsureDaemon{
		Launcher: gen.Atom("launcher"),
		Process:  core.DaemonProcess{ProcessName: gen.Atom("daemon")},
	})
	if !errors.Is(err, ErrNoAvailableNodes) {
		t.Fatalf("expected ErrNoAvailableNodes, got %v", err)
	}
}

func TestDaemonLaunchOneDaemonMissingLauncherSendsFailure(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)

	daemonName := gen.Atom("missing-launcher-daemon")
	actor.SendMessage(gen.PID{}, core.MessageLaunchOneDaemon{
		Launcher: gen.Atom("missing-launcher"),
		Process:  core.DaemonProcess{ProcessName: daemonName},
		Owner:    self,
		Epoch:    7,
	})
	if !hasImportantSend(actor, gen.ProcessID{Name: ProcessName, Node: self}, func(message any) bool {
		msg, ok := message.(core.MessageDaemonLaunchResult)
		return ok && msg.Name == daemonName && msg.State == daemonLaunchFailed && strings.Contains(msg.Err, "missing-launcher")
	}) {
		t.Fatal("expected failed daemon launch result")
	}
}

func TestDaemonLaunchOneDaemonSpawnsWorker(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	launcherName := gen.Atom("spawn-worker-launcher")
	if err := core.RegisterLauncher(launcherName, core.Launcher{
		Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
	}); err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() { core.UnregisterLauncher(launcherName) })

	actor.SendMessage(gen.PID{}, core.MessageLaunchOneDaemon{
		Launcher: launcherName,
		Process:  core.DaemonProcess{ProcessName: gen.Atom("worker-daemon")},
		Owner:    self,
		Epoch:    8,
	})
	actor.ShouldSpawn().Once().Assert()
}

func TestDaemonLaunchResultAndTimeoutStateMachine(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	daemonName := findDaemonName(t, book, self, self)
	state := daemonLaunchState{
		Launcher:   gen.Atom("launcher"),
		Process:    core.DaemonProcess{ProcessName: daemonName},
		TargetNode: self,
		Epoch:      11,
		Attempt:    1,
		Phase:      daemonLaunchPhaseLaunching,
		StartedAt:  time.Now().UTC(),
	}
	w.launching[daemonName] = state

	actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchResult{
		Name:  daemonName,
		Node:  self,
		Epoch: 11,
		State: daemonLaunchStarted,
	})
	if got := w.launching[daemonName].Phase; got != daemonLaunchPhaseRunningGrace {
		t.Fatalf("expected running grace phase, got %v", got)
	}

	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchResult{
		Name:  daemonName,
		Node:  self,
		Epoch: 11,
		State: daemonLaunchFailed,
		Err:   "boom",
	})
	if _, ok := w.launching[daemonName]; ok {
		t.Fatal("failed launch should clear launch state")
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		MessageMatching(func(message any) bool {
			msg, ok := message.(core.MessageEnsureDaemon)
			return ok && msg.Process.ProcessName == daemonName && msg.Attempt == 2
		}).
		Once().
		Assert()

	w.launching[daemonName] = state
	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, messageDaemonLaunchTimeout{Name: daemonName, Epoch: 11})
	if _, ok := w.launching[daemonName]; ok {
		t.Fatal("timeout should clear launch state")
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		MessageMatching(func(message any) bool {
			msg, ok := message.(core.MessageEnsureDaemon)
			return ok && msg.Process.ProcessName == daemonName && msg.Attempt == 2
		}).
		Once().
		Assert()
}

func TestDaemonInspectIncludesStateAndLauncher(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	w.recovered[gen.Atom("done")] = struct{}{}
	w.launching[gen.Atom("launching")] = daemonLaunchState{}

	launcherName := gen.Atom("inspect-launcher")
	if err := core.RegisterLauncher(launcherName, core.Launcher{
		Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
	}); err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() { core.UnregisterLauncher(launcherName) })

	stats := w.HandleInspect(gen.PID{})
	if stats["is_leader"] != "true" || stats["recovered_count"] != "1" || stats["launching_count"] != "1" {
		t.Fatalf("unexpected inspect stats: %#v", stats)
	}
	if !strings.Contains(stats["daemons"], string(launcherName)) {
		t.Fatalf("expected launcher in inspect stats: %#v", stats)
	}
}

func TestDaemonLeaderRecoveryScansLaunchersAndEnsuresDaemons(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true

	daemonName := findDaemonName(t, book, self, self)
	launcherName := gen.Atom("recovery-launcher")
	if err := core.RegisterLauncher(launcherName, core.Launcher{
		Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
		RecoveryScanner: func() core.DaemonIterator {
			calls := 0
			return func() ([]core.DaemonProcess, bool, error) {
				calls++
				if calls == 1 {
					return []core.DaemonProcess{{ProcessName: daemonName}}, true, nil
				}
				return nil, false, nil
			}
		},
	}); err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() { core.UnregisterLauncher(launcherName) })

	if err := w.leaderShouldRecoverDaemon(); err != nil {
		t.Fatalf("leader recover daemon: %v", err)
	}
	if _, ok := w.recovered[daemonName]; !ok {
		t.Fatalf("expected recovered daemon %s", daemonName)
	}
	if _, ok := w.launching[daemonName]; !ok {
		t.Fatalf("expected launching daemon %s", daemonName)
	}
	actor.ShouldSpawn().Once().Assert()
}

func TestDaemonRecoverDaemonScannerErrorsAndEmptyPage(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)

	wantErr := errors.New("scan failed")
	err := w.recoverDaemon(core.Launcher{
		Name: gen.Atom("error-launcher"),
		RecoveryScanner: func() core.DaemonIterator {
			return func() ([]core.DaemonProcess, bool, error) {
				return nil, false, wantErr
			}
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected scanner error, got %v", err)
	}

	err = w.recoverDaemon(core.Launcher{
		Name: gen.Atom("empty-launcher"),
		RecoveryScanner: func() core.DaemonIterator {
			return func() ([]core.DaemonProcess, bool, error) {
				return nil, true, nil
			}
		},
	})
	if err != nil {
		t.Fatalf("empty page with hasMore should only log and stop, got %v", err)
	}
}

func TestDaemonEnsureDaemonDispatchesToRemoteOwner(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	daemonName := findDaemonName(t, book, remote, "")

	if err := w.ensureDaemon(gen.Atom("launcher"), core.DaemonProcess{ProcessName: daemonName}, 3); err != nil {
		t.Fatalf("ensure daemon: %v", err)
	}
	if !hasImportantSend(actor, gen.ProcessID{Name: ProcessName, Node: remote}, func(message any) bool {
		msg, ok := message.(core.MessageEnsureDaemon)
		return ok && msg.Process.ProcessName == daemonName && msg.Attempt == 3
	}) {
		t.Fatalf("expected ensure daemon dispatch to remote owner %s", remote)
	}
}

func TestDaemonHandleMessageInitRetryAndLaunchAll(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.registrar = &daemonTestRegistrar{}

	actor.SendMessage(gen.PID{}, messageInit{})
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()

	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, core.MessageLaunchAllDaemon{})
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(core.MessageLaunchAllDaemon{}).
		Once().
		Assert()
}

func TestDaemonLaunchWorkerRegistersProcessAndReportsResult(t *testing.T) {
	launcherName := gen.Atom("worker-launcher")
	owner := gen.Atom("node-a@127.0.0.1")
	daemonName := gen.Atom("worker-managed-daemon")
	worker := &daemonLaunchWorker{
		launcher: core.Launcher{
			Name:    launcherName,
			Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
		},
		request: core.MessageLaunchOneDaemon{
			Launcher: launcherName,
			Process:  core.DaemonProcess{ProcessName: daemonName, Args: []any{"arg"}},
			Owner:    owner,
			Epoch:    42,
		},
	}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return worker }, unit.WithNodeName(owner))
	if err != nil {
		t.Fatalf("spawn launch worker: %v", err)
	}
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, messageInit{})
	actor.ShouldSpawn().Once().Assert()
	actor.ShouldSend().
		To(core.WhereIsProcess).
		MessageMatching(func(message any) bool {
			msg, ok := message.(core.MessageRegisterLocalProcess)
			return ok && msg.Name == daemonName && msg.PID != (gen.PID{})
		}).
		Once().
		Assert()
	if !hasImportantSend(actor, gen.ProcessID{Name: ProcessName, Node: owner}, func(message any) bool {
		msg, ok := message.(core.MessageDaemonLaunchResult)
		return ok && msg.Name == daemonName && msg.Epoch == 42 && msg.State == daemonLaunchStarted
	}) {
		t.Fatal("expected successful launch result")
	}
	if !actor.IsTerminated() || actor.TerminationReason() != gen.TerminateReasonNormal {
		t.Fatalf("worker should terminate normally, terminated=%v reason=%v", actor.IsTerminated(), actor.TerminationReason())
	}
}

func TestDaemonLaunchWorkerReportsTakenAndFailedSpawn(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		state gen.Atom
	}{
		{name: "taken", err: gen.ErrTaken, state: daemonLaunchTaken},
		{name: "failed", err: errors.New("spawn failed"), state: daemonLaunchFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner := gen.Atom("node-a@127.0.0.1")
			daemonName := gen.Atom("worker-" + tt.name)
			worker := &daemonLaunchWorker{
				launcher: core.Launcher{
					Name:    gen.Atom("worker-launcher-" + tt.name),
					Factory: func() gen.ProcessBehavior { return &daemonTestProc{} },
				},
				request: core.MessageLaunchOneDaemon{
					Process: core.DaemonProcess{ProcessName: daemonName},
					Owner:   owner,
					Epoch:   43,
				},
			}
			actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return worker }, unit.WithNodeName(owner))
			if err != nil {
				t.Fatalf("spawn launch worker: %v", err)
			}
			actor.ClearEvents()
			actor.Process().SetMethodFailure("Spawn", tt.err)

			actor.SendMessage(gen.PID{}, messageInit{})
			if !hasImportantSend(actor, gen.ProcessID{Name: ProcessName, Node: owner}, func(message any) bool {
				msg, ok := message.(core.MessageDaemonLaunchResult)
				return ok && msg.Name == daemonName && msg.State == tt.state
			}) {
				t.Fatalf("expected launch result state %s", tt.state)
			}
		})
	}
}

func TestDaemonFactoryHandleCallAndSendLaunchResultEdges(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	actor, err := unit.Spawn(t, Factory(book), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn daemon with default factory: %v", err)
	}
	result := actor.Call(gen.PID{}, "anything")
	if result.Error != nil || result.Response != nil {
		t.Fatalf("daemon call should return nil nil, got response=%v err=%v", result.Response, result.Error)
	}

	w := actor.Behavior().(*daemon)
	w.sendLaunchResult("", core.MessageDaemonLaunchResult{Name: gen.Atom("ignored")})
	actor.Process().SetMethodFailurePattern("SendImportant", string(self), errors.New("send failed"))
	w.sendLaunchResult(self, core.MessageDaemonLaunchResult{Name: gen.Atom("failed-send")})
}

func TestDaemonHandleEnsureDaemonLaunchInProgressBranches(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	name := findDaemonName(t, book, self, self)
	state := daemonLaunchState{
		Launcher:   gen.Atom("launcher"),
		Process:    core.DaemonProcess{ProcessName: name},
		TargetNode: self,
		Epoch:      1,
		Phase:      daemonLaunchPhaseLaunching,
		StartedAt:  time.Now().UTC(),
	}
	w.launching[name] = state
	actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{
		Launcher: state.Launcher,
		Process:  state.Process,
	})
	for _, event := range actor.Events() {
		if _, ok := event.(unit.SpawnEvent); ok {
			t.Fatal("launching daemon should not spawn another worker")
		}
	}

	state.Phase = daemonLaunchPhaseRunningGrace
	w.launching[name] = state
	if err := book.AddProcess(self, core.ProcessInfo{Name: name, Node: self}); err != nil {
		t.Fatalf("add running process: %v", err)
	}
	actor.SendMessage(gen.PID{}, core.MessageEnsureDaemon{
		Launcher: state.Launcher,
		Process:  state.Process,
	})
	if _, ok := w.launching[name]; ok {
		t.Fatal("running daemon should clear running-grace launch state")
	}
}

func TestDaemonLaunchResultAndTimeoutIgnoreStaleOrRemoteOwner(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	localStale := findDaemonName(t, book, self, "")
	w.launching[localStale] = daemonLaunchState{Epoch: 4}
	actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchResult{Name: localStale, Epoch: 3})
	if _, ok := w.launching[localStale]; !ok {
		t.Fatal("stale epoch should keep launch state")
	}

	remoteOwned := findDaemonName(t, book, remote, "")
	w.launching[remoteOwned] = daemonLaunchState{Epoch: 4}
	actor.SendMessage(gen.PID{}, core.MessageDaemonLaunchResult{Name: remoteOwned, Epoch: 4})
	if _, ok := w.launching[remoteOwned]; ok {
		t.Fatal("result for remote owner should clear local launch state")
	}

	localName := findDaemonName(t, book, self, self)
	w.launching[localName] = daemonLaunchState{
		Launcher:  gen.Atom("launcher"),
		Process:   core.DaemonProcess{ProcessName: localName},
		Epoch:     5,
		Attempt:   1,
		StartedAt: time.Now().UTC(),
	}
	actor.SendMessage(gen.PID{}, messageDaemonLaunchTimeout{Name: localName, Epoch: 4})
	if _, ok := w.launching[localName]; !ok {
		t.Fatal("stale timeout should keep launch state")
	}
	if err := book.AddProcess(self, core.ProcessInfo{Name: localName, Node: self}); err != nil {
		t.Fatalf("add running process: %v", err)
	}
	actor.SendMessage(gen.PID{}, messageDaemonLaunchTimeout{Name: localName, Epoch: 5})
	if _, ok := w.launching[localName]; ok {
		t.Fatal("timeout should clear launch state when daemon is running")
	}
}
