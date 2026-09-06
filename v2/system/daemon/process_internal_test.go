package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"github.com/qjpcpu/registrar/events"
)

func daemonTestDecorator(_ gen.Atom, factory gen.ProcessFactory) gen.ProcessFactory {
	return factory
}

type daemonRouteStore struct {
	mu     sync.RWMutex
	routes map[gen.Atom]gen.PID
}

var daemonRouteStores sync.Map

func daemonRouteStoreFor(book *core.AddressBook) *daemonRouteStore {
	value, _ := daemonRouteStores.LoadOrStore(book, &daemonRouteStore{routes: make(map[gen.Atom]gen.PID)})
	return value.(*daemonRouteStore)
}

func (s *daemonRouteStore) Locate(_ context.Context, key gen.Atom) (gen.PID, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pid, found := s.routes[key]
	return pid, found, nil
}

func setDaemonRoute(book *core.AddressBook, key gen.Atom, pid gen.PID) {
	store := daemonRouteStoreFor(book)
	store.mu.Lock()
	store.routes[key] = pid
	store.mu.Unlock()
}

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
	if concrete, ok := book.(*core.AddressBook); ok {
		if err := concrete.BindLocator(self, daemonRouteStoreFor(concrete).Locate); err != nil {
			t.Fatalf("bind test locator: %v", err)
		}
	}
	actor, err := unit.Spawn(t, FactoryWithOptions(book, daemonTestDecorator, Options{
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
		if book.PickCoordinatorNode(name) == owner && (target == "" || book.PickNode(name) == target) {
			return name
		}
	}
	t.Fatalf("failed to find daemon name for owner=%s target=%s", owner, target)
	return ""
}

func hasDaemonSend(actor *unit.TestActor, to gen.ProcessID, match func(any) bool) bool {
	for _, event := range actor.Events() {
		send, ok := event.(unit.SendEvent)
		if !ok || send.Important || send.To != to {
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
	actor, err := unit.Spawn(t, FactoryWithOptions(book, daemonTestDecorator, Options{
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

func TestDaemonSchedulesLocateRetryAndReusesRegistrar(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.registrar = &daemonTestRegistrar{}
	if err := w.setupRegistrarMonitoring(); err != nil {
		t.Fatal(err)
	}
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "l", Process: core.DaemonProcess{ProcessName: "key"}, Attempt: 2})
	state := w.launching["key"]
	w.handleIOResult(messageIOResult{key: "key", epoch: state.Epoch, err: errors.New("store unavailable")})
	if len(w.launching) != 1 || len(w.retries) != 1 || w.launching["key"].Attempt != 3 {
		t.Fatal(w.launching, w.retries)
	}
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
	if err := w.HandleMessage(actor.PID(), core.MessageTopologyUpdated{}); err != nil {
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
	self, remote := gen.Atom("node-a@127.0.0.1"), gen.Atom("node-b@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(self, remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	key := findDaemonName(t, book, remote, "")
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: key}})
	runDaemonIO(t, actor, w)
	if !hasDaemonSend(actor, gen.ProcessID{Name: ProcessName, Node: remote}, func(v any) bool {
		m, ok := v.(core.MessageLaunchOneDaemon)
		return ok && m.Owner == self && m.Process.ProcessName == key
	}) {
		t.Fatal("launch was not sent directly to target")
	}
	if len(w.launching) != 1 {
		t.Fatal("remote work not counted")
	}

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
		Owner:    gen.Atom("remote@localhost"),
		Epoch:    7,
	})
	runDaemonIO(t, actor, actor.Behavior().(*daemon))
	if !hasDaemonSend(actor, gen.ProcessID{Name: ProcessName, Node: "remote@localhost"}, func(message any) bool {
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
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	for _, result := range []core.MessageDaemonLaunchResult{{State: daemonLaunchStarted}, {State: daemonLaunchTaken}, {State: daemonLaunchNotNeeded}, {State: daemonLaunchFailed, Err: gen.TerminateReasonNormal.Error()}} {
		w.launching["key"] = daemonLaunchState{Epoch: 11, TargetNode: self}
		result.Name = "key"
		result.Node = self
		result.Epoch = 11
		w.handleDaemonLaunchResult(result)
		if len(w.launching) != 0 {
			t.Fatal("completed launch holds capacity", result)
		}
	}
	w.launching["key"] = daemonLaunchState{Epoch: 11, TargetNode: self, Attempt: 1}
	w.handleDaemonLaunchTimeout(messageDaemonLaunchTimeout{Name: "key", Epoch: 11})
	if len(w.launching) != 1 || len(w.retries) != 1 {
		t.Fatal("retry lost capacity accounting")
	}

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
	driveRecoveryScan(t, actor, w)
	if _, ok := w.launching[daemonName]; !ok {
		t.Fatalf("expected launching daemon %s", daemonName)
	}
	actor.ShouldSpawn().Once().Assert()
}

func TestDaemonScannerErrorAndEmptyPage(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		more bool
	}{{"error", errors.New("scan failed"), false}, {"empty", nil, true}} {
		t.Run(test.name, func(t *testing.T) {
			book := core.NewAddressBook()
			actor := spawnDaemonUnit(t, book, "node-a@127.0.0.1")
			w := actor.Behavior().(*daemon)
			w.isLeader = true
			w.scan = &recoveryScan{launchers: []core.Launcher{{Name: "scanner"}}}
			w.HandleMessage(actor.PID(), messageScanPage{scan: w.scan, err: test.err, more: test.more})
			driveRecoveryScan(t, actor, w)
			if w.scan != nil {
				t.Fatal("scan did not complete")
			}
		})
	}
}

func TestDaemonEnsureDaemonDispatchesToRemoteOwner(t *testing.T) {
	book := core.NewAddressBook()
	self, remote := gen.Atom("node-a@127.0.0.1"), gen.Atom("node-b@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	if err := w.ensureDaemon("launcher", core.DaemonProcess{ProcessName: "key"}, 3); err != nil {
		t.Fatal(err)
	}
	if len(w.launching) != 1 || w.launching["key"].TargetNode != remote {
		t.Fatal("remote task not tracked")
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

func TestDaemonLaunchWorkerSpawnsRoutedProcessAndReportsResult(t *testing.T) {
	launcherName := gen.Atom("worker-launcher")
	owner := gen.Atom("node-a@127.0.0.1")
	daemonName := gen.Atom("worker-managed-daemon")
	request := messageLaunch{
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
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &daemonLaunchWorker{} }, unit.WithNodeName(owner))
	if err != nil {
		t.Fatalf("spawn launch worker: %v", err)
	}
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, request)
	actor.ShouldSpawn().Once().Assert()
	if !hasLaunchCompletion(actor, func(message any) bool {
		msg, ok := message.(core.MessageDaemonLaunchResult)
		return ok && msg.Name == daemonName && msg.Epoch == 42 && msg.State == daemonLaunchStarted
	}) {
		t.Fatal("expected successful launch result")
	}
	actor.SendMessage(gen.PID{}, request)
	actor.ShouldSpawn().Times(2).Assert()
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
			request := messageLaunch{
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
			actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &daemonLaunchWorker{} }, unit.WithNodeName(owner))
			if err != nil {
				t.Fatalf("spawn launch worker: %v", err)
			}
			actor.ClearEvents()
			actor.Process().SetMethodFailure("Spawn", tt.err)

			actor.SendMessage(gen.PID{}, request)
			if !hasLaunchCompletion(actor, func(message any) bool {
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
	actor, err := unit.Spawn(t, Factory(book, daemonTestDecorator), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn daemon with default factory: %v", err)
	}
	result := actor.Call(gen.PID{}, "anything")
	if result.Error != nil || result.Response != nil {
		t.Fatalf("daemon call should return nil nil, got response=%v err=%v", result.Response, result.Error)
	}

	w := actor.Behavior().(*daemon)
	w.sendLaunchResult("", core.MessageDaemonLaunchResult{Name: gen.Atom("ignored")})
	actor.Process().SetMethodFailurePattern("Send", string(self), errors.New("send failed"))
	w.sendLaunchResult(self, core.MessageDaemonLaunchResult{Name: gen.Atom("failed-send")})
}

func TestDaemonHandleEnsureDaemonLaunchInProgressBranches(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	msg := core.MessageEnsureDaemon{Launcher: "l", Process: core.DaemonProcess{ProcessName: "key"}}
	w.handleEnsureDaemon(msg)
	epoch := w.launching["key"].Epoch
	w.handleEnsureDaemon(msg)
	if len(w.launching) != 1 || w.launching["key"].Epoch != epoch {
		t.Fatal("duplicate work")
	}

}

func TestDaemonLaunchResultAndTimeoutIgnoreStaleOrRemoteOwner(t *testing.T) {
	book := core.NewAddressBook()
	self, remote := gen.Atom("node-a@127.0.0.1"), gen.Atom("node-b@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(self, remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.launching["key"] = daemonLaunchState{Epoch: 4, TargetNode: remote}
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "key", Epoch: 3, Node: remote, State: daemonLaunchStarted})
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "key", Epoch: 4, Node: self, State: daemonLaunchStarted})
	w.handleDaemonLaunchTimeout(messageDaemonLaunchTimeout{Name: "key", Epoch: 3})
	if len(w.launching) != 1 || len(w.retries) != 0 {
		t.Fatal("stale completion changed task")
	}
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "key", Epoch: 4, Node: remote, State: daemonLaunchStarted})
	if len(w.launching) != 0 {
		t.Fatal("remote completion ignored")
	}

}

type disconnectedDaemonProcess struct{ gen.Process }

func (p disconnectedDaemonProcess) Send(to any, message any) error {
	if _, ok := to.(gen.ProcessID); ok {
		return gen.ErrNoConnection
	}
	return p.Process.Send(to, message)
}

func TestDaemonRetriesForwardConnectionFailure(t *testing.T) {
	book := core.NewAddressBook()
	self, remote := gen.Atom("node-a@127.0.0.1"), gen.Atom("node-b@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.Process = disconnectedDaemonProcess{Process: w.Process}
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "l", Process: core.DaemonProcess{ProcessName: "key"}})
	if len(w.retries) != 0 {
		t.Fatal("local I/O dispatch failed before the remote send")
	}
	runDaemonIO(t, actor, w)
	if len(w.retries) != 1 || len(w.launching) != 1 {
		t.Fatal("connection failure not retained for retry")
	}

}

func driveRecoveryScan(t *testing.T, actor *unit.TestActor, w *daemon) {
	t.Helper()
	cursor := 0
	deadline := time.Now().Add(time.Second)
	for w.scan != nil && time.Now().Before(deadline) {
		events := actor.Events()
		for cursor < len(events) {
			event := events[cursor]
			cursor++
			if send, ok := event.(unit.SendEvent); ok {
				switch send.Message.(type) {
				case messageScanStep, messageScanPage:
					w.HandleMessage(actor.PID(), send.Message)
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	if w.scan != nil {
		t.Fatal("scan did not finish")
	}
}
func hasLaunchCompletion(actor *unit.TestActor, match func(any) bool) bool {
	for _, event := range actor.Events() {
		if send, ok := event.(unit.SendEvent); ok {
			if finished, ok := send.Message.(messageLaunchFinished); ok && match(finished.result) {
				return true
			}
		}
	}
	return false
}

func TestExitedDaemonRetriesUntilRemoteReleaseCompletes(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	old := gen.PID{Node: self, ID: 100, Creation: 1}
	w.release = func(context.Context, gen.Atom, gen.PID) error { return errors.New("store unavailable") }
	w.handleDaemonExit(core.MessageDaemonExited{Ensure: core.MessageEnsureDaemon{Launcher: "l", Process: core.DaemonProcess{ProcessName: "key"}}, PID: old})
	runDaemonIO(t, actor, w)
	if len(w.retries) != 1 || w.launching["key"].Exited != old {
		t.Fatal("cleanup retry lost exited PID")
	}

}

func TestDaemonScanYieldsAtBatchAndCapacity(t *testing.T) {
	self := gen.Atom("node-a@127.0.0.1")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	w.options.ScanBatchSize = 1
	w.options.MaxInFlight = 1
	w.scan = &recoveryScan{loaded: true, launchers: []core.Launcher{{Name: "missing-launcher"}}, page: []core.DaemonProcess{{ProcessName: "a"}, {ProcessName: "b"}}}
	w.scanStep(w.scan)
	if len(w.scan.page) != 1 || len(w.launching) != 1 {
		t.Fatal("batch did not yield", len(w.scan.page), len(w.launching))
	}
	w.scanStep(w.scan)
	if len(w.scan.page) != 1 || len(w.launching) != 1 {
		t.Fatal("capacity did not pause scan")
	}
}

// Drive the fixed I/O worker explicitly; unit actors do not run spawned pools.
func runDaemonIO(t *testing.T, actor *unit.TestActor, w *daemon) {
	t.Helper()
	worker := &daemonIOWorker{book: w.book, release: w.release, parent: actor.PID()}
	worker.Process = w.Process
	for _, event := range actor.Events() {
		if send, ok := event.(unit.SendEvent); ok {
			if job, ok := send.Message.(messageIO); ok {
				worker.HandleMessage(actor.PID(), job)
			}
		}
	}
	for _, event := range actor.Events() {
		if send, ok := event.(unit.SendEvent); ok {
			if result, ok := send.Message.(messageIOResult); ok {
				w.handleIOResult(result)
			}
		}
	}
}

func TestDaemonRetriesMissingLaunchResult(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "request not delivered"
		if started {
			name = "started but reply not delivered"
		}
		t.Run(name, func(t *testing.T) {
			book := core.NewAddressBook()
			self, remote := gen.Atom("node-a@127.0.0.1"), gen.Atom("node-b@127.0.0.1")
			book.SetAvailableNodes(core.NewNodeList(remote))
			actor := spawnDaemonUnit(t, book, self)
			w := actor.Behavior().(*daemon)
			key := gen.Atom("missing-result")
			if err := w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "launcher", Process: core.DaemonProcess{ProcessName: key}}); err != nil {
				t.Fatal(err)
			}
			runDaemonIO(t, actor, w)
			state, ok := w.launching[key]
			if !ok || state.Phase != daemonLaunchPhaseLaunching || state.Cancel == nil {
				t.Fatal("sent request must wait for a launch result with a timeout")
			}
			if started {
				setDaemonRoute(book, key, gen.PID{Node: remote, ID: 42, Creation: 1})
			}
			w.handleDaemonLaunchTimeout(messageDaemonLaunchTimeout{Name: key, Epoch: state.Epoch})
			if len(w.retries) != 1 {
				t.Fatal("missing result did not schedule retry")
			}
			actor.ClearEvents()
			w.HandleMessage(actor.PID(), messageRetry{Name: key, Epoch: w.launching[key].Epoch})
			runDaemonIO(t, actor, w)
			if started {
				if len(w.launching) != 0 || len(w.retries) != 0 {
					t.Fatal("existing route did not complete the task")
				}
				return
			}
			if !hasDaemonSend(actor, gen.ProcessID{Name: ProcessName, Node: remote}, func(message any) bool {
				msg, ok := message.(core.MessageLaunchOneDaemon)
				return ok && msg.Process.ProcessName == key && msg.Epoch != state.Epoch
			}) {
				t.Fatal("missing request was not sent again with a new epoch")
			}
			if w.launching[key].Phase != daemonLaunchPhaseLaunching {
				t.Fatal("retry must wait for its launch result")
			}
		})
	}
}
