package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

func awaitRouteCondition(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("route condition did not converge")
		}
	}
}

type persistentLaunchActor struct {
	act.Actor
	inits *atomic.Int64
}

func (a *persistentLaunchActor) Init(...any) error                             { a.inits.Add(1); return nil }
func (a *persistentLaunchActor) HandleCall(gen.PID, gen.Ref, any) (any, error) { return "alive", nil }

type concurrentLaunchActor struct {
	act.Actor
	active  *atomic.Int64
	maximum *atomic.Int64
	entered chan gen.PID
	gate    <-chan struct{}
}

func (a *concurrentLaunchActor) Init(...any) error {
	active := a.active.Add(1)
	defer a.active.Add(-1)
	for {
		previous := a.maximum.Load()
		if active <= previous || a.maximum.CompareAndSwap(previous, active) {
			break
		}
	}
	a.entered <- a.Parent()
	<-a.gate
	return nil
}

func TestRoutedChildRestartsAfterBusinessCleanup(t *testing.T) {
	store := newTestRoutePersistence(t)
	defer store.Close()
	n, e := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "supervised-route@localhost", Port: 11922, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled})
	if e != nil {
		t.Fatal(e)
	}
	entered := make(chan struct{})
	finish := make(chan struct{})
	var once sync.Once
	defer func() { once.Do(func() { close(finish) }); n.Stop() }()
	sup, e := n.Spawn(func() gen.ProcessBehavior {
		return &restartingRouteSupervisor{routes: n.ActorRoutes(), entered: entered, finish: finish}
	}, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	old, e := n.ProcessPID("restart-child")
	if e != nil {
		t.Fatal(e)
	}
	n.Send(old, "restart")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("business cleanup did not start")
	}
	pid, found, e := routePID(store, "restart-child")
	if e != nil || !found || pid != old {
		t.Fatal("route released before business cleanup", pid, found, e)
	}
	once.Do(func() { close(finish) })
	awaitRouteCondition(t, func() bool {
		pid, found, e := n.ActorRoutes().Locate(context.Background(), "restart-child")
		return e == nil && found && pid != old
	})
	if _, e := n.ProcessState(sup); e != nil {
		t.Fatal("supervisor stopped", e)
	}
}

func TestDaemonRouteExpirationCreatesFreshInstance(t *testing.T) {
	s := newTestRoutePersistence(t)
	var inits atomic.Int64
	name := gen.Atom("ttl-daemon")
	system.RegisterLauncher("ttl-launcher", system.Launcher{Factory: func() gen.ProcessBehavior { return &persistentLaunchActor{inits: &inits} }, Option: gen.ProcessOptions{LinkParent: true}, RecoveryScanner: system.SingletonDaemon(name, nil)})
	defer system.UnregisterLauncher("ttl-launcher")
	n, e := StartSimpleNode(SimpleNodeOptions{NodeName: "route-ttl@localhost", Port: 11923, Registrar: mem.Create(), ActorRoutePersistence: s, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, ActorRouterOptions: system.ActorRouterOptions{RouteTTL: 500 * time.Millisecond, LeaseSafetyMargin: 20 * time.Millisecond}, DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, LeaderRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: time.Millisecond, RetryMaxDelay: 20 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if e != nil {
		t.Fatal(e)
	}
	defer n.Stop()
	var old gen.PID
	awaitRouteCondition(t, func() bool {
		var found bool
		old, found, _ = n.ActorRoutes().Locate(context.Background(), name)
		return found && inits.Load() == 1
	})
	info, e := n.ProcessInfo(old)
	if e != nil {
		t.Fatal(e)
	}
	var next gen.PID
	awaitRouteCondition(t, func() bool {
		var found bool
		next, found, _ = n.ActorRoutes().Locate(context.Background(), name)
		return found && next != old && inits.Load() >= 2
	})
	if _, e := n.ProcessState(old); !errors.Is(e, gen.ErrProcessUnknown) {
		t.Fatal("expired actor remains", e)
	}
	if _, e := n.ProcessState(info.Parent); e != nil {
		t.Fatal("launch parent stopped", e)
	}
	if st := n.ActorRoutes().Stats(); st.LeaseLosses != 0 {
		t.Fatal(st)
	}
}

func TestDaemonLaunchConcurrencyUsesPersistentWorkers(t *testing.T) {
	for _, timeout := range []int{0, 1} {
		t.Run(fmt.Sprintf("init-timeout-%d", timeout), func(t *testing.T) { testDaemonLaunchConcurrency(t, timeout) })
	}
}

func testDaemonLaunchConcurrency(t *testing.T, initTimeout int) {
	const workers = 8
	const count = workers * 2
	var active, maximum atomic.Int64
	entered := make(chan gen.PID, count)
	gate := make(chan struct{})
	var release sync.Once
	processes := make([]system.DaemonProcess, count)
	for i := range processes {
		processes[i].ProcessName = gen.Atom(fmt.Sprintf("bounded-daemon-%d", i))
	}
	if err := system.RegisterLauncher("bounded-launcher", system.Launcher{
		Factory: func() gen.ProcessBehavior {
			return &concurrentLaunchActor{active: &active, maximum: &maximum, entered: entered, gate: gate}
		},
		Option: gen.ProcessOptions{LinkParent: true, InitTimeout: initTimeout},
		RecoveryScanner: func() system.DaemonIterator {
			return func() ([]system.DaemonProcess, bool, error) { return processes, false, nil }
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer system.UnregisterLauncher("bounded-launcher")
	node, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "bounded-launch@localhost", Port: 11913, ActorRoutePersistence: newTestRoutePersistence(t), NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled,
		DaemonOptions: system.DaemonOptions{LeaderRecoveryDelay: time.Millisecond, InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, LaunchTimeout: 5 * time.Second, RetryMaxDelay: 100 * time.Millisecond, RecoveryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { release.Do(func() { close(gate) }); node.Stop() }()
	parents := make(map[gen.PID]bool)
	for i := 0; i < workers; i++ {
		select {
		case parent := <-entered:
			parents[parent] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("launch workers did not start: entered=%d active=%d routes=%+v", i, active.Load(), node.ActorRoutes().Stats())
		}
	}
	if initTimeout > 0 {
		select {
		case <-entered:
			t.Fatal("another Init started while eight timed-out callbacks still ran")
		case <-time.After(1500 * time.Millisecond):
		}
	}
	release.Do(func() { close(gate) })
	for i := workers; i < count; i++ {
		select {
		case parent := <-entered:
			parents[parent] = true
		case <-time.After(5 * time.Second):
			t.Fatal("queued launches did not start")
		}
	}
	awaitRouteCondition(t, func() bool {
		for _, process := range processes {
			if _, found, _ := node.ActorRoutes().Locate(context.Background(), process.ProcessName); !found {
				return false
			}
		}
		return true
	})
	if got := maximum.Load(); got != workers {
		t.Fatalf("concurrent launches: got %d want %d", got, workers)
	}
	if len(parents) != workers {
		t.Fatalf("launch parents: got %d want %d", len(parents), workers)
	}
	for parent := range parents {
		if _, err := node.ProcessState(parent); err != nil {
			t.Fatalf("launch parent stopped: %v", err)
		}
	}
}

func TestDaemonReplacesLeaseFromPriorNodeIncarnation(t *testing.T) {
	const name = gen.Atom("stale-daemon")
	const nodeName = gen.Atom("stale-daemon@localhost")
	store := newTestRoutePersistence(t)
	old := gen.PID{Node: nodeName, ID: 99999, Creation: 1}
	session, _ := store.OpenSession(context.Background(), old.Node, 100*time.Millisecond)
	if _, err := store.AcquireRoute(context.Background(), session.SessionID, name, old, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	var inits atomic.Int64
	if err := system.RegisterLauncher("stale-launcher", system.Launcher{
		Factory:         func() gen.ProcessBehavior { return &persistentLaunchActor{inits: &inits} },
		RecoveryScanner: system.SingletonDaemon(name, nil),
	}); err != nil {
		t.Fatal(err)
	}
	defer system.UnregisterLauncher("stale-launcher")
	node, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: string(nodeName), Port: 11914,
		ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled,
		DaemonOptions: system.DaemonOptions{LeaderRecoveryDelay: time.Millisecond, InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RecoveryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	awaitRouteCondition(t, func() bool {
		pid, err := node.ProcessPID(name)
		routed, found, lookupErr := node.ActorRoutes().Locate(context.Background(), name)
		return err == nil && lookupErr == nil && found && routed == pid && routed != old && inits.Load() == 1
	})
	if inits.Load() != 1 {
		t.Fatalf("Init count: %d", inits.Load())
	}
}

type notifiedDaemon struct {
	act.Actor
	started chan gen.PID
}

func (a *notifiedDaemon) Init(...any) error { a.started <- a.PID(); return nil }
func (a *notifiedDaemon) HandleMessage(_ gen.PID, msg any) error {
	if msg == "exit" {
		return gen.TerminateReasonNormal
	}
	return nil
}
func TestDaemonExitRecoversWithoutFullScan(t *testing.T) {
	started := make(chan gen.PID, 8)
	const launcher = gen.Atom("exit-recovery-launcher")
	const key = gen.Atom("exit-recovery-daemon")
	if err := system.RegisterLauncher(launcher, system.Launcher{Factory: func() gen.ProcessBehavior { return &notifiedDaemon{started: started} }, RecoveryScanner: system.SingletonDaemon(key, nil)}); err != nil {
		t.Fatal(err)
	}
	defer system.UnregisterLauncher(launcher)
	node, err := StartSimpleNode(SimpleNodeOptions{NodeName: "daemon-exit-recovery@localhost", Registrar: mem.Create(), ActorRoutePersistence: newTestRoutePersistence(t), NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, DaemonOptions: system.DaemonOptions{LeaderRecoveryDelay: time.Millisecond, InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: 10 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	var first gen.PID
	select {
	case first = <-started:
	case <-time.After(4 * time.Second):
		t.Fatal("daemon did not start")
	}
	if err := node.Send(first, "exit"); err != nil {
		t.Fatal(err)
	}
	select {
	case next := <-started:
		if next == first {
			t.Fatal("daemon PID did not change")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon exit waited for full scan")
	}
}

type desiredResourceActor struct {
	act.Actor
	desired *atomic.Bool
	inits   *atomic.Int64
}

func (a *desiredResourceActor) Init(...any) error {
	a.inits.Add(1)
	if !a.desired.Load() {
		return gen.TerminateReasonNormal
	}
	return nil
}
func (a *desiredResourceActor) HandleMessage(gen.PID, any) error { return gen.TerminateReasonNormal }

func TestDaemonNormalInitRefusalEndsRecovery(t *testing.T) {
	store := newTestRoutePersistence(t)
	var desired atomic.Bool
	desired.Store(true)
	var inits atomic.Int64
	system.RegisterLauncher("desired-resource-launcher", system.Launcher{Factory: func() gen.ProcessBehavior { return &desiredResourceActor{desired: &desired, inits: &inits} }, RecoveryScanner: func() system.DaemonIterator {
		return func() ([]system.DaemonProcess, bool, error) {
			if desired.Load() {
				return []system.DaemonProcess{{ProcessName: "desired-resource"}}, false, nil
			}
			return nil, false, nil
		}
	}})
	defer system.UnregisterLauncher("desired-resource-launcher")
	n, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "desired-resource@localhost", Port: 11920, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, DaemonOptions: system.DaemonOptions{LeaderRecoveryDelay: time.Millisecond, InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	var original gen.PID
	awaitRouteCondition(t, func() bool {
		original, _, _ = routePID(store, "desired-resource")
		return inits.Load() == 1 && original != (gen.PID{})
	})
	desired.Store(false)
	n.Send(original, "resource deleted")
	awaitRouteCondition(t, func() bool { return inits.Load() == 2 })
	time.Sleep(150 * time.Millisecond)
	if got := inits.Load(); got != 2 {
		t.Fatalf("normal refusal kept retrying: %d", got)
	}
}

type failedExitReleaseStore struct {
	*testRoutePersistence
	old      gen.PID
	fail     atomic.Bool
	failures atomic.Int64
}

func (s *failedExitReleaseStore) ReleaseRoute(ctx context.Context, id system.SessionID, key gen.Atom, pid gen.PID) error {
	if s.fail.Load() && pid == s.old {
		s.failures.Add(1)
		return errors.New("release temporarily unavailable")
	}
	return s.testRoutePersistence.ReleaseRoute(ctx, id, key, pid)
}
func TestDaemonRetriesExactExitCleanupWithHourLease(t *testing.T) {
	store := &failedExitReleaseStore{testRoutePersistence: newTestRoutePersistence(t)}
	var inits atomic.Int64
	system.RegisterLauncher("release-retry-launcher", system.Launcher{Factory: func() gen.ProcessBehavior { return &persistentLaunchActor{inits: &inits} }, RecoveryScanner: system.SingletonDaemon("release-retry", nil)})
	defer system.UnregisterLauncher("release-retry-launcher")
	n, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "release-retry@localhost", Port: 11921, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, ActorRouterOptions: system.ActorRouterOptions{RouteTTL: time.Hour}, DaemonOptions: system.DaemonOptions{LeaderRecoveryDelay: time.Millisecond, InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: 10 * time.Millisecond, RetryMaxDelay: 20 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	var original gen.PID
	awaitRouteCondition(t, func() bool {
		original, _, _ = routePID(store, "release-retry")
		return inits.Load() == 1 && original != (gen.PID{})
	})
	store.old = original
	store.fail.Store(true)
	n.Kill(original)
	awaitRouteCondition(t, func() bool { return store.failures.Load() >= 2 })
	store.fail.Store(false)
	awaitRouteCondition(t, func() bool {
		pid, found, _ := routePID(store, "release-retry")
		return found && pid != original && inits.Load() == 2
	})
}

type restartingRouteActor struct {
	act.Actor
	entered chan struct{}
	finish  <-chan struct{}
}

func (*restartingRouteActor) HandleMessage(gen.PID, any) error {
	return errors.New("restart requested")
}
func (a *restartingRouteActor) Terminate(error) {
	if a.entered != nil {
		close(a.entered)
		<-a.finish
	}
}

type restartingRouteSupervisor struct {
	act.Supervisor
	routes  ActorRoutes
	entered chan struct{}
	finish  <-chan struct{}
	spawns  int
}

func (s *restartingRouteSupervisor) Init(...any) (act.SupervisorSpec, error) {
	return act.SupervisorSpec{Type: act.SupervisorTypeOneForOne, Restart: act.SupervisorRestart{Strategy: act.SupervisorStrategyPermanent, Intensity: 2, Period: 5}, Children: []act.SupervisorChildSpec{{Name: "restart-child", Factory: func() gen.ProcessBehavior {
		s.spawns++
		a := &restartingRouteActor{}
		if s.spawns == 1 {
			a.entered = s.entered
			a.finish = s.finish
		}
		return s.routes.WithActorRoute("restart-child", a)
	}}}}, nil
}
