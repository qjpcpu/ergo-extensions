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

type queuedReleaseStore struct {
	*testRoutePersistence
	entered     chan struct{}
	unblock     chan struct{}
	oldReleased chan struct{}
	oldPID      gen.PID
}

func (s *queuedReleaseStore) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if key == "occupy-release-worker" {
		close(s.entered)
		select {
		case <-s.unblock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := s.testRoutePersistence.Release(ctx, key, pid)
	if key == "restart-child" && pid == s.oldPID {
		select {
		case s.oldReleased <- struct{}{}:
		default:
		}
	}
	return err
}

type restartingRouteActor struct{ act.Actor }

func (*restartingRouteActor) HandleMessage(gen.PID, any) error {
	return errors.New("restart requested")
}

type restartingRouteSupervisor struct {
	act.Supervisor
	routes ActorRoutes
}

func (s *restartingRouteSupervisor) Init(...any) (act.SupervisorSpec, error) {
	return act.SupervisorSpec{
		Type:    act.SupervisorTypeOneForOne,
		Restart: act.SupervisorRestart{Strategy: act.SupervisorStrategyPermanent, Intensity: 2, Period: 5},
		Children: []act.SupervisorChildSpec{{Name: "restart-child", Factory: func() gen.ProcessBehavior {
			return s.routes.WithActorRoute("restart-child", &restartingRouteActor{})
		}}},
	}, nil
}

func TestRoutedChildRestartsWhileReleaseQueueIsBlocked(t *testing.T) {
	store := &queuedReleaseStore{testRoutePersistence: newTestRoutePersistence(), entered: make(chan struct{}), unblock: make(chan struct{}), oldReleased: make(chan struct{}, 2)}
	node, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "route-restart@localhost", Port: 11911, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled,
		ActorRouterOptions: system.ActorRouterOptions{RenewWorkers: 1, OperationTimeout: 10 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	defer func() { release.Do(func() { close(store.unblock) }); node.Stop() }()
	blocker, err := node.Spawn(func() gen.ProcessBehavior {
		return node.ActorRoutes().WithActorRoute("occupy-release-worker", &restartingRouteActor{})
	}, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Send(blocker, "exit"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("release worker was not occupied")
	}
	sup, err := node.Spawn(func() gen.ProcessBehavior { return &restartingRouteSupervisor{routes: node.ActorRoutes()} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	old, err := node.ProcessPID("restart-child")
	if err != nil {
		t.Fatal(err)
	}
	store.oldPID = old
	if err := node.Send(old, "restart"); err != nil {
		t.Fatal(err)
	}
	var current gen.PID
	awaitRouteCondition(t, func() bool {
		current, err = node.ProcessPID("restart-child")
		if err != nil || current == old {
			return false
		}
		routed, found, _ := node.ActorRoutes().Locate(context.Background(), "restart-child")
		return found && routed == current
	})
	if _, err := node.ProcessState(sup); err != nil {
		t.Fatalf("supervisor stopped: %v", err)
	}
	release.Do(func() { close(store.unblock) })
	// The queued release must preserve the replacement owner.
	for i := 0; i < 1; i++ {
		select {
		case <-store.oldReleased:
		case <-time.After(time.Second):
			t.Fatal("old release did not finish")
		}
	}
	routed, found, err := node.ActorRoutes().Locate(context.Background(), "restart-child")
	if err != nil || !found || routed != current {
		t.Fatalf("new route after old release: %v %v %v", routed, found, err)
	}
}

type recoverableRouteStore struct {
	*testRoutePersistence
	lose   atomic.Bool
	lost   chan struct{}
	renews atomic.Int64
}

func (s *recoverableRouteStore) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if key == "recover-daemon" && s.lose.CompareAndSwap(true, false) {
		s.mu.Lock()
		delete(s.routes, key)
		s.mu.Unlock()
		close(s.lost)
		return false, nil
	}
	owned, err := s.testRoutePersistence.Renew(ctx, key, pid, ttl)
	if key == "recover-daemon" && owned {
		s.renews.Add(1)
	}
	return owned, err
}

type persistentLaunchActor struct {
	act.Actor
	inits *atomic.Int64
}

func (a *persistentLaunchActor) Init(...any) error                             { a.inits.Add(1); return nil }
func (a *persistentLaunchActor) HandleCall(gen.PID, gen.Ref, any) (any, error) { return "alive", nil }

func TestDaemonReplacesLostLeaseAndKeepsLinkedParent(t *testing.T) {
	store := &recoverableRouteStore{testRoutePersistence: newTestRoutePersistence(), lost: make(chan struct{})}
	var inits atomic.Int64
	if err := system.RegisterLauncher("recover-launcher", system.Launcher{
		Factory:         func() gen.ProcessBehavior { return &persistentLaunchActor{inits: &inits} },
		Option:          gen.ProcessOptions{LinkParent: true},
		RecoveryScanner: system.SingletonDaemon("recover-daemon", nil),
	}); err != nil {
		t.Fatal(err)
	}
	defer system.UnregisterLauncher("recover-launcher")
	node, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "route-recover@localhost", Port: 11912, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled,
		ActorRouterOptions: system.ActorRouterOptions{LeaseTTL: time.Second, RenewInterval: 20 * time.Millisecond},
		DaemonOptions:      system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: 20 * time.Millisecond, RunningGrace: time.Millisecond, LaunchTimeout: 20 * time.Millisecond, RetryInitialDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	var original gen.PID
	awaitRouteCondition(t, func() bool {
		var found bool
		original, found, _ = node.ActorRoutes().Locate(context.Background(), "recover-daemon")
		return found
	})
	info, err := node.ProcessInfo(original)
	if err != nil {
		t.Fatal(err)
	}
	store.lose.Store(true)
	select {
	case <-store.lost:
	case <-time.After(time.Second):
		t.Fatal("route loss was not injected")
	}
	before := store.renews.Load()
	var replacement gen.PID
	awaitRouteCondition(t, func() bool {
		pid, found, _ := node.ActorRoutes().Locate(context.Background(), "recover-daemon")
		replacement = pid
		return found && pid != original && store.renews.Load() > before
	})
	if inits.Load() != 2 {
		t.Fatalf("daemon initialized %d times", inits.Load())
	}
	if _, err := node.ProcessState(original); !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("old actor still exists: %v", err)
	}
	if _, err := node.ProcessState(info.Parent); err != nil {
		t.Fatalf("launch parent exited: %v", err)
	}
	if response, err := node.CallPID(replacement, "ping", 1); err != nil || response != "alive" {
		t.Fatalf("daemon call: %v %v", response, err)
	}
}

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
	node, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "bounded-launch@localhost", Port: 11913, ActorRoutePersistence: newTestRoutePersistence(), NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled,
		DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, LaunchTimeout: 5 * time.Second, RetryMaxDelay: 100 * time.Millisecond, RecoveryJitterMax: -1}})
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
			t.Fatal("launch workers did not start")
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
	store := newTestRoutePersistence()
	old := gen.PID{Node: nodeName, ID: 99999, Creation: 1}
	if _, err := store.Acquire(context.Background(), name, old, time.Minute); err != nil {
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
		DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RecoveryJitterMax: -1}})
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
	node, err := StartSimpleNode(SimpleNodeOptions{NodeName: "daemon-exit-recovery@localhost", Registrar: mem.Create(), ActorRoutePersistence: newTestRoutePersistence(), NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: 10 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
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
	store := newTestRoutePersistence()
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
	n, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "desired-resource@localhost", Port: 11920, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: time.Millisecond, RetryMaxDelay: 5 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	var original gen.PID
	awaitRouteCondition(t, func() bool {
		original, _, _ = store.Lookup(context.Background(), "desired-resource")
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

func (s *failedExitReleaseStore) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if s.fail.Load() && pid == s.old {
		s.failures.Add(1)
		return errors.New("release temporarily unavailable")
	}
	return s.testRoutePersistence.Release(ctx, key, pid)
}
func TestDaemonRetriesExactExitCleanupWithHourLease(t *testing.T) {
	store := &failedExitReleaseStore{testRoutePersistence: newTestRoutePersistence()}
	var inits atomic.Int64
	system.RegisterLauncher("release-retry-launcher", system.Launcher{Factory: func() gen.ProcessBehavior { return &persistentLaunchActor{inits: &inits} }, RecoveryScanner: system.SingletonDaemon("release-retry", nil)})
	defer system.UnregisterLauncher("release-retry-launcher")
	n, err := StartSimpleNode(SimpleNodeOptions{Registrar: mem.Create(), NodeName: "release-retry@localhost", Port: 11921, ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled, ActorRouterOptions: system.ActorRouterOptions{LeaseTTL: time.Hour, RenewInterval: time.Minute}, DaemonOptions: system.DaemonOptions{InitialRecoveryDelay: time.Millisecond, FullRecoveryInterval: time.Hour, RetryInitialDelay: 10 * time.Millisecond, RetryMaxDelay: 20 * time.Millisecond, RecoveryJitterMax: -1, RetryJitterMax: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	var original gen.PID
	awaitRouteCondition(t, func() bool {
		original, _, _ = store.Lookup(context.Background(), "release-retry")
		return inits.Load() == 1 && original != (gen.PID{})
	})
	store.old = original
	store.fail.Store(true)
	n.Kill(original)
	awaitRouteCondition(t, func() bool { return store.failures.Load() >= 2 })
	store.fail.Store(false)
	awaitRouteCondition(t, func() bool {
		pid, found, _ := store.Lookup(context.Background(), "release-retry")
		return found && pid != original && inits.Load() == 2
	})
}
