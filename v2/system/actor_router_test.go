package system

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type memoryActorRoute struct {
	pid       gen.PID
	expiresAt time.Time
}

type memoryActorRoutePersistence struct {
	mu         sync.Mutex
	routes     map[gen.Atom]memoryActorRoute
	renews     int
	onAcquire  func()
	acquireErr error
	renewErr   error
	releaseErr error
	lookupErr  error
}

func newMemoryActorRoutePersistence() *memoryActorRoutePersistence {
	return &memoryActorRoutePersistence{routes: make(map[gen.Atom]memoryActorRoute)}
}

func (m *memoryActorRoutePersistence) Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.acquireErr != nil {
		return false, m.acquireErr
	}
	if m.onAcquire != nil {
		m.onAcquire()
	}
	now := time.Now()
	current, ok := m.routes[key]
	if ok && now.Before(current.expiresAt) && current.pid != pid {
		return false, nil
	}
	m.routes[key] = memoryActorRoute{pid: pid, expiresAt: now.Add(ttl)}
	return true, nil
}

type initErrorActor struct {
	act.Actor
	err error
}

func (a *initErrorActor) Init(...any) error { return a.err }

func (m *memoryActorRoutePersistence) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.renews++
	if m.renewErr != nil {
		return false, m.renewErr
	}
	now := time.Now()
	current, ok := m.routes[key]
	if !ok || !now.Before(current.expiresAt) || current.pid != pid {
		return false, nil
	}
	current.expiresAt = now.Add(ttl)
	m.routes[key] = current
	return true, nil
}

func (m *memoryActorRoutePersistence) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releaseErr != nil {
		return m.releaseErr
	}
	if current, ok := m.routes[key]; ok && current.pid == pid {
		delete(m.routes, key)
	}
	return nil
}

func (m *memoryActorRoutePersistence) Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if err := ctx.Err(); err != nil {
		return gen.PID{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lookupErr != nil {
		return gen.PID{}, false, m.lookupErr
	}
	current, ok := m.routes[key]
	if !ok || !time.Now().Before(current.expiresAt) {
		return gen.PID{}, false, nil
	}
	return current.pid, true, nil
}

type routerTestActor struct {
	act.Actor
	initialized bool
	terminated  bool
	messages    int
	behaviorOK  bool
}

func (a *routerTestActor) Init(args ...any) error {
	a.initialized = true
	a.behaviorOK = a.Behavior() == a
	return nil
}

type routerTestSupervisor struct {
	act.Supervisor
	initialized bool
	behaviorOK  bool
}

func (s *routerTestSupervisor) Init(args ...any) (act.SupervisorSpec, error) {
	s.initialized = true
	s.behaviorOK = s.Behavior() == s
	return act.SupervisorSpec{
		Type: act.SupervisorTypeOneForOne,
		Children: []act.SupervisorChildSpec{{
			Name: "worker",
			Factory: func() gen.ProcessBehavior {
				return &routerTestActor{}
			},
		}},
	}, nil
}

type routerTestPool struct {
	act.Pool
	initialized bool
	behaviorOK  bool
}

func (p *routerTestPool) Init(args ...any) (act.PoolOptions, error) {
	p.initialized = true
	p.behaviorOK = p.Behavior() == p
	return act.PoolOptions{
		PoolSize: 1,
		WorkerFactory: func() gen.ProcessBehavior {
			return &routerTestActor{}
		},
	}, nil
}

func (a *routerTestActor) HandleMessage(from gen.PID, message any) error {
	a.messages++
	return nil
}

func (a *routerTestActor) Terminate(reason error) {
	a.terminated = true
}

func TestActorRouterOptionsAndValidation(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	if router.options != DefaultActorRouterOptions() {
		t.Fatalf("unexpected defaults: %+v", router.options)
	}
	if _, err := NewActorRouter(nil, ActorRouterOptions{}); !errors.Is(err, ErrActorRoutePersistenceNil) {
		t.Fatalf("expected nil persistence error, got %v", err)
	}
	if _, err := NewActorRouter(store, ActorRouterOptions{LeaseTTL: time.Second, RenewInterval: time.Second}); err == nil {
		t.Fatal("expected invalid lease timing")
	}
	if _, err := NewActorRouter(store, ActorRouterOptions{LeaseTTL: -1}); err == nil {
		t.Fatal("expected negative duration error")
	}
	if _, _, err := router.lookup(context.Background(), "key"); !errors.Is(err, ErrActorRouterUnbound) {
		t.Fatalf("expected unbound error, got %v", err)
	}
	if _, _, err := router.lookup(context.Background(), ""); !errors.Is(err, ErrActorRouteKeyEmpty) {
		t.Fatalf("expected empty key error, got %v", err)
	}
}

func TestActorRouterWithActorRoutePreservesBehavior(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := &routerTestActor{}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return router.WithActorRoute("business/key", original)
	})
	if err != nil {
		t.Fatalf("spawn routed actor: %v", err)
	}
	if !original.initialized || !original.behaviorOK || original.PID() != actor.PID() {
		t.Fatalf("original behavior was not initialized correctly: initialized=%v behavior=%v pid=%s", original.initialized, original.behaviorOK, original.PID())
	}
	actor.SendMessage(gen.PID{}, "message")
	if original.messages != 1 {
		t.Fatalf("expected forwarded message, got %d", original.messages)
	}
	actor.Behavior().(*routedActorBehavior).ProcessTerminate(gen.TerminateReasonNormal)
	if !original.terminated {
		t.Fatal("expected original terminate callback")
	}
}

func TestActorRouterTypedRoutesPreserveSupervisorAndPoolBehavior(t *testing.T) {
	spawn := func(t *testing.T, factory gen.ProcessFactory) {
		t.Helper()
		if _, err := unit.Spawn(t, factory); err != nil {
			t.Fatalf("spawn routed behavior: %v", err)
		}
	}

	t.Run("supervisor", func(t *testing.T) {
		router, err := NewActorRouter(newMemoryActorRoutePersistence(), ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		supervisor := &routerTestSupervisor{}
		spawn(t, func() gen.ProcessBehavior { return router.WithSupervisorRoute("supervisor", supervisor) })
		if !supervisor.initialized || !supervisor.behaviorOK {
			t.Fatalf("supervisor behavior was hidden: initialized=%v behavior=%v", supervisor.initialized, supervisor.behaviorOK)
		}
	})

	t.Run("pool", func(t *testing.T) {
		router, err := NewActorRouter(newMemoryActorRoutePersistence(), ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pool := &routerTestPool{}
		spawn(t, func() gen.ProcessBehavior { return router.WithPoolRoute("pool", pool) })
		if !pool.initialized || !pool.behaviorOK {
			t.Fatalf("pool behavior was hidden: initialized=%v behavior=%v", pool.initialized, pool.behaviorOK)
		}
	})
}

func TestActorRouterTypedRoutesRejectInvalidBehavior(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("", &routerTestActor{}) }); err == nil || !strings.Contains(err.Error(), ErrActorRouteKeyEmpty.Error()) {
		t.Fatalf("expected empty key error, got %v", err)
	}
	if _, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("key", nil) }); err == nil || !strings.Contains(err.Error(), ErrActorRouteBehaviorNil.Error()) {
		t.Fatalf("expected nil behavior error, got %v", err)
	}
	var typedNil *routerTestActor
	if _, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("key", typedNil) }); err == nil || !strings.Contains(err.Error(), ErrActorRouteBehaviorNil.Error()) {
		t.Fatalf("expected typed nil behavior error, got %v", err)
	}
	if _, err := unit.Spawn(t, router.routeFactory("key", func() gen.ProcessBehavior { return routeErrorBehavior{err: errors.New("custom")} })); err == nil || !strings.Contains(err.Error(), ErrActorRouteBehaviorMismatch.Error()) {
		t.Fatalf("expected unsupported behavior error, got %v", err)
	}
}

func TestActorRouteInitializationFailures(t *testing.T) {
	t.Run("behavior init", func(t *testing.T) {
		router, err := NewActorRouter(newMemoryActorRoutePersistence(), ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		want := errors.New("init failed")
		if _, err := unit.Spawn(t, func() gen.ProcessBehavior {
			return router.WithActorRoute("key", &initErrorActor{err: want})
		}); err == nil || !strings.Contains(err.Error(), want.Error()) {
			t.Fatalf("expected behavior init failure, got %v", err)
		}
	})

	t.Run("persistence error", func(t *testing.T) {
		store := newMemoryActorRoutePersistence()
		store.acquireErr = errors.New("storage unavailable")
		router, err := NewActorRouter(store, ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unit.Spawn(t, func() gen.ProcessBehavior {
			return router.WithActorRoute("key", &routerTestActor{})
		}); err == nil || !strings.Contains(err.Error(), store.acquireErr.Error()) {
			t.Fatalf("expected persistence failure, got %v", err)
		}
	})

	t.Run("route taken", func(t *testing.T) {
		store := newMemoryActorRoutePersistence()
		store.routes["key"] = memoryActorRoute{
			pid:       gen.PID{Node: "other@localhost", ID: 1, Creation: 1},
			expiresAt: time.Now().Add(time.Minute),
		}
		router, err := NewActorRouter(store, ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		base, err := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
		if err != nil {
			t.Fatal(err)
		}
		if err := router.Bind(base.Node()); err != nil {
			t.Fatal(err)
		}
		registrar, _ := base.Node().Network().Registrar()
		registrar.(*unit.TestRegistrar).AddNode("other@localhost", nil)
		defer router.Close()
		if _, err := unit.Spawn(t, func() gen.ProcessBehavior {
			return router.WithActorRoute("key", &routerTestActor{})
		}); err == nil || !strings.Contains(err.Error(), ErrActorRouteTaken.Error()) {
			t.Fatalf("expected route conflict, got %v", err)
		}
	})

	t.Run("router shutdown rolls back acquisition", func(t *testing.T) {
		store := newMemoryActorRoutePersistence()
		router, err := NewActorRouter(store, ActorRouterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		store.onAcquire = router.Close
		if _, err := unit.Spawn(t, func() gen.ProcessBehavior {
			return router.WithActorRoute("key", &routerTestActor{})
		}); err == nil || !strings.Contains(err.Error(), ErrActorRouterClosed.Error()) {
			t.Fatalf("expected closed router failure, got %v", err)
		}
		if _, found, err := store.Lookup(context.Background(), "key"); err != nil || found {
			t.Fatalf("failed initialization leaked a route: found=%v err=%v", found, err)
		}
	})

	t.Run("nil router", func(t *testing.T) {
		behavior := &routerTestActor{}
		routed := &routedActorBehavior{
			IActor: behavior,
			route:  newRouteLifecycle(nil, "key", behavior),
		}
		if _, err := unit.Spawn(t, func() gen.ProcessBehavior { return routed }); err == nil || !strings.Contains(err.Error(), ErrActorRoutePersistenceNil.Error()) {
			t.Fatalf("expected nil router failure, got %v", err)
		}
	})
}

func TestRouteManagerAcquireRenewRelease(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{
		LeaseTTL:         time.Second,
		RenewInterval:    20 * time.Millisecond,
		OperationTimeout: time.Second,
		RenewWorkers:     1,
		RenewQueueSize:   8,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return router.WithActorRoute("key", &routerTestActor{})
	})
	if err != nil {
		t.Fatalf("spawn routed actor: %v", err)
	}
	if pid, found, err := store.Lookup(context.Background(), "key"); err != nil || !found || pid != actor.PID() {
		t.Fatalf("route was not acquired: pid=%s found=%v err=%v", pid, found, err)
	}
	deadline := time.Now().Add(time.Second)
	renewed := false
	for time.Now().Before(deadline) {
		store.mu.Lock()
		renews := store.renews
		store.mu.Unlock()
		if renews > 0 {
			renewed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !renewed {
		t.Fatal("route was not renewed")
	}
	actor.Behavior().(*routedActorBehavior).ProcessTerminate(gen.TerminateReasonNormal)
	for time.Now().Before(deadline.Add(time.Second)) {
		_, found, err := store.Lookup(context.Background(), "key")
		if err == nil && !found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("route was not released")
}

func TestRouteManagerDoesNotOverwriteOrDeleteNewOwner(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{
		LeaseTTL:         time.Minute,
		RenewInterval:    time.Second,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return router.WithActorRoute("key", &routerTestActor{})
	})
	if err != nil {
		t.Fatalf("spawn old owner: %v", err)
	}
	oldPID := actor.PID()
	newPID := gen.PID{Node: "node@localhost", ID: 2, Creation: 1}
	store.mu.Lock()
	store.routes["key"] = memoryActorRoute{pid: newPID, expiresAt: time.Now().Add(time.Minute)}
	store.mu.Unlock()
	router.manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "key", pid: oldPID})
	actor.Behavior().(*routedActorBehavior).ProcessTerminate(gen.TerminateReasonNormal)
	if pid, found, err := store.Lookup(context.Background(), "key"); err != nil || !found || pid != newPID {
		t.Fatalf("new owner was modified: pid=%s found=%v err=%v", pid, found, err)
	}
}

func TestRouteManagerRetriesPersistenceErrorAndLimitsLogs(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	store.renewErr = errors.New("renew unavailable")
	router, err := NewActorRouter(store, ActorRouterOptions{
		LeaseTTL:         time.Minute,
		RenewInterval:    time.Second,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return router.WithActorRoute("key", &routerTestActor{})
	})
	if err != nil {
		t.Fatal(err)
	}
	router.manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "key", pid: actor.PID()})
	shard := router.manager.shard("key")
	shard.mu.Lock()
	lease, found := shard.entries["key"]
	pending := found && lease.pending
	shard.mu.Unlock()
	if !found || pending {
		t.Fatalf("renew error removed or wedged lease: found=%v pending=%v", found, pending)
	}
	router.lastRenewLog.Store(0)
	now := time.Now()
	if !router.shouldLogRenewFailure(now) {
		t.Fatal("expected the first failure to be logged")
	}
	if router.shouldLogRenewFailure(now.Add(time.Second)) {
		t.Fatal("expected a nearby failure to be rate limited")
	}
	if !router.shouldLogRenewFailure(now.Add(routeLogInterval + time.Second)) {
		t.Fatal("expected logging after the rate limit window")
	}
}

func TestRenewalDelayIsBoundedAndVariesByOwner(t *testing.T) {
	interval := 10 * time.Second
	seen := make(map[time.Duration]struct{})
	for i := range 256 {
		state := uint64(0)
		delay := renewalDelay("worker", gen.PID{Node: "node-a", ID: uint64(i + 1), Creation: 1}, interval, &state)
		if delay < 9*time.Second || delay > 11*time.Second {
			t.Fatalf("delay outside jitter bounds: %s", delay)
		}
		seen[delay] = struct{}{}
	}
	if len(seen) < 200 {
		t.Fatalf("renewals are insufficiently distributed: %d unique delays", len(seen))
	}
	state := uint64(0)
	if got := renewalDelay("worker", gen.PID{}, 5*time.Nanosecond, &state); got != 5*time.Nanosecond {
		t.Fatalf("unexpected sub-jitter delay: %s", got)
	}
	if seed := routeJitterSeed("", gen.PID{}); seed == 0 {
		t.Fatal("jitter seed must never be zero")
	}
}

func TestActorRouterLocateSelfAndOfflineNode(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
	if err != nil {
		t.Fatal(err)
	}
	book := NewAddressBook()
	if err := book.SetAvailableNodes(NewNodeList(actor.Node().Name())); err != nil {
		t.Fatal(err)
	}
	if err := router.Bind(actor.Node()); err != nil {
		t.Fatal(err)
	}
	if err := book.BindLocator(actor.Node().Name(), router.lookup); err != nil {
		t.Fatal(err)
	}
	if err := router.Bind(actor.Node()); err != nil {
		t.Fatalf("same-node bind must be idempotent: %v", err)
	}
	self := actor.PID()
	if acquired, err := store.Acquire(context.Background(), "self", self, time.Minute); err != nil || !acquired {
		t.Fatal("failed to acquire self route")
	}
	if pid, found, err := book.Locate(context.Background(), "self"); err != nil || !found || pid != self {
		t.Fatalf("locate self: pid=%s found=%v err=%v", pid, found, err)
	}
	offline := gen.PID{Node: "offline@localhost", ID: 99, Creation: 1}
	if acquired, err := store.Acquire(context.Background(), "offline", offline, time.Minute); err != nil || !acquired {
		t.Fatal("failed to acquire offline route")
	}
	if _, found, err := book.Locate(context.Background(), "offline"); err != nil || found {
		t.Fatalf("offline route must be filtered: found=%v err=%v", found, err)
	}
	if _, found, err := store.Lookup(context.Background(), "offline"); err != nil || !found {
		t.Fatal("offline lookup must not delete persistence record")
	}
}

func TestActorRouterRenewalScheduleFitsLease(t *testing.T) {
	for _, interval := range []time.Duration{29 * time.Second, 27 * time.Second} {
		if _, err := normalizeActorRouterOptions(ActorRouterOptions{LeaseTTL: 30 * time.Second, RenewInterval: interval}); err == nil {
			t.Fatalf("accepted renewal schedule that can outlive lease: %s", interval)
		}
	}
	if _, err := normalizeActorRouterOptions(ActorRouterOptions{LeaseTTL: 30 * time.Second, RenewInterval: 26 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

func (p *memoryActorRoutePersistence) Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || current.pid != old {
		return false, nil
	}
	p.routes[key] = memoryActorRoute{pid: pid, expiresAt: time.Now().Add(ttl)}
	return true, nil
}
