package system

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

func newManagerForTest(t *testing.T, persistence ActorRoutePersistence, options ActorRouterOptions) (*ActorRouter, *routeLeaseManager) {
	t.Helper()
	router, err := NewActorRouter(persistence, options)
	if err != nil {
		t.Fatal(err)
	}
	return router, newRouteLeaseManagerState(router)
}

func scheduleLeaseForTest(manager *routeLeaseManager, key gen.Atom, pid gen.PID, due time.Time) {
	manager.track(key, pid)
	shard := manager.shard(key)
	shard.mu.Lock()
	lease := shard.entries[key]
	manager.removeFromWheelLocked(shard, lease)
	lease.next = due.Sub(manager.started).Nanoseconds()
	manager.addToWheelLocked(shard, lease)
	shard.mu.Unlock()
}

func pendingLeaseCount(manager *routeLeaseManager) int {
	total := 0
	for index := range manager.shards {
		shard := &manager.shards[index]
		shard.mu.Lock()
		for _, lease := range shard.entries {
			if lease.pending {
				total++
			}
		}
		shard.mu.Unlock()
	}
	return total
}

func markLeasePendingForTest(manager *routeLeaseManager, key gen.Atom) {
	shard := manager.shard(key)
	shard.mu.Lock()
	lease := shard.entries[key]
	manager.removeFromWheelLocked(shard, lease)
	lease.pending = true
	shard.mu.Unlock()
}

func TestMillionRoutesUseBoundedManagerResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-route stress test in short mode")
	}
	before := runtime.NumGoroutine()
	_, manager := newManagerForTest(t, newMemoryActorRoutePersistence(), ActorRouterOptions{
		RenewInterval:    time.Minute,
		LeaseTTL:         2 * time.Minute,
		RenewWorkers:     3,
		RenewQueueSize:   17,
		ReleaseQueueSize: 19,
	})
	const routeCount = 1_000_000
	due := time.Now().Add(30 * time.Second)
	for i := range routeCount {
		key := gen.Atom("route/" + strconv.Itoa(i))
		scheduleLeaseForTest(manager, key, gen.PID{ID: uint64(i + 1)}, due)
	}
	manager.enqueueDue(due.Add(manager.resolution))
	if got := manager.trackedCount(); got != routeCount {
		t.Fatalf("tracked route count: got %d, want %d", got, routeCount)
	}
	if got := cap(manager.renewJobs); got != 17 {
		t.Fatalf("renewal queue capacity: got %d, want 17", got)
	}
	if got := len(manager.renewJobs); got != 17 {
		t.Fatalf("simultaneous expiry exceeded the renewal queue bound: %d", got)
	}
	if got := pendingLeaseCount(manager); got != 17 {
		t.Fatalf("simultaneous expiry created %d pending jobs, want 17", got)
	}
	if got := cap(manager.releaseJobs); got != 19 {
		t.Fatalf("release queue capacity: got %d, want 19", got)
	}
	if added := runtime.NumGoroutine() - before; added > 1 {
		t.Fatalf("route count created unexpected goroutines: %d", added)
	}
}

func TestTimingWheelBoundedQueueAndPendingSuppression(t *testing.T) {
	_, manager := newManagerForTest(t, newMemoryActorRoutePersistence(), ActorRouterOptions{
		RenewInterval:    time.Second,
		LeaseTTL:         time.Minute,
		RenewQueueSize:   1,
		ReleaseQueueSize: 1,
	})
	due := time.Now().Add(2 * manager.resolution)
	scheduleLeaseForTest(manager, "a", gen.PID{ID: 1}, due)
	scheduleLeaseForTest(manager, "b", gen.PID{ID: 2}, due)
	scheduleLeaseForTest(manager, "c", gen.PID{ID: 3}, due.Add(time.Hour))

	manager.enqueueDue(due.Add(manager.resolution))
	if len(manager.renewJobs) != 1 {
		t.Fatalf("expected one bounded renewal job, got %d", len(manager.renewJobs))
	}
	if pending := pendingLeaseCount(manager); pending != 1 {
		t.Fatalf("expected exactly one pending renewal, got %d", pending)
	}
	manager.enqueueDue(due.Add(2 * manager.resolution))
	if len(manager.renewJobs) != 1 || pendingLeaseCount(manager) != 1 {
		t.Fatal("a full queue created duplicate pending work")
	}
}

func TestTimingWheelHandlesClockJumpAndFutureRounds(t *testing.T) {
	_, manager := newManagerForTest(t, newMemoryActorRoutePersistence(), ActorRouterOptions{
		RenewInterval: 2 * time.Second,
		LeaseTTL:      time.Minute,
	})
	pid := gen.PID{ID: 1}
	farDue := time.Now().Add(time.Duration(routeLeaseWheelSlots+5) * manager.resolution)
	scheduleLeaseForTest(manager, "future", pid, farDue)
	manager.enqueueDue(farDue.Add(-manager.resolution))
	if len(manager.renewJobs) != 0 {
		t.Fatal("a future timing-wheel round was renewed early")
	}
	manager.enqueueDue(farDue.Add(manager.resolution))
	if len(manager.renewJobs) != 1 {
		t.Fatal("future timing-wheel round was not renewed when due")
	}
}

func TestRouteLeaseManagerLostOwnershipAndStaleJobs(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	_, manager := newManagerForTest(t, store, ActorRouterOptions{})
	oldPID := gen.PID{Node: "node@localhost", ID: 1, Creation: 1}
	newPID := gen.PID{Node: "node@localhost", ID: 2, Creation: 1}
	manager.track("key", oldPID)
	store.routes["key"] = memoryActorRoute{pid: newPID, expiresAt: time.Now().Add(time.Minute)}

	manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "key", pid: oldPID})
	if manager.isTracked("key", oldPID) {
		t.Fatal("a lease that lost ownership must be removed")
	}
	manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "missing", pid: oldPID})
	manager.execute(routeLeaseJob{})
}

func TestRouteLeaseManagerUntrackAndReleaseErrors(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	_, manager := newManagerForTest(t, store, ActorRouterOptions{ReleaseQueueSize: 1})
	pid := gen.PID{Node: "node@localhost", ID: 1, Creation: 1}
	manager.track("key", pid)
	manager.untrack("key", gen.PID{ID: 99})
	if !manager.isTracked("key", pid) {
		t.Fatal("a different PID must not untrack the owner")
	}
	manager.untrack("key", pid)
	job := <-manager.releaseJobs
	if job.kind != routeLeaseRelease || job.key != "key" || job.pid != pid {
		t.Fatalf("unexpected release job: %+v", job)
	}
	store.releaseErr = errors.New("unavailable")
	manager.execute(job)
	manager.untrack("missing", pid)
}

func TestExitStormKeepsReleaseQueueBounded(t *testing.T) {
	router, manager := newManagerForTest(t, newMemoryActorRoutePersistence(), ActorRouterOptions{
		ReleaseQueueSize: 7,
	})
	for i := range 100_000 {
		key := gen.Atom("exit/" + strconv.Itoa(i))
		pid := gen.PID{ID: uint64(i + 1)}
		manager.track(key, pid)
		manager.untrack(key, pid)
	}
	if got := len(manager.releaseJobs); got != 7 {
		t.Fatalf("release queue exceeded or missed its bound: %d", got)
	}
	if got := manager.trackedCount(); got != 0 {
		t.Fatalf("exit storm retained %d local leases", got)
	}
	router.Close()
	router.Close()
	if err := router.trackRoute("closed", gen.PID{ID: 1}); !errors.Is(err, ErrActorRouterClosed) {
		t.Fatalf("expected closed router error, got %v", err)
	}
}

type orderedPersistence struct {
	*memoryActorRoutePersistence
	calls chan string
}

func (p *orderedPersistence) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	p.calls <- "renew"
	return p.memoryActorRoutePersistence.Renew(ctx, key, pid, ttl)
}

func (p *orderedPersistence) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	p.calls <- "release"
	return p.memoryActorRoutePersistence.Release(ctx, key, pid)
}

func TestReleaseQueueHasPriority(t *testing.T) {
	store := &orderedPersistence{memoryActorRoutePersistence: newMemoryActorRoutePersistence(), calls: make(chan string, 2)}
	_, manager := newManagerForTest(t, store, ActorRouterOptions{RenewWorkers: 1})
	pid := gen.PID{ID: 1}
	manager.track("renew", pid)
	markLeasePendingForTest(manager, "renew")
	store.routes["renew"] = memoryActorRoute{pid: pid, expiresAt: time.Now().Add(time.Minute)}
	manager.renewJobs <- routeLeaseJob{kind: routeLeaseRenew, key: "renew", pid: pid}
	manager.releaseJobs <- routeLeaseJob{kind: routeLeaseRelease, key: "release", pid: pid}
	manager.wg.Add(1)
	go manager.work()
	if first := <-store.calls; first != "release" {
		t.Fatalf("first persistence operation was %s, want release", first)
	}
	close(manager.stop)
	manager.wg.Wait()
}

type panicOncePersistence struct {
	*memoryActorRoutePersistence
	renewPanicked   atomic.Bool
	releasePanicked atomic.Bool
	released        chan gen.Atom
}

func (p *panicOncePersistence) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if p.renewPanicked.CompareAndSwap(false, true) {
		panic("renew panic")
	}
	return p.memoryActorRoutePersistence.Renew(ctx, key, pid, ttl)
}

func (p *panicOncePersistence) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if p.releasePanicked.CompareAndSwap(false, true) {
		panic("release panic")
	}
	err := p.memoryActorRoutePersistence.Release(ctx, key, pid)
	if err == nil {
		p.released <- key
	}
	return err
}

func TestPersistencePanicDoesNotKillWorkerOrWedgeRenewal(t *testing.T) {
	store := &panicOncePersistence{
		memoryActorRoutePersistence: newMemoryActorRoutePersistence(),
		released:                    make(chan gen.Atom, 1),
	}
	router, err := NewActorRouter(store, ActorRouterOptions{RenewWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	manager := newRouteLeaseManager(router)
	router.manager = manager
	pid := gen.PID{ID: 1}
	manager.track("renew", pid)
	markLeasePendingForTest(manager, "renew")
	store.routes["renew"] = memoryActorRoute{pid: pid, expiresAt: time.Now().Add(time.Minute)}
	manager.renewJobs <- routeLeaseJob{kind: routeLeaseRenew, key: "renew", pid: pid}
	manager.releaseJobs <- routeLeaseJob{kind: routeLeaseRelease, key: "first", pid: pid}
	manager.releaseJobs <- routeLeaseJob{kind: routeLeaseRelease, key: "second", pid: pid}

	select {
	case key := <-store.released:
		if key != "second" {
			t.Fatalf("unexpected successful release: %s", key)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not survive persistence panic")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		shard := manager.shard("renew")
		shard.mu.Lock()
		lease := shard.entries["renew"]
		ready := store.renewPanicked.Load() && lease != nil && !lease.pending
		shard.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("renew panic left the lease pending")
}

type slowPersistence struct {
	active atomic.Int64
	max    atomic.Int64
	done   chan struct{}
	count  atomic.Int64
}

func (p *slowPersistence) Acquire(context.Context, gen.Atom, gen.PID, time.Duration) (bool, error) {
	return true, nil
}

func (p *slowPersistence) Renew(context.Context, gen.Atom, gen.PID, time.Duration) (bool, error) {
	active := p.active.Add(1)
	for {
		maximum := p.max.Load()
		if active <= maximum || p.max.CompareAndSwap(maximum, active) {
			break
		}
	}
	time.Sleep(5 * time.Millisecond)
	p.active.Add(-1)
	if p.count.Add(1) == 40 {
		close(p.done)
	}
	return true, nil
}

func (*slowPersistence) Release(context.Context, gen.Atom, gen.PID) error { return nil }
func (*slowPersistence) Lookup(context.Context, gen.Atom) (gen.PID, bool, error) {
	return gen.PID{}, false, nil
}

func TestSlowPersistenceConcurrencyIsBounded(t *testing.T) {
	store := &slowPersistence{done: make(chan struct{})}
	router, err := NewActorRouter(store, ActorRouterOptions{
		RenewWorkers:   4,
		RenewQueueSize: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	manager := newRouteLeaseManager(router)
	router.manager = manager
	for i := range 40 {
		key := gen.Atom("slow/" + strconv.Itoa(i))
		pid := gen.PID{ID: uint64(i + 1)}
		manager.track(key, pid)
		markLeasePendingForTest(manager, key)
		manager.renewJobs <- routeLeaseJob{kind: routeLeaseRenew, key: key, pid: pid}
	}
	select {
	case <-store.done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow persistence jobs did not complete")
	}
	if maximum := store.max.Load(); maximum != 4 {
		t.Fatalf("maximum persistence concurrency: got %d, want 4", maximum)
	}
}

func TestRouteSchedulerResolutionBounds(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{interval: 40 * time.Second, want: time.Second},
		{interval: 2 * time.Second, want: 100 * time.Millisecond},
		{interval: 10 * time.Millisecond, want: time.Millisecond},
		{interval: time.Nanosecond, want: time.Nanosecond},
	}
	for _, test := range tests {
		if got := routeSchedulerResolution(test.interval); got != test.want {
			t.Fatalf("resolution for %s: got %s, want %s", test.interval, got, test.want)
		}
	}
}

type runActor struct {
	act.Actor
	runs       int
	terminates int
}

func (a *runActor) ProcessRun() error {
	a.runs++
	return errors.New("actor run")
}

func (a *runActor) ProcessTerminate(error) { a.terminates++ }

type runSupervisor struct {
	act.Supervisor
	runs       int
	terminates int
}

func (s *runSupervisor) Init(...any) (act.SupervisorSpec, error) {
	return act.SupervisorSpec{}, nil
}

func (s *runSupervisor) ProcessRun() error {
	s.runs++
	return errors.New("supervisor run")
}

func (s *runSupervisor) ProcessTerminate(error) { s.terminates++ }

type runPool struct {
	act.Pool
	runs       int
	terminates int
}

func (p *runPool) Init(...any) (act.PoolOptions, error) {
	return act.PoolOptions{}, nil
}

func (p *runPool) ProcessRun() error {
	p.runs++
	return errors.New("pool run")
}

func (p *runPool) ProcessTerminate(error) { p.terminates++ }

func TestRoutedBehaviorsForwardRunAndTerminate(t *testing.T) {
	router, err := NewActorRouter(newMemoryActorRoutePersistence(), ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	actor := &runActor{}
	routedActor := router.WithActorRoute("actor", actor).(*routedActorBehavior)
	if err := routedActor.ProcessRun(); err == nil || actor.runs != 1 {
		t.Fatalf("actor run was not forwarded: runs=%d err=%v", actor.runs, err)
	}
	routedActor.ProcessTerminate(nil)

	supervisor := &runSupervisor{}
	routedSupervisor := router.WithSupervisorRoute("supervisor", supervisor).(*routedSupervisorBehavior)
	if err := routedSupervisor.ProcessRun(); err == nil || supervisor.runs != 1 {
		t.Fatalf("supervisor run was not forwarded: runs=%d err=%v", supervisor.runs, err)
	}
	routedSupervisor.ProcessTerminate(nil)

	pool := &runPool{}
	routedPool := router.WithPoolRoute("pool", pool).(*routedPoolBehavior)
	if err := routedPool.ProcessRun(); err == nil || pool.runs != 1 {
		t.Fatalf("pool run was not forwarded: runs=%d err=%v", pool.runs, err)
	}
	routedPool.ProcessTerminate(nil)

	invalidActor := router.WithActorRoute("", actor).(*routedActorBehavior)
	if !errors.Is(invalidActor.ProcessRun(), ErrActorRouteKeyEmpty) {
		t.Fatal("expected the route validation error from ProcessRun")
	}
	invalidSupervisor := router.WithSupervisorRoute("", supervisor).(*routedSupervisorBehavior)
	if !errors.Is(invalidSupervisor.ProcessRun(), ErrActorRouteKeyEmpty) {
		t.Fatal("expected the route validation error from ProcessRun")
	}
	invalidPool := router.WithPoolRoute("", pool).(*routedPoolBehavior)
	if !errors.Is(invalidPool.ProcessRun(), ErrActorRouteKeyEmpty) {
		t.Fatal("expected the route validation error from ProcessRun")
	}

	routeErr := routeErrorBehavior{err: ErrActorRouteBehaviorMismatch}
	if !errors.Is(routeErr.ProcessRun(), ErrActorRouteBehaviorMismatch) {
		t.Fatal("expected route error behavior to retain its error")
	}
	routeErr.ProcessTerminate(nil)
}

func (*slowPersistence) Replace(context.Context, gen.Atom, gen.PID, gen.PID, time.Duration) (bool, error) {
	return false, nil
}
