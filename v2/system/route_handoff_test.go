package system

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"testing"
	"time"
)

func TestAcquireOfflineNodeBeforeLeaseExpiry(t *testing.T) {
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{LeaseTTL: time.Hour, RenewInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	router.Bind(actor.Node())
	old := gen.PID{Node: "offline@localhost", ID: 1, Creation: 1}
	ctx := context.Background()
	store.Acquire(ctx, "key", old, time.Hour)
	acquired, err := router.acquire(ctx, "key", actor.PID())
	if err != nil || !acquired {
		t.Fatal(acquired, err)
	}
	store.Release(ctx, "key", old)
	owner, found, err := store.Lookup(ctx, "key")
	if err != nil || !found || owner != actor.PID() {
		t.Fatal(owner, found, err)
	}
}

type concurrentReplaceStore struct {
	*memoryActorRoutePersistence
	winner gen.PID
}

func (s *concurrentReplaceStore) Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error) {
	s.memoryActorRoutePersistence.Replace(ctx, key, old, s.winner, ttl)
	return s.memoryActorRoutePersistence.Replace(ctx, key, old, pid, ttl)
}
func TestAcquirePreservesConcurrentReplacement(t *testing.T) {
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
	if err != nil {
		t.Fatal(err)
	}
	store := &concurrentReplaceStore{memoryActorRoutePersistence: newMemoryActorRoutePersistence(), winner: gen.PID{Node: "winner", ID: 9, Creation: 2}}
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	router.Bind(actor.Node())
	ctx := context.Background()
	store.Acquire(ctx, "key", gen.PID{Node: "offline", ID: 1, Creation: 1}, time.Hour)
	acquired, err := router.acquire(ctx, "key", actor.PID())
	if err != nil || acquired {
		t.Fatal(acquired, err)
	}
	owner, _, _ := store.Lookup(ctx, "key")
	if owner != store.winner {
		t.Fatal(owner)
	}
}

type handoffActor struct {
	act.Actor
	entered, finish chan struct{}
}

func (a *handoffActor) Init(...any) error { return nil }
func (a *handoffActor) Terminate(error)   { close(a.entered); <-a.finish }
func TestRouteHeldThroughBusinessTermination(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{LeaseTTL: time.Second, RenewInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	behavior := &handoffActor{entered: make(chan struct{}), finish: make(chan struct{})}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("key", behavior) })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { actor.Behavior().ProcessTerminate(gen.TerminateReasonNormal); close(done) }()
	<-behavior.entered
	defer func() { close(behavior.finish); <-done }()
	owner, found, err := store.Lookup(context.Background(), "key")
	if err != nil || !found || owner != actor.PID() {
		t.Fatal(owner, found, err)
	}
	if acquired, _ := store.Acquire(context.Background(), "key", gen.PID{Node: "new", ID: 2, Creation: 2}, time.Minute); acquired {
		t.Fatal("route released before business cleanup")
	}
	if !router.manager.isTracked("key", actor.PID()) {
		t.Fatal("cleanup route not renewing")
	}
}
func TestManagerCloseDrainsReleases(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	manager := newRouteLeaseManagerState(router)
	for _, key := range []gen.Atom{"a", "b", "c"} {
		pid := gen.PID{Node: "local", ID: 1, Creation: 1}
		store.Acquire(context.Background(), key, pid, time.Hour)
		manager.track(key, pid)
		manager.untrack(key, pid)
	}
	manager.close()
	for _, key := range []gen.Atom{"a", "b", "c"} {
		if _, found, _ := store.Lookup(context.Background(), key); found {
			t.Fatal("release not drained", key)
		}
	}
}

func TestConfirmedExitCleansRouteDroppedByFullReleaseQueue(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{ReleaseQueueSize: 1, LeaseTTL: time.Hour, RenewInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	manager := newRouteLeaseManagerState(router)
	pid := gen.PID{Node: "exit@localhost", ID: 1, Creation: 1}
	for _, key := range []gen.Atom{"queued", "dropped"} {
		store.Acquire(context.Background(), key, pid, time.Hour)
		manager.track(key, pid)
		manager.untrack(key, pid)
	}
	if router.releaseDropped.Load() != 1 {
		t.Fatal("release queue was not saturated")
	}
	if err := router.releaseExitedRoute(context.Background(), "dropped", pid); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.Lookup(context.Background(), "dropped"); found {
		t.Fatal("confirmed exit left hour lease")
	}
	next := gen.PID{Node: "next@localhost", ID: 2, Creation: 2}
	store.Acquire(context.Background(), "dropped", next, time.Hour)
	router.releaseExitedRoute(context.Background(), "dropped", pid)
	if owner, found, _ := store.Lookup(context.Background(), "dropped"); !found || owner != next {
		t.Fatal("late exit cleanup changed new owner")
	}
	manager.close()
}
