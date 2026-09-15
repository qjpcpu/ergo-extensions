package system

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

func failingRouteRenewals(t *testing.T) *faultRouteStore {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	s.acquire = func(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
		if expected != nil && *expected == (RouteOwner{SessionID: id, PID: pid}) {
			return AcquireRouteResult{}, errors.New("renewal unavailable")
		}
		return s.MemoryActorRoutePersistence.AcquireRoute(ctx, id, key, pid, expected, ttl)
	}
	return s
}

func TestActorRouteRenewsThroughMultipleLifetimes(t *testing.T) {
	s := routeStore(t)
	o := shortRouteOptions()
	o.RouteTTL = 200 * time.Millisecond
	o.RouteRenewInterval = 60 * time.Millisecond
	r := routeRouter(t, s, o)
	b := &routerTestActor{}
	wrapped := r.WithActorRoute("key", b)
	a, err := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	// Cross several original TTLs: both persistence and local dispatch must renew.
	until := time.Now().Add(3 * o.RouteTTL)
	for time.Now().Before(until) {
		a.SendMessage(gen.PID{}, "work")
		time.Sleep(20 * time.Millisecond)
	}
	snapshot, found, err := s.ReadRoute(context.Background(), "key")
	if err != nil || !found || snapshot.Owner.PID != a.PID() || snapshot.ValidFor <= 0 || !snapshot.SessionValid {
		t.Fatal(snapshot, found, err)
	}
	before := b.messages
	a.SendMessage(gen.PID{}, "after original deadline")
	if b.messages != before+1 {
		t.Fatal("renewed actor stopped dispatching")
	}
	if r.Stats().LeaseLosses != 0 {
		t.Fatal(r.Stats())
	}
}

func TestActorRouteTakeoverStopsOldOwnerAtRenewal(t *testing.T) {
	s := routeStore(t)
	o := shortRouteOptions()
	o.RouteTTL = 500 * time.Millisecond
	o.RouteRenewInterval = 100 * time.Millisecond
	oldRouter := routeRouter(t, s, o)
	oldNode := unit.StartNode(t, "old-owner@localhost", gen.NodeOptions{})
	killed := make(chan gen.PID, 2)
	oldNode.OnKill(func(pid gen.PID) error { killed <- pid; return nil })
	b := &routerTestActor{}
	wrapped := oldRouter.WithActorRoute("key", b)
	a, err := oldNode.Spawn(func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	nextRouter := routeRouter(t, s, o)
	nextNode := routeNode(t) // Registrar omits the old node while its session remains live.
	if err := nextRouter.Bind(nextNode); err != nil {
		t.Fatal(err)
	}
	if _, found, err := nextRouter.lookup(context.Background(), "key"); err != nil || found {
		t.Fatal(found, err)
	}
	next := &localRouteInstance{key: "key", pid: gen.PID{Node: nextNode.Name(), ID: 42, Creation: 1}, acquiring: true}
	if err := nextRouter.acquire(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	a.SendMessage(gen.PID{}, "overlap")
	if b.messages != 1 {
		t.Fatal("expected tolerated overlap before renewal")
	}
	select {
	case pid := <-killed:
		if pid != a.PID() {
			t.Fatal(pid)
		}
	case <-time.After(350 * time.Millisecond):
		t.Fatal("takeover was not detected at renewal")
	}
	a.SendMessage(gen.PID{}, "after renewal")
	if b.messages != 1 {
		t.Fatal("displaced actor still dispatches")
	}
	if oldRouter.Stats().LeaseLosses != 0 {
		t.Fatal("one displaced route stopped the node session")
	}
	wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	routeEventually(t, func() bool { return oldRouter.Stats().Tracked == 0 })
	snapshot, found, err := s.ReadRoute(context.Background(), "key")
	if err != nil || !found || snapshot.Owner.PID != next.pid {
		t.Fatal("old cleanup affected replacement", snapshot, found, err)
	}
}

func TestActorRouteLateRenewalDoesNotResumeExpiredActor(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	entered, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	defer finishOnce.Do(func() { close(finish) })
	s.acquire = func(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
		result, err := s.MemoryActorRoutePersistence.AcquireRoute(ctx, id, key, pid, expected, ttl)
		if expected != nil && *expected == (RouteOwner{SessionID: id, PID: pid}) {
			close(entered)
			<-finish
			close(returned)
		}
		return result, err
	}
	o := shortRouteOptions()
	o.RouteTTL = 150 * time.Millisecond
	o.RouteRenewInterval = 50 * time.Millisecond
	r := routeRouter(t, s, o)
	n := unit.StartNode(t, "late-route-renewal@localhost", gen.NodeOptions{})
	killed := make(chan gen.PID, 1)
	n.OnKill(func(pid gen.PID) error { killed <- pid; return nil })
	b := &routerTestActor{}
	wrapped := r.WithActorRoute("key", b)
	a, err := n.Spawn(func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal not scheduled")
	}
	select {
	case <-killed:
	case <-time.After(time.Second):
		t.Fatal("blocked renewal prevented expiry")
	}
	finishOnce.Do(func() { close(finish) })
	<-returned
	routeEventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return !r.instances[a.PID()].renewing })
	a.SendMessage(gen.PID{}, "late")
	if b.messages != 0 {
		t.Fatal("late renewal revived dispatch")
	}
}

func TestActorRouteTerminationReleasesAfterPendingRenewal(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	defer finishOnce.Do(func() { close(finish) })
	s.acquire = func(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
		if expected != nil && *expected == (RouteOwner{SessionID: id, PID: pid}) {
			close(entered)
			<-finish
		}
		return s.MemoryActorRoutePersistence.AcquireRoute(ctx, id, key, pid, expected, ttl)
	}
	o := shortRouteOptions()
	o.RouteTTL = 500 * time.Millisecond
	o.RouteRenewInterval = 100 * time.Millisecond
	r := routeRouter(t, s, o)
	wrapped := r.WithActorRoute("key", &routerTestActor{})
	_, err := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal not scheduled")
	}
	wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	finishOnce.Do(func() { close(finish) })
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
	if snapshot, found, err := s.ReadRoute(context.Background(), "key"); err != nil || found {
		t.Fatal("terminated route retained", snapshot, found, err)
	}
}

func TestActorRouteRenewalsSpreadSimultaneousActors(t *testing.T) {
	// Simulate one million actors acquiring at the same instant on multiple nodes.
	// Check both initial and subsequent renewals in one-second write buckets.
	const count = 1_000_000
	const seconds = 20 * 60
	var buckets [2][seconds + 1]int
	start := time.Unix(0, 0)
	interval := 100 * time.Minute
	for id := 0; id < count; id++ {
		i := localRouteInstance{key: "bulk-route", pid: gen.PID{Node: gen.Atom(fmt.Sprintf("node-%d", id%16)), ID: uint64(id/16 + 1), Creation: 1}}
		for round := range buckets {
			i.scheduleRenewal(start, interval)
			delay := i.renewAt.Sub(start)
			if delay < 90*time.Minute || delay > 110*time.Minute {
				t.Fatalf("renewal outside safe window: %v", delay)
			}
			buckets[round][int((delay-90*time.Minute)/time.Second)]++
		}
	}
	for round, counts := range buckets {
		peak := 0
		for _, n := range counts {
			peak = max(peak, n)
		}
		// A synchronized implementation puts all actors in one bucket. Allow
		// generous headroom around the ~833 writes/second expected distribution.
		if peak > 2*count/seconds {
			t.Fatalf("round %d concentrated %d renewals in one second", round, peak)
		}
		t.Logf("round %d: million simultaneous actors, peak %d scheduled renewals/second", round, peak)
	}
}

func TestActorRouteRetryBackoffKeepsDeadline(t *testing.T) {
	start := time.Unix(0, 0)
	deadline := start.Add(2 * time.Hour)
	i := localRouteInstance{key: "retry", pid: gen.PID{Node: "node", ID: 1}, deadline: deadline}
	for _, base := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		i.retryRenewal(start, 100*time.Minute)
		delay := i.renewAt.Sub(start)
		if delay < base-base/10 || delay > base+base/10 {
			t.Fatalf("retry %v outside jittered base %v", delay, base)
		}
		if i.deadline != deadline {
			t.Fatal("retry moved expiry")
		}
	}
	i.scheduleRenewal(start, 100*time.Minute)
	i.retryRenewal(start, 100*time.Minute)
	if i.renewAt.Sub(start) > 1100*time.Millisecond {
		t.Fatal("successful renewal did not reset backoff")
	}
}
