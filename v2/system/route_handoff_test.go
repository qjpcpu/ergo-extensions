package system

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type handoffActor struct {
	act.Actor
	entered, finish chan struct{}
}

func (a *handoffActor) Init(...any) error { return nil }
func (a *handoffActor) Terminate(error)   { close(a.entered); <-a.finish }
func TestRouteHeldThroughBusinessTermination(t *testing.T) {
	s := routeStore(t)
	r := routeRouter(t, s, ActorRouterOptions{})
	b := &handoffActor{entered: make(chan struct{}), finish: make(chan struct{})}
	wrapped := r.WithActorRoute("key", b)
	a, e := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() { wrapped.ProcessTerminate(gen.TerminateReasonNormal); close(done) }()
	<-b.entered
	snapshot, found, e := s.ReadRoute(context.Background(), "key")
	if e != nil || !found || snapshot.Owner.PID != a.PID() {
		t.Fatal(snapshot, found, e)
	}
	r.Drain()
	close(b.finish)
	<-done
	routeEventually(t, func() bool {
		_, found, err := s.ReadRoute(context.Background(), "key")
		return err == nil && !found
	})
	r.Close()
	if _, e := s.RenewSession(context.Background(), r.session, r.options.SessionTTL); e != ErrSessionLost {
		t.Fatal(e)
	}
}

type routeExitedNode struct {
	gen.Node
	entered chan struct{}
	once    sync.Once
}

func (n *routeExitedNode) ProcessState(gen.PID) (gen.ProcessState, error) {
	n.once.Do(func() { close(n.entered) })
	return gen.ProcessStateTerminated, gen.ErrProcessUnknown
}
func TestActorRouteLocalRestartWaitsForConfirmedCleanup(t *testing.T) {
	s := routeStore(t)
	r := routeRouter(t, s, ActorRouterOptions{})
	n := &routeExitedNode{Node: routeNode(t), entered: make(chan struct{})}
	r.Bind(n)
	oldPID := gen.PID{Node: n.Name(), ID: 100}
	s.AcquireRoute(context.Background(), r.session, "key", oldPID, nil, time.Hour)
	old := &localRouteInstance{key: "key", pid: oldPID, releaseNeeded: true, done: make(chan struct{})}
	r.mu.Lock()
	r.instances[oldPID] = old
	r.mu.Unlock()
	next := &localRouteInstance{key: "key", pid: gen.PID{Node: n.Name(), ID: 101}, acquiring: true}
	result := make(chan error, 1)
	go func() { result <- r.acquire(context.Background(), next) }()
	<-n.entered
	snapshot, found, e := s.ReadRoute(context.Background(), "key")
	if e != nil || !found || snapshot.Owner.PID != oldPID {
		t.Fatal(snapshot, found, e)
	}
	select {
	case e := <-result:
		t.Fatal("restart passed unfinished cleanup", e)
	default:
	}
	r.mu.Lock()
	old.cleanup = true
	r.finishLocked(old)
	r.mu.Unlock()
	if e := <-result; e != nil {
		t.Fatal(e)
	}
	snapshot, found, e = s.ReadRoute(context.Background(), "key")
	if e != nil || !found || snapshot.Owner.PID != next.pid {
		t.Fatal(snapshot, found, e)
	}
}

func TestLocalReplacementWaitsForTimedOutAcquire(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	r := routeRouter(t, s, ActorRouterOptions{})
	n := &routeExitedNode{Node: routeNode(t), entered: make(chan struct{})}
	if err := r.Bind(n); err != nil {
		t.Fatal(err)
	}
	oldPID := gen.PID{Node: n.Name(), ID: 100}
	written, finish := make(chan struct{}), make(chan struct{})
	var releases atomic.Int64
	s.acquire = func(c context.Context, id SessionID, k gen.Atom, p gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
		result, err := s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, expected, ttl)
		if p == oldPID {
			close(written)
			<-finish
		}
		return result, err
	}
	s.release = func(c context.Context, id SessionID, k gen.Atom, p gen.PID) error {
		releases.Add(1)
		return s.MemoryActorRoutePersistence.ReleaseRoute(c, id, k, p)
	}
	old := &localRouteInstance{key: "key", pid: oldPID, done: make(chan struct{})}
	r.mu.Lock()
	r.instances[oldPID] = old
	r.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- r.acquire(ctx, old) }()
	<-written
	cancel()
	lifecycle := routeLifecycle{router: r, instance: old}
	lifecycle.cleanup()
	next := &localRouteInstance{key: "key", pid: gen.PID{Node: n.Name(), ID: 101}}
	replaced := make(chan error, 1)
	go func() { replaced <- r.acquire(context.Background(), next) }()
	<-n.entered
	if releases.Load() != 0 {
		t.Error("release preceded acquisition completion")
	}
	select {
	case err := <-replaced:
		t.Error("replacement passed an unfinished write", err)
	default:
	}
	close(finish)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
	snapshot, found, err := s.ReadRoute(context.Background(), "key")
	if err != nil || !found || snapshot.Owner.PID != next.pid {
		t.Fatal("cleanup deleted replacement", snapshot, found, err)
	}
}
