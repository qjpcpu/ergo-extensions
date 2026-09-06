package system

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"sync"
	"testing"
	"time"
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
	a, e := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped })
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
	old := &localRouteInstance{key: "key", pid: oldPID, acquired: true, done: make(chan struct{})}
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
	close(old.done)
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
