package system

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type routeStateNode struct {
	gen.Node
	stateErr error
}

func (n routeStateNode) ProcessState(gen.PID) (gen.ProcessState, error) {
	return gen.ProcessStateRunning, n.stateErr
}

func TestActorRouteAcquireHandlesExitedLocalOwner(t *testing.T) {
	for _, test := range []struct {
		name     string
		remote   bool
		stateErr error
		acquired bool
	}{
		{name: "exited local owner", stateErr: gen.ErrProcessUnknown, acquired: true},
		{name: "live local owner"},
		{name: "remote owner", remote: true, stateErr: gen.ErrProcessUnknown},
		{name: "node stopped", stateErr: gen.ErrNodeTerminated},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
			if err != nil {
				t.Fatal(err)
			}
			store := newMemoryActorRoutePersistence()
			router, err := NewActorRouter(store, ActorRouterOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer router.Close()
			if err := router.Bind(routeStateNode{Node: actor.Node(), stateErr: test.stateErr}); err != nil {
				t.Fatal(err)
			}
			old := gen.PID{Node: actor.Node().Name(), ID: 100, Creation: 1}
			if test.remote {
				old.Node = "remote@localhost"
				registrar, _ := actor.Node().Network().Registrar()
				registrar.(*unit.TestRegistrar).AddNode(old.Node, nil)
			}
			current := gen.PID{Node: actor.Node().Name(), ID: 101, Creation: 1}
			store.Acquire(context.Background(), "key", old, time.Minute)
			acquired, err := router.acquire(context.Background(), "key", current)
			if acquired != test.acquired {
				t.Fatalf("acquired=%v want=%v err=%v", acquired, test.acquired, err)
			}
			if test.stateErr == gen.ErrNodeTerminated && !errors.Is(err, gen.ErrNodeTerminated) {
				t.Fatalf("node state error: %v", err)
			}
			if test.acquired {
				// An outstanding release from the previous incarnation is still safe.
				store.Release(context.Background(), "key", old)
				pid, found, err := store.Lookup(context.Background(), "key")
				if err != nil || !found || pid != current {
					t.Fatalf("replacement route: %v %v %v", pid, found, err)
				}
			}
		})
	}
}

type lateRouteRenewStore struct {
	*memoryActorRoutePersistence
	entered chan struct{}
	finish  chan struct{}
	first   atomic.Bool
}

func (s *lateRouteRenewStore) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if s.first.CompareAndSwap(false, true) {
		s.Release(ctx, key, pid)
		close(s.entered)
		select {
		case <-s.finish:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		return false, nil
	}
	return s.memoryActorRoutePersistence.Renew(ctx, key, pid, ttl)
}

func TestActorRouteRestoreSurvivesLateRenewResult(t *testing.T) {
	store := &lateRouteRenewStore{memoryActorRoutePersistence: newMemoryActorRoutePersistence(), entered: make(chan struct{}), finish: make(chan struct{})}
	router, err := NewActorRouter(store, ActorRouterOptions{RenewInterval: time.Minute, LeaseTTL: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("key", &routerTestActor{}) })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		router.manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "key", pid: actor.PID()})
		close(done)
	}()
	<-store.entered
	restored, err := router.restoreRoute(context.Background(), "key", actor.PID())
	close(store.finish)
	<-done
	if err != nil || !restored {
		t.Fatalf("restore: %v %v", restored, err)
	}
	if !router.manager.isTracked("key", actor.PID()) {
		t.Fatal("restored route is not renewing")
	}
	router.manager.renew(routeLeaseJob{kind: routeLeaseRenew, key: "key", pid: actor.PID()})
	pid, found, err := router.lookup(context.Background(), "key")
	if err != nil || !found || pid != actor.PID() {
		t.Fatalf("restored lookup: %v %v %v", pid, found, err)
	}
	actor.Behavior().ProcessTerminate(gen.TerminateReasonNormal)
	if restored, err := router.restoreRoute(context.Background(), "key", actor.PID()); err != nil || restored {
		t.Fatalf("terminated restore: %v %v", restored, err)
	}
}

func TestActorRouteRestorePreservesOtherOwner(t *testing.T) {
	store := newMemoryActorRoutePersistence()
	router, err := NewActorRouter(store, ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return router.WithActorRoute("key", &routerTestActor{}) })
	if err != nil {
		t.Fatal(err)
	}
	other := gen.PID{Node: "other@localhost", ID: 1, Creation: 1}
	registrar, _ := actor.Node().Network().Registrar()
	registrar.(*unit.TestRegistrar).AddNode(other.Node, nil)
	store.Release(context.Background(), "key", actor.PID())
	store.Acquire(context.Background(), "key", other, time.Minute)
	restored, err := router.restoreRoute(context.Background(), "key", actor.PID())
	if restored || !errors.Is(err, ErrActorRouteTaken) {
		t.Fatalf("restore conflict: %v %v", restored, err)
	}
	pid, found, err := store.Lookup(context.Background(), "key")
	if err != nil || !found || pid != other {
		t.Fatalf("current owner: %v %v %v", pid, found, err)
	}
}
