package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type shutdownRouteActor struct {
	act.Actor
	entered chan struct{}
	finish  chan struct{}
}

func (a *shutdownRouteActor) Init(...any) error { return nil }
func (a *shutdownRouteActor) Terminate(error) {
	close(a.entered)
	<-a.finish
}

func TestSimpleNodeShutdownAllowsTakeoverDuringCleanup(t *testing.T) {
	for _, mode := range []string{"normal", "force"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := newTestRoutePersistence(t)
			router := newTestActorRouterWithPersistence(t, store)
			runtime := unit.StartNode(t, "shutdown@localhost", gen.NodeOptions{})
			actor := &shutdownRouteActor{entered: make(chan struct{}), finish: make(chan struct{})}
			var finishOnce sync.Once
			defer finishOnce.Do(func() { close(actor.finish) })
			wrapped := router.WithActorRoute("shutdown-worker", actor)
			if _, err := runtime.Spawn(func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{}); err != nil {
				t.Fatal(err)
			}
			before, found, err := store.ReadRoute(ctx, "shutdown-worker")
			if err != nil || !found || !before.SessionValid {
				t.Fatal("expected a live route", before, found, err)
			}
			stop := func() { wrapped.ProcessTerminate(gen.TerminateReasonShutdown) }
			runtime.OnStop(stop)
			runtime.OnStopForce(stop)
			node := &nodeImpl{Node: wrapped.Node(), router: router, stopped: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				if mode == "force" {
					node.StopForce()
				} else {
					node.Stop()
				}
				close(done)
			}()
			select {
			case <-actor.entered:
			case <-time.After(time.Second):
				t.Fatal("business cleanup did not start")
			}
			after, found, err := store.ReadRoute(ctx, "shutdown-worker")
			if err != nil || !found || after.SessionValid {
				t.Fatal("expected session invalidation before business cleanup finishes", after, found, err)
			}
			next, err := store.OpenSession(ctx, "replacement@localhost", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			pid := gen.PID{Node: "replacement@localhost", ID: 100, Creation: 1}
			result, err := store.AcquireRoute(ctx, next.SessionID, "shutdown-worker", pid, &after.Owner, time.Hour)
			if err != nil || result.Status != system.RouteAcquired {
				t.Fatal("replacement could not acquire during old actor cleanup", result, err)
			}
			finishOnce.Do(func() { close(actor.finish) })
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("node shutdown did not finish")
			}
			after, found, err = store.ReadRoute(ctx, "shutdown-worker")
			if err != nil || !found || !after.SessionValid || after.Owner.PID != pid {
				t.Fatal("old actor cleanup affected replacement", after, found, err)
			}
			if router.Stats().Tracked != 0 {
				t.Fatal("completed actor remained tracked")
			}
		})
	}
}

type shutdownTimeoutStore struct {
	*testRoutePersistence
	closeErr error
}

func (s *shutdownTimeoutStore) CloseSession(ctx context.Context, id system.SessionID) error {
	<-ctx.Done()
	s.closeErr = ctx.Err()
	return s.closeErr
}

func TestSimpleNodeShutdownContinuesAfterSessionCloseTimeout(t *testing.T) {
	for _, mode := range []string{"normal", "force"} {
		t.Run(mode, func(t *testing.T) {
			store := &shutdownTimeoutStore{testRoutePersistence: newTestRoutePersistence(t)}
			router, err := system.NewActorRouter(store, system.ActorRouterOptions{
				SessionTTL:           300 * time.Millisecond,
				SessionRenewInterval: 40 * time.Millisecond,
				OperationTimeout:     30 * time.Millisecond,
				LeaseSafetyMargin:    20 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer router.Close()
			runtime := unit.StartNode(t, "shutdown-timeout@localhost", gen.NodeOptions{})
			wrapped := router.WithActorRoute("shutdown-worker", &waitableActor{})
			if _, err := runtime.Spawn(func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{}); err != nil {
				t.Fatal(err)
			}
			stopped := false
			stop := func() {
				if !errors.Is(store.closeErr, context.DeadlineExceeded) {
					t.Error("node stopped before the session close attempt completed", store.closeErr)
				}
				wrapped.ProcessTerminate(gen.TerminateReasonShutdown)
				stopped = true
			}
			runtime.OnStop(stop)
			runtime.OnStopForce(stop)
			node := &nodeImpl{Node: wrapped.Node(), router: router, stopped: make(chan struct{})}
			if mode == "force" {
				node.StopForce()
			} else {
				node.Stop()
			}
			if !stopped {
				t.Fatal("session close timeout prevented node shutdown")
			}
			if err := router.Bind(wrapped.Node()); !errors.Is(err, system.ErrActorRouterClosed) {
				t.Fatal("router remained open after session close timeout", err)
			}
			awaitRouteCondition(t, func() bool {
				snapshot, found, err := store.ReadRoute(context.Background(), "shutdown-worker")
				return err == nil && found && !snapshot.SessionValid
			})
		})
	}
}

type shutdownReasonActor struct {
	act.Actor
	reason  chan error
	entered chan struct{}
	finish  chan struct{}
}

func (a *shutdownReasonActor) Init(...any) error   { return nil }
func (a *shutdownReasonActor) Terminate(err error) { a.reason <- err }
func (a *shutdownReasonActor) HandleMessage(gen.PID, any) error {
	close(a.entered)
	<-a.finish
	return nil
}

func TestSimpleNodeShutdownTerminationReason(t *testing.T) {
	for index, mode := range []string{"normal", "force"} {
		t.Run(mode, func(t *testing.T) {
			node, err := StartSimpleNode(SimpleNodeOptions{
				NodeName:              "shutdown-reason-" + mode + "@localhost",
				Port:                  uint16(11926 + index),
				Registrar:             mem.Create(),
				ActorRoutePersistence: newTestRoutePersistence(t),
				NodeForwardWorker:     1,
				LogLevel:              gen.LogLevelDisabled,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer node.StopForce()
			reason := make(chan error, 1)
			actor := &shutdownReasonActor{reason: reason, entered: make(chan struct{}), finish: make(chan struct{})}
			var finishOnce sync.Once
			defer finishOnce.Do(func() { close(actor.finish) })
			pid, err := node.Spawn(func() gen.ProcessBehavior {
				return node.ActorRoutes().WithActorRoute("shutdown-worker", actor)
			}, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			want := gen.TerminateReasonShutdown
			if mode == "force" {
				if err := node.Send(pid, "block"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-actor.entered:
				case <-time.After(time.Second):
					t.Fatal("business callback did not start")
				}
				node.StopForce()
				finishOnce.Do(func() { close(actor.finish) })
				want = gen.TerminateReasonKill
			} else {
				node.Stop()
			}
			select {
			case got := <-reason:
				if !errors.Is(got, want) {
					t.Fatalf("Terminate reason %v, want %v", got, want)
				}
			case <-time.After(time.Second):
				t.Fatal("business Terminate was not called")
			}
		})
	}
}
