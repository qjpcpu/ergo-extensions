package system

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"errors"
	"strings"
	"testing"
	"time"
)

func routeStore(t testing.TB) *MemoryActorRoutePersistence {
	t.Helper()
	s := NewMemoryActorRoutePersistence()
	t.Cleanup(s.Close)
	return s
}
func routeRouter(t testing.TB, s ActorRoutePersistence, o ActorRouterOptions) *ActorRouter {
	t.Helper()
	r, e := NewActorRouter(s, o)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(r.Close)
	return r
}
func routeEventually(t testing.TB, f func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatal("route condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func routeNode(t *testing.T) gen.Node {
	a, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} })
	if e != nil {
		t.Fatal(e)
	}
	return a.Node()
}
func routeSeed(t testing.TB, s ActorRoutePersistence, key gen.Atom, pid gen.PID) SessionID {
	t.Helper()
	ctx := context.Background()
	session, e := s.OpenSession(ctx, pid.Node, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.AcquireRoute(ctx, session.SessionID, key, pid, nil, time.Hour)
	if e != nil || v.Status != RouteAcquired {
		t.Fatal(v, e)
	}
	return session.SessionID
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
	s := routeStore(t)
	r := routeRouter(t, s, ActorRouterOptions{})
	if r.options != DefaultActorRouterOptions() {
		t.Fatal(r.options)
	}
	if _, e := NewActorRouter(nil, ActorRouterOptions{}); !errors.Is(e, ErrActorRoutePersistenceNil) {
		t.Fatal(e)
	}
	for _, o := range []ActorRouterOptions{{SessionTTL: -1}, {RouteChangeWorkers: -1}, {SessionTTL: time.Second}, {RouteTTL: time.Second}} {
		if _, e := NewActorRouter(s, o); e == nil {
			t.Fatal(o)
		}
	}
	if _, _, e := r.lookup(nil, "key"); !errors.Is(e, ErrActorRouterUnbound) {
		t.Fatal(e)
	}
	if _, _, e := r.lookup(nil, ""); !errors.Is(e, ErrActorRouteKeyEmpty) {
		t.Fatal(e)
	}
	if e := r.Bind(nil); e == nil {
		t.Fatal("nil node accepted")
	}
	n := routeNode(t)
	if e := r.Bind(n); e != nil {
		t.Fatal(e)
	}
	id := r.session
	if e := r.Bind(n); e != nil || r.session != id {
		t.Fatal(e)
	}
	if e := r.Bind(routeNode(t)); !errors.Is(e, ErrActorRouterBound) {
		t.Fatal(e)
	}
	r.Close()
	if _, _, e := r.lookup(nil, "key"); !errors.Is(e, ErrActorRouterClosed) {
		t.Fatal(e)
	}
	if e := r.Bind(n); !errors.Is(e, ErrActorRouterClosed) {
		t.Fatal(e)
	}
}
func TestActorRouterTypedBehavior(t *testing.T) {
	for _, kind := range []string{"actor", "supervisor", "pool"} {
		t.Run(kind, func(t *testing.T) {
			r := routeRouter(t, routeStore(t), ActorRouterOptions{})
			var original gen.ProcessBehavior
			switch kind {
			case "actor":
				original = &routerTestActor{}
			case "supervisor":
				original = &routerTestSupervisor{}
			case "pool":
				original = &routerTestPool{}
			}
			wrapped := r.routeFactory("key", func() gen.ProcessBehavior { return original })()
			a, e := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped })
			if e != nil {
				t.Fatal(e)
			}
			defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
			switch b := original.(type) {
			case *routerTestActor:
				if !b.initialized || !b.behaviorOK {
					t.Fatal("actor behavior hidden")
				}
				a.SendMessage(gen.PID{}, "hello")
				if b.messages != 1 {
					t.Fatal(b.messages)
				}
			case *routerTestSupervisor:
				if !b.initialized || !b.behaviorOK {
					t.Fatal("supervisor behavior hidden")
				}
			case *routerTestPool:
				if !b.initialized || !b.behaviorOK {
					t.Fatal("pool behavior hidden")
				}
			}
			pid, found, e := r.lookup(nil, "key")
			if e != nil || !found || pid != a.PID() {
				t.Fatal(pid, found, e)
			}
		})
	}
}
func TestActorRouterInvalidBehavior(t *testing.T) {
	r := routeRouter(t, routeStore(t), ActorRouterOptions{})
	var typedNil *routerTestActor
	for _, test := range []struct {
		b    gen.ProcessBehavior
		want error
	}{{r.WithActorRoute("", &routerTestActor{}), ErrActorRouteKeyEmpty}, {r.WithActorRoute("key", nil), ErrActorRouteBehaviorNil}, {r.WithActorRoute("key", typedNil), ErrActorRouteBehaviorNil}, {r.routeFactory("key", nil)(), ErrActorRouteFactoryNil}, {r.routeFactory("key", func() gen.ProcessBehavior { return routeErrorBehavior{} })(), ErrActorRouteBehaviorMismatch}, {(*ActorRouter)(nil).WithActorRoute("key", &routerTestActor{}), ErrActorRoutePersistenceNil}} {
		_, e := unit.Spawn(t, func() gen.ProcessBehavior { return test.b })
		if e == nil || !strings.Contains(e.Error(), test.want.Error()) {
			t.Fatal(e, test.want)
		}
		test.b.ProcessTerminate(e)
	}
}
func TestRenewalJitterBounded(t *testing.T) {
	var a, b uint64
	for range 100 {
		d := renewalDelay("session", gen.PID{ID: 1}, time.Second, &a)
		if d < 900*time.Millisecond || d > 1100*time.Millisecond {
			t.Fatal(d)
		}
		if d != renewalDelay("session", gen.PID{ID: 1}, time.Second, &b) {
			t.Fatal("jitter not reproducible")
		}
	}
	if renewalDelay("", gen.PID{}, 1, &a) != 1 {
		t.Fatal("small interval changed")
	}
}
