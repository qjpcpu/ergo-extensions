package system

import (
	"context"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestActorRouteLookupAndTakeoverShareValidity(t *testing.T) {
	for _, mode := range []string{"online", "offline", "session closed", "expired route", "local"} {
		t.Run(mode, func(t *testing.T) {
			s := routeStore(t)
			r := routeRouter(t, s, ActorRouterOptions{})
			n := routeNode(t)
			if e := r.Bind(n); e != nil {
				t.Fatal(e)
			}
			old := gen.PID{Node: "owner@localhost", ID: 1, Creation: 1}
			if mode == "local" {
				old.Node = n.Name()
			}
			id := routeSeed(t, s, "key", old)
			reg, _ := n.Network().Registrar()
			if mode != "offline" {
				reg.(*unit.TestRegistrar).AddNode(old.Node, nil)
			}
			if mode == "session closed" {
				s.CloseSession(context.Background(), id)
			}
			if mode == "expired route" {
				s.AcquireRoute(context.Background(), id, "key", old, nil, -time.Second)
			}
			_, found, e := r.lookup(nil, "key")
			want := mode == "online" || mode == "local"
			if e != nil || found != want {
				t.Fatal(found, e)
			}
			i := &localRouteInstance{key: "key", pid: gen.PID{Node: n.Name(), ID: 9, Creation: 1}, acquiring: true}
			e = r.acquire(context.Background(), i)
			if want {
				if !errors.Is(e, ErrActorRouteTaken) {
					t.Fatal(e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				s.ReleaseRoute(context.Background(), id, "key", old)
				snapshot, found, e := s.ReadRoute(context.Background(), "key")
				if e != nil || !found || snapshot.Owner.PID != i.pid {
					t.Fatal(snapshot, found, e)
				}
			}
		})
	}
}
func TestActorRouteRegistrarChangesAreReadDirectly(t *testing.T) {
	s := routeStore(t)
	r := routeRouter(t, s, ActorRouterOptions{})
	n := routeNode(t)
	r.Bind(n)
	pid := gen.PID{Node: "remote@localhost", ID: 1}
	routeSeed(t, s, "key", pid)
	reg, _ := n.Network().Registrar()
	registrar := reg.(*unit.TestRegistrar)
	registrar.AddNode(pid.Node, nil)
	if _, found, e := r.lookup(nil, "key"); e != nil || !found {
		t.Fatal(found, e)
	}
	registrar.RemoveNode(pid.Node)
	if _, found, e := r.lookup(nil, "key"); e != nil || found {
		t.Fatal(found, e)
	}
}

type routeNetworkNode struct {
	gen.Node
	network gen.Network
}

func (n *routeNetworkNode) Network() gen.Network { return n.network }

type routeFailNetwork struct {
	gen.Network
	registrar gen.Registrar
	err       error
}

func (n routeFailNetwork) Registrar() (gen.Registrar, error) { return n.registrar, n.err }

type routeFailRegistrar struct {
	gen.Registrar
	err error
}

func (r routeFailRegistrar) Nodes() ([]gen.Atom, error) { return nil, r.err }
func TestActorRouteRegistrarFailuresPreventTakeover(t *testing.T) {
	want := errors.New("registrar unavailable")
	for _, network := range []gen.Network{nil, routeFailNetwork{err: want}, routeFailNetwork{registrar: routeFailRegistrar{err: want}}} {
		s := routeStore(t)
		r := routeRouter(t, s, ActorRouterOptions{})
		n := &routeNetworkNode{Node: routeNode(t), network: network}
		r.Bind(n)
		pid := gen.PID{Node: "remote", ID: 1}
		routeSeed(t, s, "key", pid)
		if _, _, e := r.lookup(nil, "key"); e == nil {
			t.Fatal("lookup hid registrar failure")
		}
		i := &localRouteInstance{key: "key", pid: gen.PID{Node: n.Name(), ID: 2}, acquiring: true}
		if e := r.acquire(context.Background(), i); !errors.Is(e, ErrRouteNotApplied) {
			t.Fatal(e)
		}
		snapshot, _, _ := s.ReadRoute(context.Background(), "key")
		if snapshot.Owner.PID != pid {
			t.Fatal("registrar failure permitted takeover")
		}
	}
}
func TestActorRouteConcurrentAcquisitionRetriesComparison(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	r := routeRouter(t, s, ActorRouterOptions{})
	r.Bind(routeNode(t))
	routeSeed(t, s, "key", gen.PID{Node: "offline", ID: 1})
	const count = 16
	var ready sync.WaitGroup
	ready.Add(count)
	s.acquire = func(c context.Context, id SessionID, k gen.Atom, p gen.PID, o *RouteOwner, d time.Duration) (AcquireRouteResult, error) {
		ready.Done()
		ready.Wait()
		return s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, o, d)
	}
	var wg sync.WaitGroup
	var won atomic.Int64
	for j := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := &localRouteInstance{key: "key", pid: gen.PID{Node: r.node.Name(), ID: uint64(j + 10)}, acquiring: true}
			e := r.acquire(context.Background(), i)
			if e == nil {
				won.Add(1)
			} else if !errors.Is(e, ErrActorRouteTaken) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatal(won.Load())
	}
}
