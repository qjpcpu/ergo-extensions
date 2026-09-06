package system

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestActorRouteMemorySessionAndRouteLifetimes(t *testing.T) {
	s := routeStore(t)
	s.Close()
	now := time.Now()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	pid := gen.PID{Node: "node", ID: 1, Creation: 1}
	a, _ := s.OpenSession(ctx, pid.Node, 30*time.Second)
	b, _ := s.OpenSession(ctx, pid.Node, 30*time.Second)
	if a.SessionID == b.SessionID {
		t.Fatal("node restart reused session")
	}
	got, e := s.AcquireRoute(ctx, a.SessionID, "key", pid, nil, time.Hour)
	if e != nil || got.Status != RouteAcquired {
		t.Fatal(got, e)
	}
	now = now.Add(10 * time.Second)
	s.RenewSession(ctx, a.SessionID, 30*time.Second)
	snapshot, found, e := s.ReadRoute(ctx, "key")
	if e != nil || !found || !snapshot.SessionValid || snapshot.ValidFor != time.Hour-10*time.Second {
		t.Fatal(snapshot, found, e)
	}
	got, e = s.AcquireRoute(ctx, a.SessionID, "key", pid, nil, time.Hour)
	if e != nil || got.ValidFor != time.Hour {
		t.Fatal(got, e)
	}
	now = now.Add(31 * time.Second)
	snapshot, found, e = s.ReadRoute(ctx, "key")
	if e != nil || !found || snapshot.SessionValid {
		t.Fatal(snapshot, found, e)
	}
	if _, e = s.RenewSession(ctx, a.SessionID, time.Minute); !errors.Is(e, ErrSessionLost) {
		t.Fatal(e)
	}
	if _, e = s.AcquireRoute(ctx, a.SessionID, "key", pid, nil, time.Hour); !errors.Is(e, ErrRouteNotApplied) || !errors.Is(e, ErrSessionLost) {
		t.Fatal(e)
	}
	now = now.Add(time.Hour)
	if _, found, e = s.ReadRoute(ctx, "key"); e != nil || found {
		t.Fatal(found, e)
	}
	s.expire(now, 100)
	if len(s.sessions) != 0 || len(s.routes) != 0 || len(s.expirations) != 0 {
		t.Fatal("expiration retained records")
	}
}
func TestActorRouteMemoryExactOwnerConcurrentTakeover(t *testing.T) {
	s := routeStore(t)
	ctx := context.Background()
	old := gen.PID{Node: "old", ID: 1, Creation: 1}
	id := routeSeed(t, s, "key", old)
	expected := RouteOwner{id, old}
	var won atomic.Int64
	var winner RouteOwner
	var mu sync.Mutex
	var wg sync.WaitGroup
	for j := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := gen.PID{Node: "next", ID: uint64(j + 2), Creation: 2}
			session, _ := s.OpenSession(ctx, pid.Node, time.Minute)
			v, e := s.AcquireRoute(ctx, session.SessionID, "key", pid, &expected, time.Hour)
			if e != nil {
				t.Error(e)
			}
			if v.Status == RouteAcquired {
				won.Add(1)
				mu.Lock()
				winner = RouteOwner{session.SessionID, pid}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatal(won.Load())
	}
	s.ReleaseRoute(ctx, id, "key", old)
	s.ReleaseRoute(ctx, winner.SessionID, "key", gen.PID{Node: winner.PID.Node, ID: winner.PID.ID, Creation: 1})
	snap, found, e := s.ReadRoute(ctx, "key")
	if e != nil || !found || snap.Owner != winner {
		t.Fatal(snap, found, e)
	}
	v, e := s.AcquireRoute(ctx, id, "key", old, nil, time.Hour)
	if e != nil || v.Status != RouteOccupied {
		t.Fatal(v, e)
	}
	s.ReleaseRoute(ctx, winner.SessionID, "key", winner.PID)
	v, e = s.AcquireRoute(ctx, id, "key", old, &expected, time.Hour)
	if e != nil || v.Status != RouteCompareFailed {
		t.Fatal(v, e)
	}
}
func TestActorRouteMemoryCloseIsTerminal(t *testing.T) {
	s := routeStore(t)
	ctx := context.Background()
	session, _ := s.OpenSession(ctx, "node", time.Minute)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); s.RenewSession(ctx, session.SessionID, time.Minute) }()
	}
	s.CloseSession(ctx, session.SessionID)
	wg.Wait()
	if _, e := s.RenewSession(ctx, session.SessionID, time.Minute); !errors.Is(e, ErrSessionLost) {
		t.Fatal(e)
	}
	if e := s.CloseSession(ctx, session.SessionID); e != nil {
		t.Fatal(e)
	}
}
func TestActorRouteMemoryCanceledOperations(t *testing.T) {
	s := routeStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.OpenSession(ctx, "node", time.Minute); e == nil {
		t.Fatal("open")
	}
	if _, e := s.RenewSession(ctx, "missing", time.Minute); e == nil {
		t.Fatal("renew")
	}
	if e := s.CloseSession(ctx, "missing"); e == nil {
		t.Fatal("close")
	}
	if _, _, e := s.ReadRoute(ctx, "key"); e == nil {
		t.Fatal("read")
	}
	if _, e := s.AcquireRoute(ctx, "missing", "key", gen.PID{}, nil, time.Hour); !errors.Is(e, ErrRouteNotApplied) {
		t.Fatal(e)
	}
	if e := s.ReleaseRoute(ctx, "missing", "key", gen.PID{}); e == nil {
		t.Fatal("release")
	}
}
func TestActorRouteMemoryReclaimsUnreadRecords(t *testing.T) {
	s := routeStore(t)
	ctx := context.Background()
	session, _ := s.OpenSession(ctx, "node", 50*time.Millisecond)
	for _, key := range []gen.Atom{"a", "b", "c"} {
		s.AcquireRoute(ctx, session.SessionID, key, gen.PID{Node: "node", ID: 1}, nil, 20*time.Millisecond)
	}
	routeEventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.routes) == 0 && len(s.sessions) == 0 && len(s.expirations) == 0
	})
}
func TestActorRouteMemoryRepeatedRegistrationReplacesExpiry(t *testing.T) {
	s := routeStore(t)
	ctx := context.Background()
	id := routeSeed(t, s, "key", gen.PID{ID: 1})
	for range 100 {
		s.AcquireRoute(ctx, id, "key", gen.PID{ID: 1}, nil, time.Hour)
		s.RenewSession(ctx, id, time.Minute)
	}
	s.mu.Lock()
	size := len(s.expirations)
	s.mu.Unlock()
	if size != 2 {
		t.Fatal(size)
	}
}

func TestActorRouteMemoryAcquiresExpiredEntry(t *testing.T) {
	s := routeStore(t)
	s.Close()
	now := time.Now()
	s.now = func() time.Time { return now }
	ctx := context.Background()
	session, _ := s.OpenSession(ctx, "node", time.Hour)
	pid := gen.PID{Node: "node", ID: 1}
	s.AcquireRoute(ctx, session.SessionID, "key", pid, nil, time.Second)
	now = now.Add(2 * time.Second)
	pid.ID++
	v, e := s.AcquireRoute(ctx, session.SessionID, "key", pid, nil, time.Minute)
	if e != nil || v.Status != RouteAcquired {
		t.Fatal(v, e)
	}
	if len(s.expirations) != 2 {
		t.Fatal("expired entry retained", len(s.expirations))
	}
}
