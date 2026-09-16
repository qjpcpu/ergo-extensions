package system

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

func shortRouteOptions() ActorRouterOptions {
	return ActorRouterOptions{SessionTTL: 300 * time.Millisecond, SessionRenewInterval: 40 * time.Millisecond, OperationTimeout: 30 * time.Millisecond, LeaseSafetyMargin: 20 * time.Millisecond, RouteTTL: time.Hour, RouteRenewInterval: 50 * time.Minute, RouteChangeWorkers: 2, RouteChangeQueueSize: 8}
}

type faultRouteStore struct {
	open         func(context.Context, gen.Atom, time.Duration) (SessionLease, error)
	closeSession func(context.Context, SessionID) error
	read         func(context.Context, gen.Atom) (RouteSnapshot, bool, error)
	*MemoryActorRoutePersistence
	acquire  func(context.Context, SessionID, gen.Atom, gen.PID, *RouteOwner, time.Duration) (AcquireRouteResult, error)
	renew    func(context.Context, SessionID, time.Duration) (SessionLease, error)
	release  func(context.Context, SessionID, gen.Atom, gen.PID) error
	renewals atomic.Int64
}

func (s *faultRouteStore) OpenSession(c context.Context, n gen.Atom, d time.Duration) (SessionLease, error) {
	if s.open != nil {
		return s.open(c, n, d)
	}
	return s.MemoryActorRoutePersistence.OpenSession(c, n, d)
}
func (s *faultRouteStore) CloseSession(c context.Context, id SessionID) error {
	if s.closeSession != nil {
		return s.closeSession(c, id)
	}
	return s.MemoryActorRoutePersistence.CloseSession(c, id)
}
func (s *faultRouteStore) ReadRoute(c context.Context, k gen.Atom) (RouteSnapshot, bool, error) {
	if s.read != nil {
		return s.read(c, k)
	}
	return s.MemoryActorRoutePersistence.ReadRoute(c, k)
}
func (s *faultRouteStore) AcquireRoute(c context.Context, id SessionID, k gen.Atom, p gen.PID, o *RouteOwner, d time.Duration) (AcquireRouteResult, error) {
	if s.acquire != nil {
		return s.acquire(c, id, k, p, o, d)
	}
	return s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, o, d)
}
func (s *faultRouteStore) RenewSession(c context.Context, id SessionID, d time.Duration) (SessionLease, error) {
	s.renewals.Add(1)
	if s.renew != nil {
		return s.renew(c, id, d)
	}
	return s.MemoryActorRoutePersistence.RenewSession(c, id, d)
}
func (s *faultRouteStore) ReleaseRoute(c context.Context, id SessionID, k gen.Atom, p gen.PID) error {
	if s.release != nil {
		return s.release(c, id, k, p)
	}
	return s.MemoryActorRoutePersistence.ReleaseRoute(c, id, k, p)
}

type routeTestProcess struct {
	gen.Process
	pid gen.PID
}

func (p routeTestProcess) PID() gen.PID { return p.pid }

func TestActorRouteFailedAcquireKeepsExistingActor(t *testing.T) {
	for _, mode := range []string{"unknown", "panic", "not applied", "session lost"} {
		t.Run(mode, func(t *testing.T) {
			s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
			s.acquire = func(c context.Context, id SessionID, k gen.Atom, p gen.PID, o *RouteOwner, d time.Duration) (AcquireRouteResult, error) {
				if k == "existing" {
					return s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, o, d)
				}
				if mode == "not applied" {
					return AcquireRouteResult{}, notApplied(errors.New("unavailable"))
				}
				if mode == "session lost" {
					return AcquireRouteResult{}, notApplied(ErrSessionLost)
				}
				if _, err := s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, o, d); err != nil {
					return AcquireRouteResult{}, err
				}
				if mode == "panic" {
					panic("response lost")
				}
				return AcquireRouteResult{}, errors.New("response lost")
			}
			r := routeRouter(t, s, shortRouteOptions())
			node := unit.StartNode(t, "acquire@localhost", gen.NodeOptions{})
			existing := &routerTestActor{}
			first := r.WithActorRoute("existing", existing)
			a, err := node.Spawn(func() gen.ProcessBehavior { return first }, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer first.ProcessTerminate(gen.TerminateReasonNormal)
			b := &routerTestActor{}
			wrapped := r.WithActorRoute("key", b)
			pid := a.PID()
			pid.ID++
			err = wrapped.ProcessInit(routeTestProcess{Process: first, pid: pid})
			if err == nil || b.initialized {
				t.Fatal("failed acquisition ran Init", err)
			}
			wrapped.ProcessTerminate(err)
			if mode == "session lost" {
				if r.Stats().LeaseLosses != 1 {
					t.Fatal(r.Stats())
				}
				return
			}
			routeEventually(t, func() bool { return r.Stats().Tracked == 1 })
			if _, found, err := s.ReadRoute(context.Background(), "key"); err != nil || found {
				t.Fatal("failed route was not released", found, err)
			}
			snapshot, found, err := s.ReadRoute(context.Background(), "existing")
			if err != nil || !found || !snapshot.SessionValid || r.Stats().LeaseLosses != 0 {
				t.Fatal("existing route lost its session", snapshot, err, r.Stats())
			}
			a.SendMessage(gen.PID{}, "still running")
			if existing.messages != 1 {
				t.Fatal("existing actor stopped dispatching")
			}
		})
	}
}
func TestActorRouteSessionRenewalIndependentOfRouteWorkers(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	r := routeRouter(t, s, shortRouteOptions())
	r.Bind(routeNode(t))
	entered := make(chan struct{}, 2)
	finish := make(chan struct{})
	defer close(finish)
	for range 2 {
		r.manager.jobs <- func() { entered <- struct{}{}; <-finish }
	}
	<-entered
	<-entered
	routeEventually(t, func() bool { return s.renewals.Load() >= 3 })
	r.mu.Lock()
	active := r.state == routerActive
	r.mu.Unlock()
	if !active {
		t.Fatal("route work blocked heartbeat")
	}
}
func TestActorRouteSessionDeadlineSurvivesBlockedRenewal(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	finish := make(chan struct{})
	defer close(finish)
	s.renew = func(context.Context, SessionID, time.Duration) (SessionLease, error) {
		<-finish
		return SessionLease{}, errors.New("late")
	}
	r := routeRouter(t, s, shortRouteOptions())
	r.Bind(routeNode(t))
	routeEventually(t, func() bool { return r.Stats().LeaseLosses == 1 })
}
func TestReleaseQueueRetainsFailuresAndAppliesBackpressure(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	var fail atomic.Bool
	fail.Store(true)
	s.release = func(c context.Context, id SessionID, k gen.Atom, p gen.PID) error {
		if fail.Load() {
			return errors.New("unavailable")
		}
		return s.MemoryActorRoutePersistence.ReleaseRoute(c, id, k, p)
	}
	o := shortRouteOptions()
	o.ReleaseQueueSize = 1
	r := routeRouter(t, s, o)
	b := r.WithActorRoute("key", &routerTestActor{})
	_, e := unit.Spawn(t, func() gen.ProcessBehavior { return b }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	b.ProcessTerminate(gen.TerminateReasonNormal)
	routeEventually(t, func() bool { return r.Stats().ReleaseFailures >= 2 })
	if r.Stats().ReleaseQueued != 1 {
		t.Fatal(r.Stats())
	}
	fail.Store(false)
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
	if _, found, _ := s.ReadRoute(context.Background(), "key"); found {
		t.Fatal("release lost")
	}
}
func TestActorRouteDeadlineStopsBusinessDispatch(t *testing.T) {
	o := shortRouteOptions()
	o.RouteTTL = 100 * time.Millisecond
	o.RouteRenewInterval = 20 * time.Millisecond
	s := failingRouteRenewals(t)
	r := routeRouter(t, s, o)
	b := &routerTestActor{}
	wrapped := r.WithActorRoute("key", b)
	node := unit.StartNode(t, "deadline@localhost", gen.NodeOptions{})
	killed := make(chan gen.PID, 1)
	node.OnKill(func(pid gen.PID) error {
		killed <- pid
		return nil
	})
	a, e := node.Spawn(func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	select {
	case pid := <-killed:
		if pid != a.PID() {
			t.Fatalf("killed %v, want %v", pid, a.PID())
		}
	case <-time.After(time.Second):
		t.Fatal("expired route was not stopped")
	}
	a.SendMessage(gen.PID{}, "late")
	if b.messages != 0 {
		t.Fatal("expired route handled business")
	}
	if r.Stats().LeaseLosses != 0 {
		t.Fatal("route expiry lost shared session")
	}
}
func TestTimingWheelMillionRoutesBoundedResources(t *testing.T) {
	if testing.Short() {
		t.Skip("million route stress")
	}
	r := routeRouter(t, routeStore(t), ActorRouterOptions{})
	m := newRouteLeaseManager(r)
	before := runtime.NumGoroutine()
	const count = 1_000_000
	deadline := m.started.Add(time.Hour)
	for j := range count {
		i := &localRouteInstance{pid: gen.PID{ID: uint64(j + 1)}, deadline: deadline}
		m.scheduleLocked(i)
	}
	if len(m.wheel) != 1 || runtime.NumGoroutine()-before > 2 {
		t.Fatal("unbounded route resources")
	}
	now := deadline.Add(time.Second)
	expired := 0
	for expired < count {
		expired += len(m.expire(now))
	}
	if expired != count {
		t.Fatal(expired)
	}
}

type lateInitActor struct {
	act.Actor
	entered, finish chan struct{}
	initError       error
	terminated      bool
}

func (a *lateInitActor) Init(...any) error { close(a.entered); <-a.finish; return a.initError }
func (a *lateInitActor) Terminate(error)   { a.terminated = true }

func TestActorRouteLostDuringInitClosesBeforeCleanup(t *testing.T) {
	s := routeStore(t)
	r := routeRouter(t, s, shortRouteOptions())
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	b := &lateInitActor{entered: make(chan struct{}), finish: make(chan struct{})}
	wrapped := r.WithActorRoute("key", b)
	done := make(chan error, 1)
	go func() { done <- wrapped.ProcessInit(base.Behavior().(gen.Process)) }()
	<-b.entered
	r.lose()
	routeEventually(t, func() bool {
		snapshot, found, _ := s.ReadRoute(context.Background(), "key")
		return found && !snapshot.SessionValid
	})
	close(b.finish)
	if e := <-done; !errors.Is(e, ErrSessionLost) {
		t.Fatal(e)
	}
	if e := wrapped.ProcessRun(); !errors.Is(e, ErrSessionLost) {
		t.Fatal(e)
	}
	wrapped.ProcessTerminate(gen.TerminateReasonKill)
	if !b.terminated || r.Stats().Tracked != 0 {
		t.Fatal("late Init skipped cleanup")
	}
}
func TestActorRouteFailedBusinessInitReleasesAfterCleanup(t *testing.T) {
	s := routeStore(t)
	r := routeRouter(t, s, ActorRouterOptions{})
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	finish := make(chan struct{})
	close(finish)
	want := errors.New("business init failed")
	b := &lateInitActor{entered: make(chan struct{}), finish: finish, initError: want}
	wrapped := r.WithActorRoute("key", b)
	if e := wrapped.ProcessInit(base.Behavior().(gen.Process)); !errors.Is(e, want) {
		t.Fatal(e)
	}
	if _, found, _ := s.ReadRoute(context.Background(), "key"); !found {
		t.Fatal("route released before termination")
	}
	wrapped.ProcessTerminate(want)
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
	if !b.terminated {
		t.Fatal("cleanup skipped")
	}
}
func TestActorRouteQueuedCancellationKeepsSession(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	var reads atomic.Int64
	s.read = func(ctx context.Context, key gen.Atom) (RouteSnapshot, bool, error) {
		reads.Add(1)
		return s.MemoryActorRoutePersistence.ReadRoute(ctx, key)
	}
	r := routeRouter(t, s, shortRouteOptions())
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r.Bind(base.Behavior().(gen.Process).Node())
	entered := make(chan struct{}, 2)
	finish := make(chan struct{})
	for range 2 {
		r.manager.jobs <- func() { entered <- struct{}{}; <-finish }
	}
	<-entered
	<-entered
	wrapped := r.WithActorRoute("canceled", &routerTestActor{})
	e = wrapped.ProcessInit(base.Behavior().(gen.Process))
	wrapped.ProcessTerminate(e)
	if !errors.Is(e, context.DeadlineExceeded) {
		close(finish)
		t.Fatal(e)
	}
	if r.Stats().LeaseLosses != 0 {
		close(finish)
		t.Fatal("queued cancellation lost session")
	}
	// Execute the queued work after admission has already timed out.
	job := <-r.manager.jobs
	job()
	close(finish)
	if reads.Load() != 0 {
		t.Fatal("expired queued admission accessed storage")
	}
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
}
func TestActorRouteAdmissionBackpressure(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	var fail atomic.Bool
	fail.Store(true)
	s.release = func(c context.Context, id SessionID, k gen.Atom, p gen.PID) error {
		if fail.Load() {
			return errors.New("release unavailable")
		}
		return s.MemoryActorRoutePersistence.ReleaseRoute(c, id, k, p)
	}
	o := shortRouteOptions()
	o.ReleaseQueueSize = 1
	r := routeRouter(t, s, o)
	first := r.WithActorRoute("first", &routerTestActor{})
	a, e := unit.Spawn(t, func() gen.ProcessBehavior { return first }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	first.ProcessTerminate(gen.TerminateReasonNormal)
	next := r.WithActorRoute("next", &routerTestActor{})
	e = next.ProcessInit(a.Behavior().(gen.Process))
	next.ProcessTerminate(e)
	if !errors.Is(e, ErrActorRouterBusy) {
		t.Fatal(e)
	}
	fail.Store(false)
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
}
func TestActorRouteLateRenewalCannotReviveSession(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	entered := make(chan struct{})
	finish := make(chan struct{})
	returned := make(chan struct{})
	s.renew = func(c context.Context, id SessionID, d time.Duration) (SessionLease, error) {
		lease, e := s.MemoryActorRoutePersistence.RenewSession(c, id, d)
		close(entered)
		<-finish
		defer close(returned)
		return lease, e
	}
	r := routeRouter(t, s, shortRouteOptions())
	r.Bind(routeNode(t))
	<-entered
	routeEventually(t, func() bool { return r.Stats().LeaseLosses == 1 })
	close(finish)
	<-returned
	if e := r.Bind(r.node); !errors.Is(e, ErrActorRouterClosed) {
		t.Fatal(e)
	}
	routeEventually(t, func() bool {
		_, e := s.MemoryActorRoutePersistence.RenewSession(context.Background(), r.session, time.Second)
		return errors.Is(e, ErrSessionLost)
	})
}
func TestActorRouteTransientRenewalRecoversBeforeDeadline(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	var calls atomic.Int64
	s.renew = func(c context.Context, id SessionID, d time.Duration) (SessionLease, error) {
		if calls.Add(1) == 1 {
			panic("temporary renewal failure")
		}
		return s.MemoryActorRoutePersistence.RenewSession(c, id, d)
	}
	r := routeRouter(t, s, shortRouteOptions())
	r.Bind(routeNode(t))
	routeEventually(t, func() bool { return calls.Load() >= 3 })
	if st := r.Stats(); st.RenewFailures != 1 || st.LeaseLosses != 0 {
		t.Fatal(st)
	}
}

func TestActorRouterOpenFailureAndDelayedOpen(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprint(delayed), func(t *testing.T) {
			s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
			want := errors.New("open unavailable")
			s.open = func(c context.Context, n gen.Atom, d time.Duration) (SessionLease, error) {
				if !delayed {
					return SessionLease{}, want
				}
				lease, e := s.MemoryActorRoutePersistence.OpenSession(c, n, d)
				time.Sleep(40 * time.Millisecond)
				return lease, e
			}
			r := routeRouter(t, s, shortRouteOptions())
			e := r.Bind(routeNode(t))
			if delayed {
				if !errors.Is(e, ErrSessionLost) {
					t.Fatal(e)
				}
				if _, e := s.RenewSession(context.Background(), r.session, time.Second); !errors.Is(e, ErrSessionLost) {
					t.Fatal(e)
				}
			} else if !errors.Is(e, want) {
				t.Fatal(e)
			}
		})
	}
}
func TestActorRouteCloseInvalidatesSession(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	var calls atomic.Int64
	s.closeSession = func(c context.Context, id SessionID) error {
		calls.Add(1)
		return s.MemoryActorRoutePersistence.CloseSession(c, id)
	}
	r := routeRouter(t, s, ActorRouterOptions{})
	wrapped := r.WithActorRoute("key", &routerTestActor{})
	if _, err := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{}); err != nil {
		t.Fatal(err)
	}
	defer wrapped.ProcessTerminate(gen.TerminateReasonNormal)
	before, found, err := s.ReadRoute(context.Background(), "key")
	if err != nil || !found || !before.SessionValid {
		t.Fatal("expected a live route", before, found, err)
	}
	done := make(chan struct{}, 2)
	for range 2 {
		go func() { r.Close(); done <- struct{}{} }()
	}
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("session closure did not complete")
		}
	}
	after, found, err := s.ReadRoute(context.Background(), "key")
	if err != nil || !found || after.SessionValid || after.Owner != before.Owner {
		t.Fatal("expected the route to retain its owner with an invalid session", after, found, err)
	}
	if calls.Load() != 1 {
		t.Fatal("concurrent closure was not coalesced", calls.Load())
	}
	if _, err := s.RenewSession(context.Background(), before.Owner.SessionID, time.Hour); !errors.Is(err, ErrSessionLost) {
		t.Fatal("closed session was renewed", err)
	}
	if err := r.Bind(r.node); !errors.Is(err, ErrActorRouterClosed) {
		t.Fatal("closed router admitted binding", err)
	}
}

func TestActorRouteCloseDuringRenewal(t *testing.T) {
	for _, kind := range []string{"session", "route"} {
		t.Run(kind, func(t *testing.T) {
			s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
			entered, finish, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-finish:
				default:
					close(finish)
				}
			}()
			if kind == "session" {
				s.renew = func(ctx context.Context, id SessionID, ttl time.Duration) (SessionLease, error) {
					lease, err := s.MemoryActorRoutePersistence.RenewSession(ctx, id, ttl)
					close(entered)
					<-finish
					defer close(returned)
					return lease, err
				}
			} else {
				s.acquire = func(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
					result, err := s.MemoryActorRoutePersistence.AcquireRoute(ctx, id, key, pid, expected, ttl)
					if expected != nil && *expected == (RouteOwner{SessionID: id, PID: pid}) {
						close(entered)
						<-finish
						close(returned)
					}
					return result, err
				}
			}
			o := shortRouteOptions()
			o.RouteTTL = 500 * time.Millisecond
			o.RouteRenewInterval = 50 * time.Millisecond
			r := routeRouter(t, s, o)
			b := &routerTestActor{}
			wrapped := r.WithActorRoute("key", b)
			_, err := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("renewal did not start")
			}
			r.Close()
			snapshot, found, err := s.ReadRoute(context.Background(), "key")
			if err != nil || !found || snapshot.SessionValid {
				t.Fatal("in-flight renewal prevented session closure", snapshot, found, err)
			}
			wrapped.ProcessTerminate(gen.TerminateReasonShutdown)
			close(finish)
			<-returned
			routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
			if _, err := s.MemoryActorRoutePersistence.RenewSession(context.Background(), r.session, time.Hour); !errors.Is(err, ErrSessionLost) {
				t.Fatal("closed session became renewable", err)
			}
		})
	}
}

func TestActorRouteLookupFailureDoesNotWrite(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	want := errors.New("read unavailable")
	s.read = func(context.Context, gen.Atom) (RouteSnapshot, bool, error) { return RouteSnapshot{}, false, want }
	r := routeRouter(t, s, shortRouteOptions())
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r.Bind(base.Behavior().(gen.Process).Node())
	if _, _, e := r.lookup(nil, "key"); !errors.Is(e, want) {
		t.Fatal(e)
	}
	wrapped := r.WithActorRoute("key", &routerTestActor{})
	e = wrapped.ProcessInit(base.Behavior().(gen.Process))
	wrapped.ProcessTerminate(e)
	if !errors.Is(e, want) || r.Stats().LeaseLosses != 0 {
		t.Fatal(e, r.Stats())
	}
}
func TestActorRouteLookupContext(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	s.read = func(ctx context.Context, _ gen.Atom) (RouteSnapshot, bool, error) {
		<-ctx.Done()
		return RouteSnapshot{}, false, ctx.Err()
	}
	r := routeRouter(t, s, shortRouteOptions())
	if err := r.Bind(routeNode(t)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.lookup(context.Background(), "key"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("lookup did not apply the operation timeout", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := r.lookup(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatal("lookup did not preserve caller cancellation", err)
	}
}

func TestActorRouteLateAcquireIsReleasedAfterReturn(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
			finish := make(chan struct{})
			var releases atomic.Int64
			s.acquire = func(_ context.Context, id SessionID, k gen.Atom, p gen.PID, o *RouteOwner, d time.Duration) (AcquireRouteResult, error) {
				<-finish
				result, err := s.MemoryActorRoutePersistence.AcquireRoute(context.Background(), id, k, p, o, d)
				if uncertain && err == nil {
					err = errors.New("response lost")
				}
				return result, err
			}
			s.release = func(c context.Context, id SessionID, k gen.Atom, p gen.PID) error {
				if releases.Add(1) == 1 {
					return errors.New("temporary release failure")
				}
				return s.MemoryActorRoutePersistence.ReleaseRoute(c, id, k, p)
			}
			r := routeRouter(t, s, shortRouteOptions())
			b := &routerTestActor{}
			wrapped := r.WithActorRoute("key", b)
			_, err := unit.Spawn(t, func() gen.ProcessBehavior { return wrapped }, gen.ProcessOptions{})
			wrapped.ProcessTerminate(err)
			if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				close(finish)
				t.Fatal(err)
			}
			if r.Stats().Tracked != 1 || r.Stats().LeaseLosses != 0 || releases.Load() != 0 {
				close(finish)
				t.Fatal("in-flight write lost cleanup responsibility", r.Stats(), releases.Load())
			}
			close(finish)
			routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
			if b.initialized || releases.Load() < 2 || r.Stats().LeaseLosses != 0 {
				t.Fatal("late result was not cleaned locally", b.initialized, releases.Load(), r.Stats())
			}
			if _, found, err := s.ReadRoute(context.Background(), "key"); err != nil || found {
				t.Fatal("late write retained its route", found, err)
			}
		})
	}
}
func TestActorRouteQueueSaturationIsBounded(t *testing.T) {
	o := shortRouteOptions()
	o.RouteChangeQueueSize = 1
	s := routeStore(t)
	r := routeRouter(t, s, o)
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r.Bind(base.Behavior().(gen.Process).Node())
	routeSeed(t, s, "available", base.PID())
	entered := make(chan struct{}, 2)
	finish := make(chan struct{})
	defer close(finish)
	for range 2 {
		r.manager.jobs <- func() { entered <- struct{}{}; <-finish }
		<-entered
	}
	r.manager.jobs <- func() {}
	if pid, found, err := r.lookup(context.Background(), "available"); err != nil || !found || pid != base.PID() {
		t.Fatal("lookup failed while route workers were occupied", pid, found, err)
	}
	wrapped := r.WithActorRoute("busy", &routerTestActor{})
	e = wrapped.ProcessInit(base.Behavior().(gen.Process))
	wrapped.ProcessTerminate(e)
	if !errors.Is(e, ErrActorRouterBusy) {
		t.Fatal(e)
	}
	if st := r.Stats(); st.Tracked != 0 || st.RouteQueued != 1 || st.LeaseLosses != 0 {
		t.Fatal(st)
	}
}

func TestActorRouteExpirationDuringInitKeepsSessionUsable(t *testing.T) {
	s := failingRouteRenewals(t)
	o := shortRouteOptions()
	o.RouteTTL = 100 * time.Millisecond
	o.RouteRenewInterval = 20 * time.Millisecond
	r := routeRouter(t, s, o)
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	b := &lateInitActor{entered: make(chan struct{}), finish: make(chan struct{})}
	wrapped := r.WithActorRoute("key", b)
	done := make(chan error, 1)
	go func() { done <- wrapped.ProcessInit(base.Behavior().(gen.Process)) }()
	<-b.entered
	time.Sleep(120 * time.Millisecond)
	close(b.finish)
	e = <-done
	if !errors.Is(e, ErrRouteExpired) {
		t.Fatal(e)
	}
	wrapped.ProcessTerminate(e)
	if st := r.Stats(); st.LeaseLosses != 0 {
		t.Fatal(st)
	}
	if _, e := s.RenewSession(context.Background(), r.session, o.SessionTTL); e != nil {
		t.Fatal("route expiration ended shared session", e)
	}
}
func TestActorRouteDrainingDuringAcquireReleasesKnownWrite(t *testing.T) {
	s := &faultRouteStore{MemoryActorRoutePersistence: routeStore(t)}
	entered := make(chan struct{})
	finish := make(chan struct{})
	s.acquire = func(c context.Context, id SessionID, k gen.Atom, p gen.PID, o *RouteOwner, d time.Duration) (AcquireRouteResult, error) {
		v, e := s.MemoryActorRoutePersistence.AcquireRoute(c, id, k, p, o, d)
		close(entered)
		<-finish
		return v, e
	}
	r := routeRouter(t, s, ActorRouterOptions{})
	base, e := unit.Spawn(t, func() gen.ProcessBehavior { return &routerTestActor{} }, gen.ProcessOptions{})
	if e != nil {
		t.Fatal(e)
	}
	b := &routerTestActor{}
	wrapped := r.WithActorRoute("key", b)
	done := make(chan error, 1)
	go func() { done <- wrapped.ProcessInit(base.Behavior().(gen.Process)) }()
	<-entered
	r.Drain()
	close(finish)
	e = <-done
	wrapped.ProcessTerminate(e)
	if !errors.Is(e, ErrActorRouterClosed) || b.initialized {
		t.Fatal(e, b.initialized)
	}
	routeEventually(t, func() bool { return r.Stats().Tracked == 0 })
	if _, found, _ := s.ReadRoute(context.Background(), "key"); found {
		t.Fatal("known write was not released")
	}
}

func TestActorRouteCanceledBeforeQueueHasKnownOutcome(t *testing.T) {
	r := routeRouter(t, routeStore(t), ActorRouterOptions{})
	r.Bind(routeNode(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, e := routeWork(r, ctx, func() (struct{}, error) { called = true; return struct{}{}, nil })
	if called || !errors.Is(e, context.Canceled) || !errors.Is(e, ErrRouteNotApplied) {
		t.Fatal(called, e)
	}
}
