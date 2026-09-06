package system

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
)

// MemoryActorRoutePersistence is a shared in-process backend for examples and
// tests. Close stops its expiration worker; it is not shared between machines.
type MemoryActorRoutePersistence struct {
	mu          sync.Mutex
	sessions    map[SessionID]*memoryExpiry
	routes      map[gen.Atom]*memoryExpiry
	expirations memoryExpiryHeap
	next        uint64
	prefix      uint64
	now         func() time.Time
	stop        chan struct{}
	done        chan struct{}
	once        sync.Once
}

type memoryExpiry struct {
	at      time.Time
	index   int
	session SessionID
	route   *ActorRoute
}
type memoryExpiryHeap []*memoryExpiry

func (h memoryExpiryHeap) Len() int           { return len(h) }
func (h memoryExpiryHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h memoryExpiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *memoryExpiryHeap) Push(v any)        { e := v.(*memoryExpiry); e.index = len(*h); *h = append(*h, e) }
func (h *memoryExpiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	e.index = -1
	return e
}

var memoryStoreIDs atomic.Uint64

func NewMemoryActorRoutePersistence() *MemoryActorRoutePersistence {
	p := &MemoryActorRoutePersistence{sessions: make(map[SessionID]*memoryExpiry), routes: make(map[gen.Atom]*memoryExpiry), prefix: memoryStoreIDs.Add(1), now: time.Now, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				p.mu.Lock()
				p.expire(p.now(), 1024)
				p.mu.Unlock()
			}
		}
	}()
	return p
}
func (p *MemoryActorRoutePersistence) Close() { p.once.Do(func() { close(p.stop) }); <-p.done }
func (p *MemoryActorRoutePersistence) expire(now time.Time, limit int) {
	for limit > 0 && len(p.expirations) > 0 && !p.expirations[0].at.After(now) {
		e := heap.Pop(&p.expirations).(*memoryExpiry)
		if e.route == nil {
			delete(p.sessions, e.session)
		} else {
			delete(p.routes, e.route.Key)
		}
		limit--
	}
}
func (p *MemoryActorRoutePersistence) liveSession(id SessionID, now time.Time) bool {
	e := p.sessions[id]
	return e != nil && now.Before(e.at)
}
func (p *MemoryActorRoutePersistence) OpenSession(ctx context.Context, node gen.Atom, ttl time.Duration) (SessionLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SessionLease{}, err
	}
	p.next++
	id := SessionID(fmt.Sprintf("%d/%d/%s", p.prefix, p.next, node))
	e := &memoryExpiry{session: id, at: p.now().Add(ttl)}
	p.sessions[id] = e
	heap.Push(&p.expirations, e)
	return SessionLease{id, ttl}, nil
}
func (p *MemoryActorRoutePersistence) RenewSession(ctx context.Context, id SessionID, ttl time.Duration) (SessionLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SessionLease{}, err
	}
	now := p.now()
	if !p.liveSession(id, now) {
		return SessionLease{}, ErrSessionLost
	}
	e := p.sessions[id]
	e.at = now.Add(ttl)
	heap.Fix(&p.expirations, e.index)
	return SessionLease{id, ttl}, nil
}
func (p *MemoryActorRoutePersistence) CloseSession(ctx context.Context, id SessionID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if e := p.sessions[id]; e != nil {
		heap.Remove(&p.expirations, e.index)
		delete(p.sessions, id)
	}
	return nil
}
func (p *MemoryActorRoutePersistence) ReadRoute(ctx context.Context, key gen.Atom) (RouteSnapshot, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, false, err
	}
	now := p.now()
	e := p.routes[key]
	if e == nil {
		return RouteSnapshot{}, false, nil
	}
	if !now.Before(e.at) {
		heap.Remove(&p.expirations, e.index)
		delete(p.routes, key)
		return RouteSnapshot{}, false, nil
	}
	return RouteSnapshot{ActorRoute: *e.route, ValidFor: e.at.Sub(now), SessionValid: p.liveSession(e.route.Owner.SessionID, now)}, true, nil
}
func (p *MemoryActorRoutePersistence) AcquireRoute(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID, expected *RouteOwner, ttl time.Duration) (AcquireRouteResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return AcquireRouteResult{}, notApplied(err)
	}
	now := p.now()
	if !p.liveSession(id, now) {
		return AcquireRouteResult{}, notApplied(ErrSessionLost)
	}
	owner := RouteOwner{id, pid}
	e := p.routes[key]
	if e != nil && !now.Before(e.at) {
		heap.Remove(&p.expirations, e.index)
		delete(p.routes, key)
		e = nil
	}
	if e != nil && e.route.Owner == owner {
		e.at = now.Add(ttl)
		heap.Fix(&p.expirations, e.index)
		return AcquireRouteResult{RouteAcquired, ttl}, nil
	}
	if expected == nil && e != nil {
		return AcquireRouteResult{Status: RouteOccupied}, nil
	}
	if expected != nil && (e == nil || e.route.Owner != *expected) {
		return AcquireRouteResult{Status: RouteCompareFailed}, nil
	}
	if e == nil {
		e = &memoryExpiry{route: &ActorRoute{Key: key}, at: now.Add(ttl)}
		p.routes[key] = e
		heap.Push(&p.expirations, e)
	} else {
		e.at = now.Add(ttl)
		heap.Fix(&p.expirations, e.index)
	}
	e.route.Owner = owner
	return AcquireRouteResult{RouteAcquired, ttl}, nil
}
func (p *MemoryActorRoutePersistence) ReleaseRoute(ctx context.Context, id SessionID, key gen.Atom, pid gen.PID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if e := p.routes[key]; e != nil && e.route.Owner == (RouteOwner{id, pid}) {
		heap.Remove(&p.expirations, e.index)
		delete(p.routes, key)
	}
	return nil
}
