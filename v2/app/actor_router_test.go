package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type testRouteRecord struct {
	pid       gen.PID
	expiresAt time.Time
}

type testRoutePersistence struct {
	mu     sync.Mutex
	routes map[gen.Atom]testRouteRecord
}

func newTestRoutePersistence() *testRoutePersistence {
	return &testRoutePersistence{routes: make(map[gen.Atom]testRouteRecord)}
}

func newTestActorRouter(t testing.TB) *system.ActorRouter {
	t.Helper()
	return newTestActorRouterWithPersistence(t, newTestRoutePersistence())
}

func newTestActorRouterWithPersistence(t testing.TB, persistence system.ActorRoutePersistence) *system.ActorRouter {
	t.Helper()
	router, err := system.NewActorRouter(persistence, system.ActorRouterOptions{})
	if err != nil {
		t.Fatalf("create test actor router: %v", err)
	}
	return router
}

func (p *testRoutePersistence) Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	current, found := p.routes[key]
	if found && now.Before(current.expiresAt) && current.pid != pid {
		return false, nil
	}
	p.routes[key] = testRouteRecord{pid: pid, expiresAt: now.Add(ttl)}
	return true, nil
}

func (p *testRoutePersistence) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	current, found := p.routes[key]
	if !found || !now.Before(current.expiresAt) || current.pid != pid {
		return false, nil
	}
	current.expiresAt = now.Add(ttl)
	p.routes[key] = current
	return true, nil
}

func (p *testRoutePersistence) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, found := p.routes[key]; found && current.pid == pid {
		delete(p.routes, key)
	}
	return nil
}

func (p *testRoutePersistence) Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if err := ctx.Err(); err != nil {
		return gen.PID{}, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || !time.Now().Before(current.expiresAt) {
		return gen.PID{}, false, nil
	}
	return current.pid, true, nil
}

func (p *testRoutePersistence) Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || current.pid != old {
		return false, nil
	}
	p.routes[key] = testRouteRecord{pid: pid, expiresAt: time.Now().Add(ttl)}
	return true, nil
}
