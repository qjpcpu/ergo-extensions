package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/app"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
)

type routeRecord struct {
	pid       gen.PID
	expiresAt time.Time
}

// memoryRoutes demonstrates the persistence contract. Production clusters
// need a shared, durable implementation such as Redis or MySQL.
type memoryRoutes struct {
	mu     sync.Mutex
	routes map[gen.Atom]routeRecord
}

func (m *memoryRoutes) Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	current, found := m.routes[key]
	if found && now.Before(current.expiresAt) && current.pid != pid {
		return false, nil
	}
	m.routes[key] = routeRecord{pid: pid, expiresAt: now.Add(ttl)}
	return true, nil
}

func (m *memoryRoutes) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	current, found := m.routes[key]
	if !found || current.pid != pid || !now.Before(current.expiresAt) {
		return false, nil
	}
	current.expiresAt = now.Add(ttl)
	m.routes[key] = current
	return true, nil
}

func (m *memoryRoutes) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, found := m.routes[key]; found && current.pid == pid {
		delete(m.routes, key)
	}
	return nil
}

func (m *memoryRoutes) Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if err := ctx.Err(); err != nil {
		return gen.PID{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.routes[key]
	if !found || !time.Now().Before(current.expiresAt) {
		return gen.PID{}, false, nil
	}
	return current.pid, true, nil
}

type echo struct{ act.Actor }

func (e *echo) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	return request, nil
}

func main() {
	store := &memoryRoutes{routes: make(map[gen.Atom]routeRecord)}
	node, err := app.StartSimpleNode(app.SimpleNodeOptions{
		Registrar:             mem.Create(),
		NodeName:              "example@localhost",
		ActorRoutePersistence: store,
	})
	if err != nil {
		panic(err)
	}
	defer node.Stop()
	routes := node.ActorRoutes()

	key := gen.Atom("examples/echo")
	_, err = node.Spawn(func() gen.ProcessBehavior {
		return routes.WithActorRoute(key, &echo{})
	}, gen.ProcessOptions{})
	if err != nil {
		panic(err)
	}

	pid, found, err := routes.Locate(context.Background(), key)
	if err != nil {
		panic(err)
	}
	if !found {
		panic("route was not found")
	}

	reply, err := node.Call(pid, "hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
}

func (p *memoryRoutes) Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || current.pid != old {
		return false, nil
	}
	p.routes[key] = routeRecord{pid: pid, expiresAt: time.Now().Add(ttl)}
	return true, nil
}
