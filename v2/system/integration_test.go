package system_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/app"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

var integrationNodeSequence atomic.Int64

type integrationRoute struct {
	pid       gen.PID
	expiresAt time.Time
}

type integrationRoutePersistence struct {
	mu     sync.Mutex
	routes map[gen.Atom]integrationRoute
}

var sharedIntegrationRoutes = &integrationRoutePersistence{routes: make(map[gen.Atom]integrationRoute)}

func (p *integrationRoutePersistence) Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if found && time.Now().Before(current.expiresAt) && current.pid != pid {
		return false, nil
	}
	p.routes[key] = integrationRoute{pid: pid, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (p *integrationRoutePersistence) Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || current.pid != pid || !time.Now().Before(current.expiresAt) {
		return false, nil
	}
	current.expiresAt = time.Now().Add(ttl)
	p.routes[key] = current
	return true, nil
}

func (p *integrationRoutePersistence) Release(ctx context.Context, key gen.Atom, pid gen.PID) error {
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

func (p *integrationRoutePersistence) Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
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

func uniqueNodeName(base string) string {
	sequence := integrationNodeSequence.Add(1)
	parts := strings.SplitN(base, "@", 2)
	if len(parts) != 2 {
		return fmt.Sprintf("%s-%d", base, sequence)
	}
	return fmt.Sprintf("%s-%d@%s", parts[0], sequence, parts[1])
}

type testProc struct{ act.Actor }

func (p *testProc) Init(args ...any) error { return nil }

func (p *testProc) HandleMessage(from gen.PID, message any) error { return nil }

func startNode(t *testing.T, cluster *mem.Cluster, name string) app.Node {
	t.Helper()
	node, err := app.StartSimpleNode(app.SimpleNodeOptions{
		ActorRoutePersistence: sharedIntegrationRoutes,
		NodeName:              uniqueNodeName(name),
		Cookie:                "v2-integration-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
		MembershipOptions: system.MembershipOptions{
			RefreshInterval: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("start node %s: %v", name, err)
	}
	t.Cleanup(func() {
		node.Stop()
		_ = node.WaitWithTimeout(3 * time.Second)
	})
	return node
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}

func locateNode(node app.Node, key gen.Atom) (gen.Atom, bool) {
	pid, found, err := node.ActorRoutes().Locate(context.Background(), key)
	if err != nil || !found {
		return "", false
	}
	return pid.Node, true
}

func (p *integrationRoutePersistence) Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, found := p.routes[key]
	if !found || current.pid != old {
		return false, nil
	}
	p.routes[key] = integrationRoute{pid: pid, expiresAt: time.Now().Add(ttl)}
	return true, nil
}
