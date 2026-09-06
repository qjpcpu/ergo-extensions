package system_test

import (
	"context"
	"fmt"
	"strings"
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

var sharedIntegrationRoutes = system.NewMemoryActorRoutePersistence()

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
