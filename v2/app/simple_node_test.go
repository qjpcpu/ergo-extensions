package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

func TestSimpleNode_Integration(t *testing.T) {
	cluster := mem.NewCluster()
	routes := newTestRoutePersistence()
	node1, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: routes,
		NodeName:              "node1@localhost",
		Port:                  11001,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node1: %v", err)
	}
	defer node1.Stop()
	routes1 := node1.ActorRoutes()

	node2, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: routes,
		NodeName:              "node2@localhost",
		Port:                  11002,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node2: %v", err)
	}
	defer node2.Stop()

	// Wait for cluster stabilization.
	time.Sleep(2 * time.Second)

	if node1.Topology() == nil {
		t.Error("Topology should not be nil")
	}

	started := make(chan gen.PID, 1)
	factory := func() gen.ProcessBehavior {
		return routes1.WithActorRoute("integration-worker", &waitableActor{started: started})
	}
	pid, err := node1.ForwardSpawn("integration-worker", factory)
	if err != nil {
		t.Fatalf("ForwardSpawn failed: %v", err)
	}
	if startedPID := <-started; startedPID != pid {
		t.Fatalf("expected returned PID %v to match started PID %v", pid, startedPID)
	}
	waitForProcessLocation(t, node2, "integration-worker", pid)
	response, err := node2.ForwardCall("integration-worker", "ping", ForwardTimeout(1))
	if err != nil {
		t.Fatalf("ForwardCall failed: %v", err)
	}
	if response != "pong" {
		t.Fatalf("expected routed actor callback response, got %#v", response)
	}
	if err := node2.ForwardSend("integration-worker", "stop"); err != nil {
		t.Fatalf("ForwardSend failed: %v", err)
	}
}

func TestAcceptorNetFamily(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "default", want: "tcp"},
		{name: "tcp", value: "tcp", want: "tcp"},
		{name: "tcp4", value: "tcp4", want: "tcp4"},
		{name: "tcp6", value: "tcp6", want: "tcp6"},
		{name: "invalid", value: "udp", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := acceptorNetFamily(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), "invalid AcceptorNetFamily") {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestSimpleNodeRequiresActorRoutePersistence(t *testing.T) {
	if _, err := StartSimpleNode(SimpleNodeOptions{}); !errors.Is(err, system.ErrActorRoutePersistenceNil) {
		t.Fatalf("expected missing actor route persistence error, got %v", err)
	}
}

func TestSimpleNodeStopForce(t *testing.T) {
	var configuredRoutes ActorRoutes
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-stop-force@localhost",
		Registrar:             mem.Create(),
		Port:                  11009,
		MoreApps: func(routes ActorRoutes) []gen.ApplicationBehavior {
			configuredRoutes = routes
			return nil
		},
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	if node.ActorRoutes() == nil || node.ActorRoutes() != configuredRoutes {
		t.Fatal("expected the node and application factory to share the actor routes facade")
	}
	concreteRouter := node.(*nodeImpl).router
	node.StopForce()
	if err := concreteRouter.Bind(node); !errors.Is(err, system.ErrActorRouterClosed) {
		t.Fatalf("expected StopForce to close the node-owned actor router, got %v", err)
	}
}

func TestSimpleNodeValidatesActorRouterOptions(t *testing.T) {
	_, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		ActorRouterOptions: system.ActorRouterOptions{
			LeaseTTL:      time.Second,
			RenewInterval: time.Second,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "renew interval must be shorter") {
		t.Fatalf("expected invalid actor router options error, got %v", err)
	}
}

func TestForwardOptions(t *testing.T) {
	options := new(forwardopts)
	ForwardTimeout(7)(options)
	ForwardNode("node-b@localhost")(options)
	ForwardImportant()(options)
	if options.Timeout != 7 || options.Node != "node-b@localhost" || !options.Important {
		t.Fatalf("unexpected forwarding options: %+v", options)
	}
}

func TestAcceptorHost(t *testing.T) {
	tests := []struct {
		name      string
		netFamily string
		want      string
	}{
		{name: "tcp", netFamily: "tcp", want: "0.0.0.0"},
		{name: "tcp4", netFamily: "tcp4", want: "0.0.0.0"},
		{name: "tcp6", netFamily: "tcp6", want: "::"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := acceptorHost(tt.netFamily); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestStrReturnsFirstNonEmptyOrEmpty(t *testing.T) {
	if got := str("", "", "value", "fallback"); got != "value" {
		t.Fatalf("expected first non-empty value, got %q", got)
	}
	if got := str("", ""); got != "" {
		t.Fatalf("expected empty value, got %q", got)
	}
}

func TestForwardCallUnknownProcessDoesNotFallbackLocal(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-forward@localhost",
		Port:                  11003,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	waitForTopology(t, node)

	_, err = node.ForwardCall("missing_process", "ping", ForwardTimeout(1))
	if !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("expected ErrProcessUnknown, got %v", err)
	}
}

func TestForwardSendUnknownProcessDoesNotFallbackLocal(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-forward-send@localhost",
		Port:                  11004,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	waitForTopology(t, node)

	err = node.ForwardSend("missing_process", "ping")
	if !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("expected ErrProcessUnknown, got %v", err)
	}
}

func TestWaitPIDUsesRouteActor(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-waitpid@localhost",
		Port:                  11005,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	waitForTopology(t, node)

	pid, err := node.Spawn(func() gen.ProcessBehavior { return &waitableActor{} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("failed to spawn actor: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- node.WaitPID(pid)
	}()

	time.Sleep(100 * time.Millisecond)
	if err := node.Send(pid, "stop"); err != nil {
		t.Fatalf("failed to stop actor: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("WaitPID failed: %v", err)
	}
}

func TestForwardSpawnReturnsPIDAndCanWaitWithRouteActor(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-spawnwait@localhost",
		Port:                  11006,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	routes := node.ActorRoutes()
	waitForTopology(t, node)

	started := make(chan gen.PID, 1)
	processName := "spawnwait-worker"
	pid, err := node.ForwardSpawn(processName, func() gen.ProcessBehavior {
		return routes.WithActorRoute(gen.Atom(processName), &waitableActor{started: started})
	})
	if err != nil {
		t.Fatalf("ForwardSpawn failed: %v", err)
	}
	if startedPID := <-started; startedPID != pid {
		t.Fatalf("expected returned PID %v to match started PID %v", pid, startedPID)
	}

	done := make(chan error, 1)
	go func() {
		done <- node.WaitPID(pid)
	}()

	waitForProcessLocation(t, node, gen.Atom(processName), pid)
	time.Sleep(100 * time.Millisecond)
	if err := node.Send(pid, "stop"); err != nil {
		t.Fatalf("failed to stop spawned actor: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("WaitPID failed: %v", err)
	}
}

func TestForwardPIDUsesRouteActor(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-forwardpid@localhost",
		Port:                  11008,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	waitForTopology(t, node)

	pid, err := node.Spawn(func() gen.ProcessBehavior { return &waitableActor{} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("failed to spawn actor: %v", err)
	}

	res, err := node.ForwardCallPID(pid, "ping", ForwardTimeout(1))
	if err != nil {
		t.Fatalf("ForwardCallPID failed: %v", err)
	}
	if res != "pong" {
		t.Fatalf("expected pong, got %#v", res)
	}

	done := make(chan error, 1)
	go func() {
		done <- node.WaitPID(pid)
	}()

	time.Sleep(100 * time.Millisecond)
	if err := node.ForwardSendPID(pid, "stop"); err != nil {
		t.Fatalf("ForwardSendPID failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("WaitPID failed: %v", err)
	}
}

func TestForwardSpawnUsesRouteActor(t *testing.T) {
	cluster := mem.NewCluster()
	node, err := StartSimpleNode(SimpleNodeOptions{
		ActorRoutePersistence: newTestRoutePersistence(),
		NodeName:              "node-spawn@localhost",
		Port:                  11007,
		Cookie:                "test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()
	routes := node.ActorRoutes()
	waitForTopology(t, node)

	started := make(chan gen.PID, 1)
	processName := "spawn-worker"
	pid, err := node.ForwardSpawn(processName, func() gen.ProcessBehavior {
		return routes.WithActorRoute(gen.Atom(processName), &waitableActor{started: started})
	})
	if err != nil {
		t.Fatalf("ForwardSpawn failed: %v", err)
	}

	if startedPID := <-started; startedPID != pid {
		t.Fatalf("expected returned PID %v to match started PID %v", pid, startedPID)
	}
	waitForProcessLocation(t, node, gen.Atom(processName), pid)
	if err := node.Send(pid, "stop"); err != nil {
		t.Fatalf("failed to stop spawned actor: %v", err)
	}
}

func waitForProcessLocation(t *testing.T, node Node, process gen.Atom, want gen.PID) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, found, err := node.ActorRoutes().Locate(context.Background(), process)
		if err == nil && found && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process %s was not located as %s", process, want)
}

func waitForTopology(t *testing.T, node Node) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if node.Topology().GetAvailableNodes().Len() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("address book did not initialize")
}

type waitableActor struct {
	act.Actor
	started chan gen.PID
}

func (a *waitableActor) Init(args ...any) error {
	if a.started != nil {
		a.started <- a.PID()
	}
	return nil
}

func (a *waitableActor) HandleMessage(from gen.PID, message any) error {
	if msg, ok := message.(string); ok && msg == "stop" {
		return gen.TerminateReasonNormal
	}
	return nil
}

func (a *waitableActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	if msg, ok := request.(string); ok && msg == "ping" {
		return "pong", nil
	}
	return nil, nil
}
