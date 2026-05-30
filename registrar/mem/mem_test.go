package mem

import (
	"testing"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/constants"
	"github.com/qjpcpu/registrar/events"
)

func TestCluster(t *testing.T) {
	c := NewCluster()
	if len(c.GetNodes()) != 0 {
		t.Error("expected 0 nodes")
	}

	c.AddRoutes("node1", []gen.Route{{Host: "h1"}}, func(e any) {})
	if len(c.GetNodes()) != 1 {
		t.Error("expected 1 node")
	}

	if len(c.GetRoutes("node1")) != 1 {
		t.Error("expected 1 route")
	}

	if c.GetLeader() != "node1" {
		t.Error("expected node1 to be leader")
	}

	c.AddRoutes("node2", []gen.Route{{Host: "h2"}}, func(e any) {})
	if len(c.GetNodes()) != 2 {
		t.Error("expected 2 nodes")
	}

	c.RemoveNode("node1")
	if len(c.GetNodes()) != 1 {
		t.Error("expected 1 node after removal")
	}
	if c.GetLeader() != "node2" {
		t.Error("leader should switch to node2")
	}

	if c.GetVersion("node2") == -1 {
		t.Error("version should be valid")
	}
}

func TestClientConfigItem(t *testing.T) {
	cluster := NewCluster()
	cluster.AddRoutes("node1", []gen.Route{{Host: "h1"}}, func(e any) {})
	cluster.AddRoutes("node2", []gen.Route{{Host: "h2"}}, func(e any) {})

	client := &client{cluster: cluster}

	leader, err := client.ConfigItem(constants.LeaderNodeConfigItem)
	if err != nil {
		t.Fatalf("leader ConfigItem failed: %v", err)
	}
	if leader != gen.Atom("node1") {
		t.Fatalf("expected leader node1, got %v", leader)
	}

	version, err := client.ConfigItem("node2")
	if err != nil {
		t.Fatalf("version ConfigItem failed: %v", err)
	}
	v, ok := version.(int)
	if !ok || v < 0 {
		t.Fatalf("expected non-negative version for node2, got %v", version)
	}
}

func TestClientRegisterResolveNodesAndShutdown(t *testing.T) {
	cluster := NewCluster()
	client := CreateWithCluster(cluster).(*client)
	node := &registrarNodeStub{name: "node1"}
	routes := []gen.Route{{Host: "127.0.0.1", Port: 11144}}

	if _, err := client.Register(node, gen.RegisterRoutes{Routes: routes}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if node.eventName != "memory-node-event" {
		t.Fatalf("unexpected event name: %s", node.eventName)
	}

	gotRoutes, err := client.Resolve("node1")
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if len(gotRoutes) != 1 || gotRoutes[0].Host != "127.0.0.1" {
		t.Fatalf("unexpected routes: %#v", gotRoutes)
	}

	cluster.AddRoutes("node2", []gen.Route{{Host: "127.0.0.2"}}, func(any) {})
	nodes, err := client.Nodes()
	if err != nil {
		t.Fatalf("Nodes failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "node2" {
		t.Fatalf("expected only node2, got %#v", nodes)
	}

	client.Terminate()
	if got := cluster.GetRoutes("node1"); len(got) != 0 {
		t.Fatalf("node1 routes should be removed, got %#v", got)
	}
}

func TestClientRegisterFailure(t *testing.T) {
	client := Create().(*client)
	node := &registrarNodeStub{name: "node1", registerErr: gen.ErrTaken}

	if _, err := client.Register(node, gen.RegisterRoutes{}); err != gen.ErrTaken {
		t.Fatalf("expected RegisterEvent error, got %v", err)
	}
	if len(client.cluster.GetNodes()) != 0 {
		t.Fatalf("node should not be registered after RegisterEvent failure")
	}
}

func TestClientSendsClusterEvents(t *testing.T) {
	cluster := NewCluster()
	node1 := &registrarNodeStub{name: "node1"}
	node2 := &registrarNodeStub{name: "node2"}
	client1 := CreateWithCluster(cluster).(*client)
	client2 := CreateWithCluster(cluster).(*client)

	if _, err := client1.Register(node1, gen.RegisterRoutes{}); err != nil {
		t.Fatalf("Register node1 failed: %v", err)
	}
	if _, err := client2.Register(node2, gen.RegisterRoutes{}); err != nil {
		t.Fatalf("Register node2 failed: %v", err)
	}

	if !hasEvent[events.EventNodeJoined](node1.events, "node2") {
		t.Fatalf("node1 did not receive node2 joined event: %#v", node1.events)
	}
	if !hasEvent[events.EventNodeSwitchedToLeader](node1.events, "node1") {
		t.Fatalf("node1 did not receive leader event: %#v", node1.events)
	}

	client1.Shutdown()
	if !hasEvent[events.EventNodeLeft](node2.events, "node1") {
		t.Fatalf("node2 did not receive node1 left event: %#v", node2.events)
	}
	if !hasEvent[events.EventNodeSwitchedToLeader](node2.events, "node2") {
		t.Fatalf("node2 did not receive leader switch event: %#v", node2.events)
	}
}

func TestClientUnsupportedAndMetadata(t *testing.T) {
	client := Create().(*client)

	if _, err := client.ResolveApplication("app"); err != gen.ErrNoRoute {
		t.Fatalf("ResolveApplication expected ErrNoRoute, got %v", err)
	}
	if _, err := client.ResolveProxy("proxy"); err != gen.ErrNoRoute {
		t.Fatalf("ResolveProxy expected ErrNoRoute, got %v", err)
	}
	if err := client.RegisterProxy("proxy"); err != gen.ErrUnsupported {
		t.Fatalf("RegisterProxy expected ErrUnsupported, got %v", err)
	}
	if err := client.UnregisterProxy("proxy"); err != gen.ErrUnsupported {
		t.Fatalf("UnregisterProxy expected ErrUnsupported, got %v", err)
	}
	if err := client.RegisterApplicationRoute(gen.ApplicationRoute{}); err != gen.ErrUnsupported {
		t.Fatalf("RegisterApplicationRoute expected ErrUnsupported, got %v", err)
	}
	if err := client.UnregisterApplicationRoute("app"); err != gen.ErrUnsupported {
		t.Fatalf("UnregisterApplicationRoute expected ErrUnsupported, got %v", err)
	}
	if _, err := client.Config("item"); err != gen.ErrUnsupported {
		t.Fatalf("Config expected ErrUnsupported, got %v", err)
	}

	if client.Resolver() != client {
		t.Fatalf("Resolver should return client")
	}
	evt, err := client.Event()
	if err != nil {
		t.Fatalf("Event failed: %v", err)
	}
	if evt != (gen.Event{}) {
		t.Fatalf("unexpected zero event: %#v", evt)
	}

	info := client.Info()
	if !info.SupportEvent || info.SupportRegisterProxy || info.SupportRegisterApplication {
		t.Fatalf("unexpected registrar info: %#v", info)
	}
	version := client.Version()
	if version.Name != RegistrarName || version.Release != RegistrarVersion || version.License != gen.LicenseMIT {
		t.Fatalf("unexpected version: %#v", version)
	}
}

func hasEvent[T any](items []any, name gen.Atom) bool {
	for _, item := range items {
		evt, ok := item.(T)
		if ok && eventName(evt) == name {
			return true
		}
	}
	return false
}

func eventName(item any) gen.Atom {
	switch evt := item.(type) {
	case events.EventNodeJoined:
		return evt.Name
	case events.EventNodeLeft:
		return evt.Name
	case events.EventNodeSwitchedToLeader:
		return evt.Name
	case events.EventNodeSwitchedToFollower:
		return evt.Name
	default:
		return ""
	}
}

type registrarNodeStub struct {
	name        gen.Atom
	registerErr error
	eventName   gen.Atom
	eventRef    gen.Ref
	events      []any
}

func (n *registrarNodeStub) Name() gen.Atom { return n.name }

func (n *registrarNodeStub) Creation() int64 { return 0 }

func (n *registrarNodeStub) SetEnv(name gen.Env, value any) {}

func (n *registrarNodeStub) RegisterEvent(name gen.Atom, options gen.EventOptions) (gen.Ref, error) {
	if n.registerErr != nil {
		return gen.Ref{}, n.registerErr
	}
	n.eventName = name
	n.eventRef = gen.Ref{ID: [3]uint64{1, 2, 3}}
	return n.eventRef, nil
}

func (n *registrarNodeStub) UnregisterEvent(name gen.Atom) error { return nil }

func (n *registrarNodeStub) SendEvent(name gen.Atom, token gen.Ref, options gen.MessageOptions, message any) error {
	n.events = append(n.events, message)
	return nil
}

func (n *registrarNodeStub) Log() gen.Log { return nil }

func (n *registrarNodeStub) Stop() {}

func (n *registrarNodeStub) StopForce() {}
