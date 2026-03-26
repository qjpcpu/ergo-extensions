package mem

import (
	"testing"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/constants"
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
