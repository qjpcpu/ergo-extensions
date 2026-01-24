package mem

import (
	"testing"

	"ergo.services/ergo/gen"
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
