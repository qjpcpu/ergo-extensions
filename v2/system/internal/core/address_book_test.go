package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"ergo.services/ergo/gen"
)

func TestAddressBookTopology(t *testing.T) {
	book := NewAddressBook()
	if got := book.GetAvailableNodes().Len(); got != 0 {
		t.Fatalf("expected empty topology, got %d nodes", got)
	}
	if got := book.PickNode("worker"); got != "" {
		t.Fatalf("expected no owner, got %s", got)
	}

	if err := book.SetAvailableNodes(NewNodeList("node-b", "", "node-a", "node-a")); err != nil {
		t.Fatalf("SetAvailableNodes failed: %v", err)
	}
	if got := book.GetAvailableNodes().GetAll(); !equalAtoms(got, []gen.Atom{"node-a", "node-b"}) {
		t.Fatalf("unexpected canonical nodes: %v", got)
	}
	owner := book.PickNode("worker")
	if owner == "" || owner != book.PickCoordinatorNode("worker") {
		t.Fatalf("unexpected owner: %s", owner)
	}
	version := book.NodesVersion()

	if err := book.SetAvailableNodes(NewNodeList("node-b", "node-a")); err != nil {
		t.Fatalf("SetAvailableNodes failed: %v", err)
	}
	if book.NodesVersion() != version {
		t.Fatal("equivalent topology changed the version")
	}

	if err := book.SetAvailableNodes(NewNodeList("node-b")); err != nil {
		t.Fatalf("SetAvailableNodes failed: %v", err)
	}
	if got := book.PickNode("worker"); got != "node-b" {
		t.Fatalf("expected node-b, got %s", got)
	}
	if book.NodesVersion() != version+1 {
		t.Fatal("changed topology did not increment the version")
	}

	if err := book.SetAvailableNodes(nil); err != nil {
		t.Fatalf("SetAvailableNodes(nil) failed: %v", err)
	}
	if got := book.GetAvailableNodes().Len(); got != 0 {
		t.Fatalf("expected empty topology, got %d nodes", got)
	}
}

func TestAddressBookLocateFiltersInvalidAndOfflineRoutes(t *testing.T) {
	book := NewAddressBook()
	if _, _, err := book.Locate(context.Background(), "key"); err == nil {
		t.Fatal("expected an unbound locator error")
	}
	if err := book.BindLocator("", func(context.Context, gen.Atom) (gen.PID, bool, error) {
		return gen.PID{}, false, nil
	}); err == nil {
		t.Fatal("expected an empty node error")
	}
	if err := book.BindLocator("node-a", nil); err == nil {
		t.Fatal("expected a nil locator error")
	}

	routes := map[gen.Atom]gen.PID{
		"self":    {Node: "node-a", ID: 1, Creation: 1},
		"remote":  {Node: "node-b", ID: 2, Creation: 1},
		"offline": {Node: "node-c", ID: 3, Creation: 1},
		"invalid": {},
	}
	wantErr := errors.New("lookup failed")
	locator := func(_ context.Context, key gen.Atom) (gen.PID, bool, error) {
		if key == "error" {
			return gen.PID{}, false, wantErr
		}
		pid, found := routes[key]
		return pid, found, nil
	}
	if err := book.BindLocator("node-a", locator); err != nil {
		t.Fatal(err)
	}
	if err := book.SetAvailableNodes(NewNodeList("node-a", "node-b")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []gen.Atom{"self", "remote"} {
		if pid, found, err := book.Locate(context.Background(), key); err != nil || !found || pid != routes[key] {
			t.Fatalf("locate %s: pid=%s found=%v err=%v", key, pid, found, err)
		}
	}
	for _, key := range []gen.Atom{"missing", "offline", "invalid"} {
		if _, found, err := book.Locate(context.Background(), key); err != nil || found {
			t.Fatalf("locate %s: found=%v err=%v", key, found, err)
		}
	}
	if _, _, err := book.Locate(context.Background(), "error"); !errors.Is(err, wantErr) {
		t.Fatalf("expected lookup error, got %v", err)
	}
	if err := book.BindLocator("node-b", locator); err == nil {
		t.Fatal("expected rebinding to another node to fail")
	}
}

func TestAddressBookStableMappingAndLimitedMovement(t *testing.T) {
	book := NewAddressBook()
	if err := book.SetAvailableNodes(NewNodeList("node-a", "node-b", "node-c")); err != nil {
		t.Fatal(err)
	}

	owners := make(map[gen.Atom]gen.Atom, 2000)
	for i := range 2000 {
		key := gen.Atom(fmt.Sprintf("actor-%d", i))
		owner := book.PickNode(key)
		if owner == "" || owner != book.PickNode(key) {
			t.Fatalf("unstable mapping for %s", key)
		}
		owners[key] = owner
	}

	if err := book.SetAvailableNodes(NewNodeList("node-a", "node-b", "node-c", "node-d")); err != nil {
		t.Fatal(err)
	}
	moved := 0
	for key, owner := range owners {
		if book.PickNode(key) != owner {
			moved++
		}
	}
	if moved == 0 || moved >= len(owners)/2 {
		t.Fatalf("unexpected key movement: %d/%d", moved, len(owners))
	}
}

func TestAddressBookConcurrentReadersAndUpdates(t *testing.T) {
	book := NewAddressBook()
	if err := book.SetAvailableNodes(NewNodeList("node-a", "node-b")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range 1000 {
				key := gen.Atom(fmt.Sprintf("actor-%d-%d", worker, i))
				_ = book.PickNode(key)
				_ = book.GetAvailableNodes()
				_ = book.NodesVersion()
			}
		}(worker)
	}
	for i := range 100 {
		nodes := NewNodeList("node-a", "node-b")
		if i%2 == 0 {
			nodes = NewNodeList("node-a", "node-c")
		}
		if err := book.SetAvailableNodes(nodes); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestCanonicalNodesAndNodeSetEquality(t *testing.T) {
	got := canonicalNodes([]gen.Atom{"b", "", "a", "b"})
	if !equalAtoms(got, []gen.Atom{"a", "b"}) {
		t.Fatalf("unexpected canonical nodes: %v", got)
	}
	if !sameNodeSet(map[gen.Atom]struct{}{"a": {}}, map[gen.Atom]struct{}{"a": {}}) {
		t.Fatal("equal sets were not recognized")
	}
	if sameNodeSet(map[gen.Atom]struct{}{"a": {}}, map[gen.Atom]struct{}{"b": {}}) {
		t.Fatal("different sets were considered equal")
	}
}

func equalAtoms(a, b []gen.Atom) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkAddressBookLocateParallel(b *testing.B) {
	book := NewAddressBook()
	pid := gen.PID{Node: "node-b", ID: 42, Creation: 1}
	if err := book.SetAvailableNodes(NewNodeList("node-a", "node-b")); err != nil {
		b.Fatal(err)
	}
	if err := book.BindLocator("node-a", func(context.Context, gen.Atom) (gen.PID, bool, error) {
		return pid, true, nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			located, found, err := book.Locate(context.Background(), "worker")
			if err != nil || !found || located != pid {
				b.Fatalf("unexpected result: pid=%s found=%v err=%v", located, found, err)
			}
		}
	})
}
