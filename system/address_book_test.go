package system

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestAddressBook_Basic(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")

	book.SetAvailableNodes(NewNodeList(node1, node2))

	p1 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 1}}
	p2 := ProcessInfo{Name: "p2", PID: gen.PID{Node: node2, ID: 2}}

	// Test SetProcess
	book.SetProcess(node1, p1)
	book.SetProcess(node2, p2)

	// Test Locate
	if node, ok := book.LocateLocal("p1"); !ok || node != node1 {
		t.Errorf("Locate p1 failed: got %v, %v", node, ok)
	}
	if node, ok := book.LocateLocal("p2"); !ok || node != node2 {
		t.Errorf("Locate p2 failed: got %v, %v", node, ok)
	}
	if _, ok := book.LocateLocal("p3"); ok {
		t.Error("Locate p3 should fail")
	}

	// Test GetProcessList
	list1, err := book.GetProcessList(node1)
	if err != nil {
		t.Fatalf("GetProcessList node1 failed: %v", err)
	}
	if len(list1) != 1 || list1[0].Name != "p1" {
		t.Errorf("GetProcessList node1 failed: got %v", list1)
	}

	// Test RemoveProcess
	book.RemoveProcess(node1, p1)
	if _, ok := book.LocateLocal("p1"); ok {
		t.Errorf("Locate p1 should fail after remove")
	}

	// Test AddProcess
	book.AddProcess(node1, p1)
	if _, ok := book.LocateLocal("p1"); !ok {
		t.Errorf("Locate p1 should succeed after AddProcess")
	}
}

func TestAddressBook_NodeAvailability(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	p1 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 1}}

	book.SetAvailableNodes(NewNodeList(node1))
	book.SetProcess(node1, p1)

	if _, ok := book.LocateLocal("p1"); !ok {
		t.Fatal("Locate p1 failed")
	}
	if nodes := book.GetAvailableNodes(); nodes.Len() != 1 {
		t.Errorf("GetAvailableNodes mismatch: got %v", nodes.GetAll())
	} else if n, ok := nodes.Get(0); !ok || n != node1 {
		t.Errorf("GetAvailableNodes mismatch: got %v", nodes.GetAll())
	}

	// Node goes offline
	book.SetAvailableNodes(NewNodeList())

	if _, ok := book.LocateLocal("p1"); ok {
		t.Error("Locate p1 should fail when node is offline")
	}

	// Node comes back
	book.SetAvailableNodes(NewNodeList(node1))
	// Note: SetAvailableNodes clears processes for removed nodes, so p1 is gone unless re-added.
	if _, ok := book.LocateLocal("p1"); ok {
		t.Error("Locate p1 should still fail (cleared) until re-added")
	}

	book.SetProcess(node1, p1)
	if _, ok := book.LocateLocal("p1"); !ok {
		t.Error("Locate p1 should succeed after re-add")
	}
}

func TestAddressBook_ConsistentHashing(t *testing.T) {
	book := NewAddressBook()

	// Test PickNode with empty nodes
	if n := book.PickNode("any"); n != "" {
		t.Errorf("PickNode should return empty when no nodes available, got %s", n)
	}

	nodes := []gen.Atom{"node1", "node2", "node3"}
	book.SetAvailableNodes(NewNodeList(nodes...))

	// Test PickNode consistency
	target := "my-process"
	nodeA := book.PickNode(gen.Atom(target))
	if nodeA == "" {
		t.Fatal("PickNode returned empty")
	}

	nodeB := book.PickNode(gen.Atom(target))
	if nodeA != nodeB {
		t.Errorf("PickNode inconsistent: %s vs %s", nodeA, nodeB)
	}

	if n := book.PickNode("unknown-key"); n == "" {
		// Should return something as long as nodes are available
		t.Error("PickNode should return a node")
	}

	// Add a node
	book.SetAvailableNodes(NewNodeList(append(nodes, "node4")...))
}

func TestAddressBook_EdgeCases(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	p1 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 1}}

	// AddProcess with empty list
	book.AddProcess(node1)
	if list, err := book.GetProcessList(node1); err != nil || len(list) != 0 {
		t.Error("AddProcess empty should do nothing")
	}

	// RemoveProcess with empty list
	book.RemoveProcess(node1)

	// AddProcess with invalid name (empty)
	book.AddProcess(node1, ProcessInfo{Name: "", PID: gen.PID{Node: node1, ID: 2}})
	if list, err := book.GetProcessList(node1); err != nil || len(list) != 0 {
		t.Error("AddProcess with empty name should be ignored")
	}

	// NOTE: Locate requires the node to be in `AvailableNodes`.
	// In this test, we didn't call SetAvailableNodes.
	// So Locate returns false.
	book.SetAvailableNodes(NewNodeList(node1))

	// RemoveProcess with non-existent node
	book.RemoveProcess("non-existent", p1)

	// Note: previous RemoveProcess("non-existent", p1) might have failed silently but logic in SetAvailableNodes/etc might have side effects?
	// But wait, p1 was added with book.AddProcess(node1, p1).
	// Let's verify p1 exists before the "non-existent" removal.
	book.SetProcess(node1, p1)
	if _, ok := book.LocateLocal("p1"); !ok {
		t.Fatal("p1 missing after SetProcess")
	}

	book.RemoveProcess("non-existent", p1)
	// p1 should still be there
	if _, ok := book.LocateLocal("p1"); !ok {
		t.Fatal("p1 missing after RemoveProcess(non-existent)")
	}

	// RemoveProcess with non-existent process
	// Ensure p1 is really there
	if _, ok := book.LocateLocal("p1"); !ok {
		t.Fatal("p1 missing before RemoveProcess(p2)")
	}
	book.RemoveProcess(node1, ProcessInfo{Name: "p2", PID: gen.PID{Node: node1, ID: 2}})
	if _, ok := book.LocateLocal("p1"); !ok {
		list, _ := book.GetProcessList(node1)
		t.Errorf("RemoveProcess p2 should not affect p1. ProcessList: %v", list)
	}

	// RemoveProcess with correct name but wrong node
	// The logic for RemoveProcess(node, ps...) is:
	// For each p in ps:
	//   name := p.Name
	//   if old, ok := book.nodeProcesses[node][name]; ok && old.Node == node { remove... }
	// So it only checks if a process with `name` exists on `node`.
	// The `PID` in the passed `p` is IGNORED.
	// Therefore, calling RemoveProcess(node1, ProcessInfo{Name: "p1", ...}) WILL remove p1 from node1,
	// regardless of what PID or Node we put in the ProcessInfo argument, as long as Name matches.
	book.RemoveProcess(node1, ProcessInfo{Name: "p1", PID: gen.PID{Node: "other", ID: 1}})
	if _, ok := book.LocateLocal("p1"); ok {
		t.Error("RemoveProcess should have removed p1 because name matches")
	}
}

func TestAddressBook_SetProcess_Update(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	p1 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 1}}
	p2 := ProcessInfo{Name: "p2", PID: gen.PID{Node: node1, ID: 2}}

	book.SetAvailableNodes(NewNodeList(node1))

	// Initial set
	book.SetProcess(node1, p1)
	list, _ := book.GetProcessList(node1)
	if got := findPID(list, gen.Atom("p1")); got.ID != 1 {
		t.Error("p1 ID mismatch")
	}

	// Update set (replace p1 with p2, effectively renaming or changing PID if name same)
	// If we use same name "p1" but different PID
	p1New := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 3}}
	book.SetProcess(node1, p1New)
	list, _ = book.GetProcessList(node1)
	if got := findPID(list, gen.Atom("p1")); got.ID != 3 {
		t.Error("p1 ID should be updated")
	}

	// Replace p1 with p2 (p1 removed, p2 added)
	book.SetProcess(node1, p2)
	if _, ok := book.LocateLocal("p1"); ok {
		t.Error("p1 should be removed")
	}
	if _, ok := book.LocateLocal("p2"); !ok {
		t.Error("p2 should be added")
	}
}

func TestAddressBook_Coverage_Boost(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")
	book.SetAvailableNodes(NewNodeList(node1, node2))

	// 1. Test removeNode with multiple elements and gap closure
	// Pattern: [A, B, A] remove A -> [B]
	list := []gen.Atom{"A", "B", "A"}
	res := removeNode(list, "A")
	if len(res) != 1 || res[0] != "B" {
		t.Errorf("removeNode [A, B, A] -> B failed: got %v", res)
	}

	// 2. Test SetProcess where process exists on multiple nodes (multi-home process?)
	// Technically AddressBook supports it via processToNodes list.
	// p1 on node1 AND node2.
	p1n1 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node1, ID: 1}}
	p1n2 := ProcessInfo{Name: "p1", PID: gen.PID{Node: node2, ID: 2}}

	book.AddProcess(node1, p1n1)
	book.AddProcess(node2, p1n2)

	// processToNodes["p1"] should be [node1, node2]

	// Now remove p1 from node1 using SetProcess (by setting empty list or other process)
	// SetProcess(node1) -> clears node1 processes.
	book.SetProcess(node1)

	// p1 should still exist on node2
	if node, ok := book.LocateLocal("p1"); !ok || node != node2 {
		t.Errorf("p1 should still be on node2, got %v", node)
	}
	// processToNodes["p1"] should now be [node2] (not empty)
	// This exercises the `else` branch of `if len(arr) == 0` in SetProcess/RemoveProcess cleanup.
}

func TestAddressBook_Helpers(t *testing.T) {
	// unifyNodes
	nodes := []gen.Atom{"b", "a", "a", "c"}
	unified := unifyNodes(nodes)
	expected := []gen.Atom{"a", "b", "c"}
	if !reflect.DeepEqual(unified, expected) {
		t.Errorf("unifyNodes failed: got %v, want %v", unified, expected)
	}

	// shortInfo
	ps := []ProcessInfo{
		{Name: "1"}, {Name: "2"}, {Name: "3"}, {Name: "4"},
	}
	s := shortInfo(ps)
	if s != "(1,2,3,...)" {
		t.Errorf("shortInfo failed: got %s", s)
	}
	if shortInfo([]ProcessInfo{}) != "" {
		t.Error("shortInfo empty failed")
	}
}

func TestAddressBook_Internal_Locate(t *testing.T) {
	// Test the loop in locate where node is not found or process not in node
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")
	p1 := gen.Atom("p1")

	// Manually inject state to test edge cases in `locate`
	// Case: processToNodes has node, but node not in `book.nodes`
	book.processToNodes[p1] = []gen.Atom{node1}
	// book.nodes is empty
	if _, ok := book.LocateLocal(p1); ok {
		t.Error("Locate should fail if node not available")
	}

	// Case: node in `book.nodes`, but not in `book.nodeProcesses`
	book.nodes[node1] = struct{}{}
	// book.nodeProcesses empty
	if _, ok := book.LocateLocal(p1); ok {
		t.Error("Locate should fail if nodeProcesses missing")
	}

	// Case: nodeProcesses[node] exists but process not in it
	book.nodeProcesses[node1] = make(map[gen.Atom]ProcessInfo)
	if _, ok := book.LocateLocal(p1); ok {
		t.Error("Locate should fail if process not in nodeProcesses")
	}

	// Case: Multiple nodes for process, first one invalid, second valid
	book.processToNodes[p1] = []gen.Atom{node1, node2}
	// node1 is invalid (already set up as valid node but empty processes)
	// Let's make node1 invalid by removing from available nodes
	delete(book.nodes, node1)

	// Setup node2 as valid
	book.nodes[node2] = struct{}{}
	book.nodeProcesses[node2] = map[gen.Atom]ProcessInfo{
		p1: {Name: p1, Node: node2},
	}

	if node, ok := book.LocateLocal(p1); !ok || node != node2 {
		t.Errorf("Locate should skip invalid node1 and find on node2, got %v", node)
	}
}

func findPID(list ProcessInfoList, name gen.Atom) gen.PID {
	for _, item := range list {
		if item.Name == name {
			return item.PID
		}
	}
	return gen.PID{}
}

func TestAddressBook_SetAvailableNodes_MultiNode(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")
	p1 := gen.Atom("p1")

	// Setup: p1 exists on both node1 and node2
	book.SetAvailableNodes(NewNodeList(node1, node2))

	p1Info1 := ProcessInfo{Name: p1, PID: gen.PID{Node: node1, ID: 1}}
	p1Info2 := ProcessInfo{Name: p1, PID: gen.PID{Node: node2, ID: 2}}

	book.AddProcess(node1, p1Info1)
	book.AddProcess(node2, p1Info2)

	// Verify p1 is on both
	if len(book.processToNodes[p1]) != 2 {
		t.Fatal("p1 should be on 2 nodes")
	}

	// Remove node1 via SetAvailableNodes
	book.SetAvailableNodes(NewNodeList(node2))

	// Check p1 is still in processToNodes (pointing to node2)
	// This exercises the `else` branch in SetAvailableNodes cleanup loop
	nodes, ok := book.processToNodes[p1]
	if !ok {
		t.Fatal("p1 should still be in processToNodes")
	}
	if len(nodes) != 1 || nodes[0] != node2 {
		t.Errorf("p1 should be only on node2, got %v", nodes)
	}
}

func TestAddressBook_RemoveProcess_InconsistentState(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	p1 := gen.Atom("p1")

	book.SetAvailableNodes(NewNodeList(node1))

	// Manually inject inconsistent state:
	// Process "p1" is recorded in nodeProcesses[node1], but the ProcessInfo says it's on "otherNode"
	// This shouldn't happen normally, but RemoveProcess has a check `old.Node == node`
	book.nodeProcesses[node1] = map[gen.Atom]ProcessInfo{
		p1: {Name: p1, PID: gen.PID{Node: "otherNode", ID: 1}},
	}
	// Also set reverse index so we can verify it wasn't removed
	book.processToNodes[p1] = []gen.Atom{node1}

	// Try to remove p1 from node1
	// The check `old.Node == node` ( "otherNode" == "node1" ) should fail, so it should NOT remove it.
	book.RemoveProcess(node1, ProcessInfo{Name: p1})

	if _, ok := book.nodeProcesses[node1][p1]; !ok {
		t.Error("RemoveProcess should not have removed p1 because Node mismatch")
	}

	// RemoveProcess with empty name in ProcessInfo
	// Should just skip it
	book.RemoveProcess(node1, ProcessInfo{Name: "", PID: gen.PID{Node: node1, ID: 999}})
	// No panic, no side effect
}

func TestAddressBook_RemoveProcess_MultiNode(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")
	p1 := gen.Atom("p1")

	// Setup: p1 exists on both node1 and node2
	book.SetAvailableNodes(NewNodeList(node1, node2))

	p1Info1 := ProcessInfo{Name: p1, PID: gen.PID{Node: node1, ID: 1}}
	p1Info2 := ProcessInfo{Name: p1, PID: gen.PID{Node: node2, ID: 2}}

	book.AddProcess(node1, p1Info1)
	book.AddProcess(node2, p1Info2)

	// Remove p1 from node1 ONLY
	book.RemoveProcess(node1, p1Info1)

	// Verify p1 is still in processToNodes (pointing to node2)
	// This exercises the `else` branch in RemoveProcess cleanup loop
	nodes, ok := book.processToNodes[p1]
	if !ok {
		t.Fatal("p1 should still be in processToNodes")
	}
	if len(nodes) != 1 || nodes[0] != node2 {
		t.Errorf("p1 should be only on node2, got %v", nodes)
	}

	// Verify p1 removed from node1 processes
	if _, ok := book.nodeProcesses[node1][p1]; ok {
		t.Error("p1 should be removed from node1")
	}
}

func TestAddressBook_Concurrency(t *testing.T) {
	book := NewAddressBook()
	node1 := gen.Atom("node1")
	node2 := gen.Atom("node2")
	book.SetAvailableNodes(NewNodeList(node1, node2))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: SetProcess
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				i++
				name := gen.Atom("p" + strconv.Itoa(i%100))
				book.SetProcess(node1, ProcessInfo{Name: name, PID: gen.PID{Node: node1, ID: 1}})
				// Occasional yield
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}
	}()

	// Writer: SetAvailableNodes (Topology change)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				// Toggle node2 availability
				if book.GetAvailableNodes().Len() == 2 {
					book.SetAvailableNodes(NewNodeList(node1))
				} else {
					book.SetAvailableNodes(NewNodeList(node1, node2))
				}
			}
		}
	}()

	// Reader: Locate & PickNode
	for k := 0; k < 5; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					i++
					name := gen.Atom("p" + strconv.Itoa(i%100))
					book.LocateLocal(name)
					book.PickNode(name)
					book.GetProcessList(node1)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
