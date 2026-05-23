package system_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/app"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

var nodeSeq int64
var procSeq int64

func uniqueNodeName(base string) string {
	seq := atomic.AddInt64(&nodeSeq, 1)
	parts := strings.SplitN(base, "@", 2)
	if len(parts) != 2 {
		return fmt.Sprintf("%s-%d", base, seq)
	}
	return fmt.Sprintf("%s-%d@%s", parts[0], seq, parts[1])
}

func uniqueProcessName(base string) gen.Atom {
	seq := atomic.AddInt64(&procSeq, 1)
	return gen.Atom(fmt.Sprintf("%s.%d", base, seq))
}

type testProc struct{ act.Actor }

func (p *testProc) Init(args ...any) error { return nil }

func (p *testProc) HandleMessage(from gen.PID, message any) error { return nil }

func startNode(t *testing.T, cluster *mem.Cluster, name string) app.Node {
	t.Helper()
	name = uniqueNodeName(name)
	return startNodeExact(t, cluster, name)
}

func startNodeExact(t *testing.T, cluster *mem.Cluster, name string) app.Node {
	t.Helper()
	return startNodeExactWithSyncInterval(t, cluster, name, 50*time.Millisecond)
}

func startNodeExactWithSyncInterval(t *testing.T, cluster *mem.Cluster, name string, syncInterval time.Duration) app.Node {
	t.Helper()
	n, err := app.StartSimpleNode(app.SimpleNodeOptions{
		NodeName:                 name,
		Port:                     0,
		Cookie:                   "whereis-test-cookie",
		Registrar:                mem.CreateWithCluster(cluster),
		WhereIsOptions:           system.WhereIsOptions{SyncInterval: syncInterval},
		PlacementMonitorInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start node %s: %v", name, err)
	}
	t.Cleanup(func() {
		n.Stop()
		_ = n.WaitWithTimeout(3 * time.Second)
		n.StopForce()
		_ = n.WaitWithTimeout(3 * time.Second)
	})
	return n
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout after %s", timeout)
}

func waitForClusterNodes(t *testing.T, timeout time.Duration, nodes ...app.Node) {
	t.Helper()
	waitUntil(t, timeout, func() bool {
		for _, n := range nodes {
			available := n.AddressBook().GetAvailableNodes()
			if available.Len() != len(nodes) {
				return false
			}
			for _, expected := range nodes {
				if !available.Exist(expected.Name()) {
					return false
				}
			}
		}
		return true
	})
}

func spawnNamed(t *testing.T, n app.Node, name gen.Atom) gen.PID {
	t.Helper()
	return spawnNamedWithBirthAt(t, n, name, 0)
}

func spawnNamedWithBirthAt(t *testing.T, n app.Node, name gen.Atom, birthAt int64) gen.PID {
	t.Helper()
	pid, err := n.SpawnRegister(name, func() gen.ProcessBehavior { return &testProc{} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn %s on %s: %v", name, n.Name(), err)
	}
	if err := n.Send(system.WhereIsProcess, system.MessageRegisterLocalProcess{Name: name, PID: pid, BirthAt: birthAt}); err != nil {
		t.Fatalf("register %s on %s: %v", name, n.Name(), err)
	}
	return pid
}

func killAndWaitPID(t *testing.T, n app.Node, pid gen.PID) {
	t.Helper()
	_ = n.Kill(pid)
	if err := n.WaitPID(pid); err != nil && !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("wait for killed pid %s on %s: %v", pid, n.Name(), err)
	}
}

func locateNode(n app.Node, name gen.Atom) (gen.Atom, bool) {
	node := n.LocateProcess(name)
	return node, node != ""
}

func nodeMin(a, b gen.Atom) gen.Atom {
	if a < b {
		return a
	}
	return b
}

func TestWhereisConvergesOnJoin(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2)

	nameA := uniqueProcessName("proc.A")
	_ = spawnNamed(t, n1, nameA)

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n2, nameA)
		return ok && node == n1.Name()
	})

	nameB := uniqueProcessName("proc.B")
	_ = spawnNamed(t, n2, nameB)

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n1, nameB)
		return ok && node == n2.Name()
	})
}

func TestWhereisFastUnregisterRemovesRouteBeforeSlowScan(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNodeExactWithSyncInterval(t, cluster, uniqueNodeName("node-a@127.0.0.1"), 5*time.Second)
	defer n1.Stop()
	n2 := startNodeExactWithSyncInterval(t, cluster, uniqueNodeName("node-b@127.0.0.1"), 5*time.Second)
	defer n2.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2)

	name := uniqueProcessName("proc.fast-unregister")
	pid := spawnNamed(t, n2, name)

	waitUntil(t, 30*time.Second, func() bool {
		node, ok := locateNode(n1, name)
		return ok && node == n2.Name()
	})

	if err := n2.Send(system.WhereIsProcess, system.MessageUnregisterLocalProcess{Name: name, PID: pid}); err != nil {
		t.Fatalf("fast unregister %s on %s: %v", name, n2.Name(), err)
	}
	killAndWaitPID(t, n2, pid)

	waitUntil(t, 4*time.Second, func() bool {
		_, ok := locateNode(n1, name)
		return !ok
	})
}

func TestWhereisRemovesProcessesOnNodeLeave(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2)

	name := uniqueProcessName("proc.leave")
	_ = spawnNamed(t, n2, name)

	waitUntil(t, 30*time.Second, func() bool {
		node, ok := locateNode(n1, name)
		return ok && node == n2.Name()
	})

	n2.Stop()
	_ = n2.WaitWithTimeout(3 * time.Second)

	waitUntil(t, 30*time.Second, func() bool {
		_, ok := locateNode(n1, name)
		if ok {
			return false
		}
		if n1.AddressBook().GetAvailableNodes().Exist(n2.Name()) {
			return false
		}
		return true
	})
}

func TestWhereisDuplicateNameDeterministicWinnerAndFailover(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()
	n3 := startNode(t, cluster, "node-c@127.0.0.1")
	defer n3.Stop()

	dup := uniqueProcessName("proc.dup")
	waitForClusterNodes(t, 10*time.Second, n1, n2, n3)
	pid2 := spawnNamedWithBirthAt(t, n2, dup, 1)
	pid3 := spawnNamedWithBirthAt(t, n3, dup, 2)

	// n2's process is definitively older, so locate() will always select n2
	// as the winner via BirthAt.
	winner := n2.Name()

	waitWinner := func(n app.Node) bool {
		_ = n2.Send(system.WhereIsProcess, system.MessageRegisterLocalProcess{Name: dup, PID: pid2, BirthAt: 1})
		_ = n3.Send(system.WhereIsProcess, system.MessageRegisterLocalProcess{Name: dup, PID: pid3, BirthAt: 2})
		node, ok := locateNode(n, dup)
		return ok && node == winner
	}

	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n1) })
	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n2) })
	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n3) })

	// winner is always n2; kill n2's process and expect n3 to take over.
	killAndWaitPID(t, n2, pid2)
	loserNode := n3

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n1, dup)
		return ok && node == loserNode.Name()
	})
}

func TestWhereisDuplicateNameOldestWins(t *testing.T) {
	book := system.NewAddressBook()
	n2 := gen.Atom("node-b@127.0.0.1")
	n3 := gen.Atom("node-c@127.0.0.1")
	dup := uniqueProcessName("proc.dup.oldest")

	book.SetAvailableNodes(system.NewNodeList(n2, n3))
	if err := book.AddProcess(n3, system.ProcessInfo{Name: dup, PID: gen.PID{Node: n3, ID: 3}, Node: n3, BirthAt: 2}); err != nil {
		t.Fatalf("add newer process: %v", err)
	}
	if err := book.AddProcess(n2, system.ProcessInfo{Name: dup, PID: gen.PID{Node: n2, ID: 2}, Node: n2, BirthAt: 1}); err != nil {
		t.Fatalf("add older process: %v", err)
	}

	node, ok := book.LocateLocal(dup)
	if !ok || node != n2 {
		t.Fatalf("expected oldest node %s, got %s (ok=%v)", n2, node, ok)
	}
}

func TestWhereisDuplicateNameTieBreakStable(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()
	n3 := startNode(t, cluster, "node-c@127.0.0.1")
	defer n3.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2, n3)

	dup := uniqueProcessName("proc.dup.tie")
	winner := nodeMin(n2.Name(), n3.Name())

	pid2 := spawnNamedWithBirthAt(t, n2, dup, 1)
	pid3 := spawnNamedWithBirthAt(t, n3, dup, 1)
	defer func() {
		killAndWaitPID(t, n2, pid2)
		killAndWaitPID(t, n3, pid3)
	}()

	waitUntil(t, 60*time.Second, func() bool {
		// In sharded model, processList might not work as before because it syncs ONLY to owner.
		// But here we can still check if it's found globally.
		node, ok := locateNode(n1, dup)
		return ok && (node == n2.Name() || node == n3.Name())
	})

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n1, dup)
		return ok && node == winner
	})

	for i := 0; i < 10; i++ {
		node, ok := locateNode(n1, dup)
		if !ok || node != winner {
			t.Fatalf("expected stable winner %s, got %s (ok=%v)", winner, node, ok)
		}
	}
}

func TestWhereisConvergesAfterManyLocalChanges(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2)

	var pids []gen.PID
	var names []gen.Atom
	for i := 0; i < 25; i++ {
		name := uniqueProcessName(fmt.Sprintf("proc.bulk.%02d", i))
		names = append(names, name)
		pids = append(pids, spawnNamed(t, n1, name))
	}

	for i := 0; i < len(pids); i += 2 {
		killAndWaitPID(t, n1, pids[i])
	}

	expected := make(map[gen.Atom]struct{})
	for i := 1; i < len(names); i += 2 {
		expected[names[i]] = struct{}{}
	}

	waitUntil(t, 60*time.Second, func() bool {
		for name := range expected {
			node, ok := locateNode(n2, name)
			if !ok || node != n1.Name() {
				return false
			}
		}
		for i := 0; i < len(names); i += 2 {
			if _, ok := locateNode(n2, names[i]); ok {
				return false
			}
		}
		return true
	})
}

func TestWhereisClearsStaleOwnerStateAfterTopologyRebalance(t *testing.T) {
	cluster := mem.NewCluster()

	n1 := startNodeExact(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNodeExact(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()
	n3 := startNodeExact(t, cluster, "node-c@127.0.0.1")
	defer n3.Stop()
	n4 := startNodeExact(t, cluster, "node-d@127.0.0.1")
	defer n4.Stop()
	n5 := startNodeExact(t, cluster, "node-e@127.0.0.1")
	defer n5.Stop()

	waitForClusterNodes(t, 10*time.Second, n1, n2, n3, n4, n5)

	n6Name := "node-f@127.0.0.1"
	n6 := startNodeExact(t, cluster, n6Name)
	defer n6.Stop()

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 6 &&
			n6.AddressBook().GetAvailableNodes().Len() == 6
	})

	ownerOnSix := make(map[gen.Atom]gen.Atom)
	rebalancePrefix := fmt.Sprintf("proc.rebalance.%d", atomic.AddInt64(&procSeq, 1))
	for i := 0; i < 5000; i++ {
		name := gen.Atom(fmt.Sprintf("%s.%04d", rebalancePrefix, i))
		ownerOnSix[name] = n1.AddressBook().PickDirectoryNode(name)
	}

	n6.Stop()
	_ = n6.WaitWithTimeout(3 * time.Second)

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 5 &&
			!n1.AddressBook().GetAvailableNodes().Exist(gen.Atom(n6Name))
	})

	var target gen.Atom
	for i := 0; i < 5000; i++ {
		name := gen.Atom(fmt.Sprintf("%s.%04d", rebalancePrefix, i))
		if ownerOnFive := n1.AddressBook().PickDirectoryNode(name); ownerOnFive != "" && ownerOnFive != ownerOnSix[name] {
			target = name
			break
		}
	}
	if target == "" {
		t.Fatal("failed to find a process name whose directory owner changes across rebalance")
	}

	pid := spawnNamed(t, n1, target)
	waitUntil(t, 30*time.Second, func() bool {
		node, ok := locateNode(n2, target)
		return ok && node == n1.Name()
	})

	n6 = startNodeExact(t, cluster, n6Name)
	defer n6.Stop()

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 6 &&
			n1.AddressBook().PickDirectoryNode(target) == ownerOnSix[target]
	})

	waitUntil(t, 30*time.Second, func() bool {
		node, ok := locateNode(n3, target)
		return ok && node == n1.Name()
	})

	killAndWaitPID(t, n1, pid)
	waitUntil(t, 30*time.Second, func() bool {
		_, ok := locateNode(n4, target)
		return !ok
	})

	n6.Stop()
	_ = n6.WaitWithTimeout(3 * time.Second)

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 5 &&
			n1.AddressBook().PickDirectoryNode(target) != ownerOnSix[target]
	})

	waitUntil(t, 30*time.Second, func() bool {
		_, ok := locateNode(n5, target)
		return !ok
	})
}
