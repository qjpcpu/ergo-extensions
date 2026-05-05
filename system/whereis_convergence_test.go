package system_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/app"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
)

var nodeSeq int64

func uniqueNodeName(base string) string {
	seq := atomic.AddInt64(&nodeSeq, 1)
	parts := strings.SplitN(base, "@", 2)
	if len(parts) != 2 {
		return fmt.Sprintf("%s-%d", base, seq)
	}
	return fmt.Sprintf("%s-%d@%s", parts[0], seq, parts[1])
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
	n, err := app.StartSimpleNode(app.SimpleNodeOptions{
		NodeName:            name,
		Port:                0,
		Cookie:              "whereis-test-cookie",
		Registrar:           mem.CreateWithCluster(cluster),
		SyncProcessInterval: 50 * time.Millisecond,
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

func spawnNamed(t *testing.T, n app.Node, name gen.Atom) gen.PID {
	t.Helper()
	pid, err := n.SpawnRegister(name, func() gen.ProcessBehavior { return &testProc{} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn %s on %s: %v", name, n.Name(), err)
	}
	return pid
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

	// Wait for nodes to discover each other before spawning processes
	waitUntil(t, 10*time.Second, func() bool {
		nodes1 := n1.AddressBook().GetAvailableNodes()
		nodes2 := n2.AddressBook().GetAvailableNodes()
		return nodes1.Len() == 2 && nodes2.Len() == 2 &&
			nodes1.Exist(n1.Name()) && nodes1.Exist(n2.Name()) &&
			nodes2.Exist(n1.Name()) && nodes2.Exist(n2.Name())
	})

	nameA := gen.Atom("proc.A")
	_ = spawnNamed(t, n1, nameA)

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n2, nameA)
		return ok && node == n1.Name()
	})

	nameB := gen.Atom("proc.B")
	_ = spawnNamed(t, n2, nameB)

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n1, nameB)
		return ok && node == n2.Name()
	})
}

func TestWhereisRemovesProcessesOnNodeLeave(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()

	name := gen.Atom("proc.leave")
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

	dup := gen.Atom("proc.dup")
	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 3 &&
			n2.AddressBook().GetAvailableNodes().Len() == 3 &&
			n3.AddressBook().GetAvailableNodes().Len() == 3
	})
	pid2 := spawnNamed(t, n2, dup)
	time.Sleep(1200 * time.Millisecond)
	_ = spawnNamed(t, n3, dup)

	// n2's process is definitively older (spawned 1.2s before n3's),
	// so locate() will always select n2 as the winner via BirthAt.
	winner := n2.Name()

	waitWinner := func(n app.Node) bool {
		node, ok := locateNode(n, dup)
		return ok && node == winner
	}

	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n1) })
	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n2) })
	waitUntil(t, 60*time.Second, func() bool { return waitWinner(n3) })

	// winner is always n2; kill n2's process and expect n3 to take over.
	_ = n2.Kill(pid2)
	loserNode := n3

	waitUntil(t, 60*time.Second, func() bool {
		node, ok := locateNode(n1, dup)
		return ok && node == loserNode.Name()
	})
}

func TestWhereisDuplicateNameOldestWins(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()
	n3 := startNode(t, cluster, "node-c@127.0.0.1")
	defer n3.Stop()

	// Wait for nodes to discover each other
	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 3 &&
			n2.AddressBook().GetAvailableNodes().Len() == 3 &&
			n3.AddressBook().GetAvailableNodes().Len() == 3
	})

	dup := gen.Atom("proc.dup.oldest")
	pid2 := spawnNamed(t, n2, dup)
	time.Sleep(1200 * time.Millisecond)
	pid3 := spawnNamed(t, n3, dup)

	// Add a small delay to allow directory owner to stabilize
	time.Sleep(200 * time.Millisecond)

	waitUntil(t, 30*time.Second, func() bool { // Reduced timeout - if it takes longer, there's likely an issue
		node, ok := locateNode(n1, dup)
		return ok && node == n2.Name()
	})

	_ = n2.Kill(pid2)
	waitUntil(t, 30*time.Second, func() bool { // Reduced timeout
		node, ok := locateNode(n1, dup)
		return ok && node == n3.Name()
	})

	_ = n3.Kill(pid3)
}

func TestWhereisDuplicateNameTieBreakStable(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()
	n3 := startNode(t, cluster, "node-c@127.0.0.1")
	defer n3.Stop()

	dup := gen.Atom("proc.dup.tie")
	winner := nodeMin(n2.Name(), n3.Name())

	try := func() bool {
		now := time.Now()
		next := time.Unix(now.Unix()+1, 0).Add(20 * time.Millisecond)
		time.Sleep(time.Until(next))

		var (
			pid2 gen.PID
			pid3 gen.PID
			err2 error
			err3 error
		)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			pid2, err2 = n2.SpawnRegister(dup, func() gen.ProcessBehavior { return &testProc{} }, gen.ProcessOptions{})
		}()
		go func() {
			defer wg.Done()
			pid3, err3 = n3.SpawnRegister(dup, func() gen.ProcessBehavior { return &testProc{} }, gen.ProcessOptions{})
		}()
		wg.Wait()
		if err2 != nil || err3 != nil {
			return false
		}
		defer func() {
			_ = n2.Kill(pid2)
			_ = n3.Kill(pid3)
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
				return false
			}
		}

		return true
	}

	ok := false
	for i := 0; i < 5; i++ {
		if try() {
			ok = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("failed to reproduce tie BirthAt across nodes")
	}
}

func TestWhereisConvergesAfterManyLocalChanges(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	defer n1.Stop()
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	defer n2.Stop()

	waitUntil(t, 5*time.Second, func() bool {
		n1Nodes := n1.AddressBook().GetAvailableNodes()
		n2Nodes := n2.AddressBook().GetAvailableNodes()
		return n1Nodes.Exist(n1.Name()) &&
			n1Nodes.Exist(n2.Name()) &&
			n2Nodes.Exist(n1.Name()) &&
			n2Nodes.Exist(n2.Name())
	})

	var pids []gen.PID
	var names []gen.Atom
	for i := 0; i < 25; i++ {
		name := gen.Atom(fmt.Sprintf("proc.bulk.%02d", i))
		names = append(names, name)
		pids = append(pids, spawnNamed(t, n1, name))
	}

	for i := 0; i < len(pids); i += 2 {
		_ = n1.Kill(pids[i])
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

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 5 &&
			n2.AddressBook().GetAvailableNodes().Len() == 5 &&
			n3.AddressBook().GetAvailableNodes().Len() == 5 &&
			n4.AddressBook().GetAvailableNodes().Len() == 5 &&
			n5.AddressBook().GetAvailableNodes().Len() == 5
	})

	n6Name := "node-f@127.0.0.1"
	n6 := startNodeExact(t, cluster, n6Name)
	defer n6.Stop()

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 6 &&
			n6.AddressBook().GetAvailableNodes().Len() == 6
	})

	ownerOnSix := make(map[gen.Atom]gen.Atom)
	for i := 0; i < 5000; i++ {
		name := gen.Atom(fmt.Sprintf("proc.rebalance.%04d", i))
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
		name := gen.Atom(fmt.Sprintf("proc.rebalance.%04d", i))
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

	_ = n1.Kill(pid)
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
