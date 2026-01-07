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
	"github.com/qjpcpu/ergo-extensions/system"
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
	n, err := app.StartSimpleNode(app.SimpleNodeOptions{
		NodeName:              name,
		Port:                  0,
		Cookie:                "whereis-test-cookie",
		Registrar:             mem.CreateWithCluster(cluster),
		NodeForwardWorker:     1,
		SyncProcessInterval:   50 * time.Millisecond,
		ProcessChangeBuffer:   16,
		DefaultRequestTimeout: 3,
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

func locateNode(book system.IAddressBook, name gen.Atom) (gen.Atom, bool) {
	if node, ok := book.Locate(name); ok {
		return node, true
	}
	return "", false
}

func processList(book system.IAddressBook, node gen.Atom) system.ProcessInfoList {
	if b, ok := book.(interface {
		GetProcessList(gen.Atom) system.ProcessInfoList
	}); ok {
		return b.GetProcessList(node)
	}
	return nil
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
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	nameA := gen.Atom("proc.A")
	_ = spawnNamed(t, n1, nameA)

	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n2.AddressBook(), nameA)
		return ok && node == n1.Name()
	})

	nameB := gen.Atom("proc.B")
	_ = spawnNamed(t, n2, nameB)

	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n1.AddressBook(), nameB)
		return ok && node == n2.Name()
	})
}

func TestWhereisRemovesProcessesOnNodeLeave(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	name := gen.Atom("proc.leave")
	_ = spawnNamed(t, n2, name)

	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n1.AddressBook(), name)
		return ok && node == n2.Name()
	})

	n2.Stop()
	_ = n2.WaitWithTimeout(3 * time.Second)

	waitUntil(t, 5*time.Second, func() bool {
		_, ok := locateNode(n1.AddressBook(), name)
		if ok {
			return false
		}
		for _, n := range n1.AddressBook().GetAvailableNodes() {
			if n == n2.Name() {
				return false
			}
		}
		return true
	})
}

func TestWhereisDuplicateNameDeterministicWinnerAndFailover(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	n3 := startNode(t, cluster, "node-c@127.0.0.1")

	dup := gen.Atom("proc.dup")
	pid2 := spawnNamed(t, n2, dup)
	pid3 := spawnNamed(t, n3, dup)

	winner := nodeMin(n2.Name(), n3.Name())

	waitWinner := func(book system.IAddressBook) bool {
		node, ok := locateNode(book, dup)
		return ok && node == winner
	}

	waitUntil(t, 5*time.Second, func() bool { return waitWinner(n1.AddressBook()) })
	waitUntil(t, 5*time.Second, func() bool { return waitWinner(n2.AddressBook()) })
	waitUntil(t, 5*time.Second, func() bool { return waitWinner(n3.AddressBook()) })

	var loserNode app.Node
	if winner == n2.Name() {
		_ = n2.Kill(pid2)
		loserNode = n3
	} else {
		_ = n3.Kill(pid3)
		loserNode = n2
	}

	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n1.AddressBook(), dup)
		return ok && node == loserNode.Name()
	})
}

func TestWhereisDuplicateNameOldestWins(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	n3 := startNode(t, cluster, "node-c@127.0.0.1")

	dup := gen.Atom("proc.dup.oldest")
	pid2 := spawnNamed(t, n2, dup)
	time.Sleep(1200 * time.Millisecond)
	pid3 := spawnNamed(t, n3, dup)

	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n1.AddressBook(), dup)
		return ok && node == n2.Name()
	})

	_ = n2.Kill(pid2)
	waitUntil(t, 5*time.Second, func() bool {
		node, ok := locateNode(n1.AddressBook(), dup)
		return ok && node == n3.Name()
	})

	_ = n3.Kill(pid3)
}

func TestWhereisDuplicateNameTieBreakStable(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")
	n3 := startNode(t, cluster, "node-c@127.0.0.1")

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

		waitUntil(t, 5*time.Second, func() bool {
			l2 := processList(n1.AddressBook(), n2.Name())
			l3 := processList(n1.AddressBook(), n3.Name())
			has2 := false
			has3 := false
			for _, p := range l2 {
				if p.Name == dup {
					has2 = true
				}
			}
			for _, p := range l3 {
				if p.Name == dup {
					has3 = true
				}
			}
			return has2 && has3
		})

		var b2, b3 int64
		for _, p := range processList(n1.AddressBook(), n2.Name()) {
			if p.Name == dup {
				b2 = p.BirthAt
			}
		}
		for _, p := range processList(n1.AddressBook(), n3.Name()) {
			if p.Name == dup {
				b3 = p.BirthAt
			}
		}
		if b2 == 0 || b3 == 0 || b2 != b3 {
			return false
		}

		waitUntil(t, 5*time.Second, func() bool {
			node, ok := locateNode(n1.AddressBook(), dup)
			return ok && node == winner
		})

		for i := 0; i < 10; i++ {
			node, ok := locateNode(n1.AddressBook(), dup)
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
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

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

	waitUntil(t, 10*time.Second, func() bool {
		for name := range expected {
			node, ok := locateNode(n2.AddressBook(), name)
			if !ok || node != n1.Name() {
				return false
			}
		}
		for i := 0; i < len(names); i += 2 {
			if _, ok := locateNode(n2.AddressBook(), names[i]); ok {
				return false
			}
		}
		return true
	})
}
