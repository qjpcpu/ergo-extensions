package system_test

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/app"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

type mockLauncher struct {
	processes []system.DaemonProcess
}

func (m *mockLauncher) Scan() ([]system.DaemonProcess, bool, error) {
	return m.processes, false, nil
}

func TestDaemonLeaderRecovery(t *testing.T) {
	cluster := mem.NewCluster()

	// Register a mock launcher
	launcherName := gen.Atom("mock_launcher")
	daemonName := gen.Atom("mock_daemon")
	mock := &mockLauncher{
		processes: []system.DaemonProcess{
			{ProcessName: daemonName},
		},
	}

	system.RegisterLauncher(launcherName, system.Launcher{
		Factory: func() gen.ProcessBehavior {
			return &testProc{}
		},
		RecoveryScanner: func() system.DaemonIterator {
			return mock.Scan
		},
	})
	t.Cleanup(func() {
		system.UnregisterLauncher(launcherName)
	})

	// Start two nodes
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	// Wait for convergence and leader election
	// Node A should be leader because it was started first (seq 1)
	waitUntil(t, 5*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 2
	})

	// The daemon process should be started by the leader (node-a)
	// We wait for some time because launchAllAfter has a 10s delay + jitter
	// For testing, we might want to trigger it faster or just wait.
	// Since I can't easily change the 10s delay in daemon_process.go without editing it, 
	// I'll wait up to 15s.
	
	t.Log("Waiting for daemon to be launched...")
	waitUntil(t, 20*time.Second, func() bool {
		node, ok := locateNode(n1, daemonName)
		if ok {
			t.Logf("Daemon located on node %s", node)
		}
		return ok
	})

	// Now stop the node where the daemon is running
	daemonNodeName, _ := locateNode(n1, daemonName)
	var remainingNode app.Node
	if daemonNodeName == n1.Name() {
		t.Log("Stopping node-a (leader)")
		n1.Stop()
		remainingNode = n2
	} else {
		t.Log("Stopping node-b")
		n2.Stop()
		remainingNode = n1
	}

	// If leader (n1) stopped, n2 should become new leader and recover daemon
	// If follower (n2) stopped, n1 (leader) should detect it and recover daemon
	
	t.Log("Waiting for daemon to be recovered...")
	waitUntil(t, 30*time.Second, func() bool {
		_, ok := locateNode(remainingNode, daemonName)
		return ok
	})

	// Test HandleCall/Inspect
	res, err := remainingNode.CallLocal(string(system.DaemonMonitorProcess), "inspect")
	if err != nil {
		t.Errorf("Daemon inspect failed: %v", err)
	} else {
		m, ok := res.(map[string]string)
		if !ok {
			t.Errorf("Daemon inspect returned unexpected type: %T", res)
		} else {
			if m["daemons"] == "" {
				t.Error("Daemon inspect returned empty daemons list")
			}
			t.Logf("Daemon stats: %v", m)
		}
	}
}
