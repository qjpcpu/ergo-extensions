package system_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/app"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
	"github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

func TestDaemonRecoveryPullsAcrossNodes(t *testing.T) {
	const pageSize = 100
	for _, tc := range []struct {
		total, limit int
		initDelay    time.Duration
	}{
		{1000, 64, 0}, {100, 64, 200 * time.Millisecond}, {20, 4, 0},
	} {
		t.Run(fmt.Sprintf("tasks-%d-limit-%d", tc.total, tc.limit), func(t *testing.T) {
			ready, release := make(chan struct{}), make(chan struct{})
			var readyOnce, releaseOnce sync.Once
			publish := func() { readyOnce.Do(func() { close(ready) }) }
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer publish()
			defer unblock()
			entered := make(chan gen.Atom, tc.total)
			var active, peak, completed, scans, pages atomic.Int32
			var processes []system.DaemonProcess
			launcher := gen.Atom(t.Name())
			if err := system.RegisterLauncher(launcher, system.Launcher{
				Factory: func() gen.ProcessBehavior {
					return &pullRecoveryProc{onInit: func(node gen.Atom) {
						n := active.Add(1)
						for old := peak.Load(); n > old; old = peak.Load() {
							if peak.CompareAndSwap(old, n) {
								break
							}
						}
						entered <- node
						<-release
						if tc.initDelay > 0 {
							time.Sleep(tc.initDelay)
						}
						active.Add(-1)
						completed.Add(1)
					}}
				},
				RecoveryScanner: func() system.DaemonIterator {
					<-ready
					scans.Add(1)
					offset := 0
					return func() ([]system.DaemonProcess, bool, error) {
						pages.Add(1)
						end := min(offset+pageSize, len(processes))
						page := processes[offset:end]
						offset = end
						return page, offset < len(processes), nil
					}
				},
			}); err != nil {
				t.Fatal(err)
			}
			defer system.UnregisterLauncher(launcher)
			cluster := mem.NewCluster()
			store := system.NewMemoryActorRoutePersistence()
			start := func(name string, limit int) app.Node {
				opts := system.DefaultDaemonOptions()
				opts.MaxInFlight = limit
				opts.InitialRecoveryDelay, opts.LeaderRecoveryDelay, opts.NodeLeftRecoveryDelay = time.Hour, time.Hour, time.Hour
				opts.FullRecoveryInterval, opts.RetryMaxDelay = time.Hour, time.Hour
				opts.RecoveryJitterMax = -1
				opts.LaunchTimeout = time.Second
				node, err := app.StartSimpleNode(app.SimpleNodeOptions{
					NodeName: uniqueNodeName(name), Cookie: "pull-recovery",
					Registrar: mem.CreateWithCluster(cluster), ActorRoutePersistence: store,
					DaemonOptions: opts, LogLevel: gen.LogLevelDisabled,
					MembershipOptions: system.MembershipOptions{RefreshInterval: 50 * time.Millisecond},
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { publish(); unblock(); node.Stop(); node.WaitWithTimeout(3 * time.Second) })
				return node
			}
			source := start("pull-source@127.0.0.1", tc.limit)
			target := start("pull-target@127.0.0.1", 4)
			stats := func(node app.Node) map[string]string {
				pid, err := node.ProcessPID("extensions_daemon")
				if err != nil {
					return nil
				}
				result, _ := node.Inspect(pid)
				return result
			}
			waitUntil(t, 5*time.Second, func() bool {
				return source.Topology().GetAvailableNodes().Len() == 2 && target.Topology().GetAvailableNodes().Len() == 2 && stats(source)["is_leader"] == "true"
			})
			for i := 0; len(processes) < tc.total; i++ {
				name := gen.Atom(fmt.Sprintf("pulled-%d", i))
				if source.Topology().PickNode(name) == target.Name() {
					processes = append(processes, system.DaemonProcess{ProcessName: name})
				}
			}
			publish()
			if err := source.Send(gen.Atom("extensions_daemon"), core.MessageLaunchAllDaemon{}); err != nil {
				t.Fatal(err)
			}
			workers := min(8, tc.limit)
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for i := 0; i < workers; i++ {
				select {
				case node := <-entered:
					if node != target.Name() {
						t.Fatalf("Init ran on %s, want %s", node, target.Name())
					}
				case <-deadline.C:
					t.Fatalf("only %d workers began Init; source=%v target=%v", i, stats(source), stats(target))
				}
			}
			waitUntil(t, 5*time.Second, func() bool {
				return stats(source)["launching_count"] == fmt.Sprint(tc.limit) && stats(target)["pending_launches"] == fmt.Sprint(tc.limit)
			})
			if got := stats(source)["scan_pending"]; got != fmt.Sprint(min(tc.total, pageSize)-tc.limit) {
				t.Fatalf("remaining scanner items = %s", got)
			}
			unblock()
			waitUntil(t, 10*time.Second, func() bool {
				return completed.Load() == int32(tc.total) && stats(source)["launching_count"] == "0" && stats(target)["pending_launches"] == "0"
			})
			if got := peak.Load(); got != int32(workers) {
				t.Fatalf("Init concurrency = %d, want %d", got, workers)
			}
			if got := scans.Load(); got != 1 {
				t.Fatalf("recovery needed %d scans, want 1", got)
			}
			if got, want := pages.Load(), int32((tc.total+pageSize-1)/pageSize); got != want {
				t.Fatalf("scanner pages = %d, want %d", got, want)
			}
			for i := workers; i < tc.total; i++ {
				if node := <-entered; node != target.Name() {
					t.Fatalf("Init ran on %s, want %s", node, target.Name())
				}
			}
		})
	}
}

type pullRecoveryProc struct {
	act.Actor
	onInit func(gen.Atom)
}

func (p *pullRecoveryProc) Init(...any) error {
	p.onInit(p.Node().Name())
	return nil
}

type mockLauncher struct {
	processes []system.DaemonProcess
}

func (m *mockLauncher) Scan() ([]system.DaemonProcess, bool, error) {
	return m.processes, false, nil
}

type countingProc struct {
	act.Actor
	onInit func()
}

func (p *countingProc) Init(args ...any) error {
	if p.onInit != nil {
		p.onInit()
	}
	return nil
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
		return n1.Topology().GetAvailableNodes().Len() == 2
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
}

func TestDaemonEnsureConcurrentSingleLaunch(t *testing.T) {
	cluster := mem.NewCluster()
	launcherName := gen.Atom("counting_launcher")
	daemonName := gen.Atom("counting_daemon")
	var starts atomic.Int32

	err := system.RegisterLauncher(launcherName, system.Launcher{
		Factory: func() gen.ProcessBehavior {
			return &countingProc{
				onInit: func() {
					starts.Add(1)
				},
			}
		},
	})
	if err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() {
		system.UnregisterLauncher(launcherName)
	})

	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	waitUntil(t, 5*time.Second, func() bool {
		return n1.Topology().GetAvailableNodes().Len() == 2 &&
			n2.Topology().GetAvailableNodes().Len() == 2
	})

	owner := n1.Topology().PickCoordinatorNode(daemonName)
	if owner == "" {
		t.Fatal("expected directory owner")
	}

	msg := core.MessageEnsureDaemon{
		Launcher: launcherName,
		Process: system.DaemonProcess{
			ProcessName: daemonName,
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = n1.ForwardSend(string(system.DaemonMonitorProcess), msg, app.ForwardNode(owner))
		}()
	}
	wg.Wait()

	waitUntil(t, 10*time.Second, func() bool {
		node, ok := locateNode(n1, daemonName)
		return ok && node != ""
	})

	time.Sleep(500 * time.Millisecond)
	if got := starts.Load(); got != 1 {
		t.Fatalf("expected single process start, got %d", got)
	}
}
