package system_test

import (
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

type placementMonitorReceiver struct {
	act.Actor
	name gen.Atom
	ch   chan system.DuplicatePlacement
}

func (p *placementMonitorReceiver) Init(args ...any) error {
	return p.Send(system.PlacementMonitorProcess, system.MonitorPlacement{Name: p.name})
}

func (p *placementMonitorReceiver) HandleMessage(from gen.PID, message any) error {
	if msg, ok := message.(system.DuplicatePlacement); ok {
		p.ch <- msg
	}
	return nil
}

func TestPlacementMonitorReportsDuplicatePlacement(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	waitForClusterNodes(t, 10*time.Second, n1, n2)

	name := uniqueProcessName("proc.placement.dup")
	_ = spawnNamed(t, n1, name)
	time.Sleep(1200 * time.Millisecond)
	_ = spawnNamed(t, n2, name)

	var winner gen.Atom
	waitUntil(t, 30*time.Second, func() bool {
		w1 := n1.LocateProcess(name)
		w2 := n2.LocateProcess(name)
		if w1 == "" || w1 != w2 {
			return false
		}
		winner = w1
		return true
	})
	loser := n2
	if winner == n2.Name() {
		loser = n1
	}

	ch := make(chan system.DuplicatePlacement, 1)
	_, err := loser.Spawn(func() gen.ProcessBehavior {
		return &placementMonitorReceiver{name: name, ch: ch}
	}, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn placement monitor receiver: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.Name != name {
			t.Fatalf("expected duplicate name %s, got %s", name, msg.Name)
		}
		if msg.Node != winner {
			t.Fatalf("expected duplicate node %s, got %s", winner, msg.Node)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for DuplicatePlacement")
	}
}
