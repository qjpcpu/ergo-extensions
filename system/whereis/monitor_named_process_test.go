package whereis

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
)

func spawnPlacementMonitorUnit(t *testing.T, self gen.Atom) *unit.TestActor {
	t.Helper()
	actor, err := unit.Spawn(t, MonitorPlacementFactory(time.Millisecond), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn placement monitor: %v", err)
	}
	actor.ClearEvents()
	return actor
}

func TestPlacementMonitorInitSchedulesTick(t *testing.T) {
	actor, err := unit.Spawn(t, MonitorPlacementFactory(time.Millisecond), unit.WithNodeName(gen.Atom("node-a@127.0.0.1")))
	if err != nil {
		t.Fatalf("spawn placement monitor: %v", err)
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageMonitorPlacementTick{}).
		Once().
		Assert()
}

func TestPlacementMonitorTracksAndReplacesPlacements(t *testing.T) {
	actor := spawnPlacementMonitorUnit(t, gen.Atom("node-a@127.0.0.1"))
	monitor := actor.Behavior().(*placementMonitor)
	name := gen.Atom("proc")
	pid1 := gen.PID{Node: gen.Atom("node-a@127.0.0.1"), ID: 1}
	pid2 := gen.PID{Node: gen.Atom("node-a@127.0.0.1"), ID: 2}

	actor.SendMessage(pid1, core.MonitorPlacement{})
	if len(monitor.placements) != 0 {
		t.Fatal("empty placement name should be ignored")
	}
	actor.SendMessage(gen.PID{}, core.MonitorPlacement{Name: name})
	if len(monitor.placements) != 0 {
		t.Fatal("zero pid placement should be ignored")
	}

	actor.SendMessage(pid1, core.MonitorPlacement{Name: name})
	if got := monitor.placements[name]; got != pid1 {
		t.Fatalf("expected pid1 placement, got %v", got)
	}
	foundMonitor := false
	for _, event := range actor.Events() {
		monitorEvent, ok := event.(unit.MonitorEvent)
		if ok && monitorEvent.Target == pid1 {
			foundMonitor = true
		}
	}
	if !foundMonitor {
		t.Fatal("expected pid1 monitor event")
	}

	actor.ClearEvents()
	actor.SendMessage(pid1, core.MonitorPlacement{Name: name})
	actor.ShouldNotSend().Assert()
	if got := monitor.placements[name]; got != pid1 {
		t.Fatalf("same pid should keep placement, got %v", got)
	}

	actor.SendMessage(pid2, core.MonitorPlacement{Name: name})
	if got := monitor.placements[name]; got != pid2 {
		t.Fatalf("expected pid2 replacement, got %v", got)
	}
	foundDemonitor := false
	for _, event := range actor.Events() {
		demonitorEvent, ok := event.(unit.DemonitorEvent)
		if ok && demonitorEvent.Target == pid1 {
			foundDemonitor = true
		}
	}
	if !foundDemonitor {
		t.Fatal("expected old pid demonitor event")
	}
}

func TestPlacementMonitorTickLocateResultAndDownPID(t *testing.T) {
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	actor := spawnPlacementMonitorUnit(t, self)
	monitor := actor.Behavior().(*placementMonitor)
	name := gen.Atom("proc")
	pid := gen.PID{Node: self, ID: 1}
	actor.SendMessage(pid, core.MonitorPlacement{Name: name})
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, messageMonitorPlacementTick{})
	actor.ShouldSend().
		To(ProcessName).
		Message(core.MessageLocate{Name: name}).
		Once().
		Assert()
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageMonitorPlacementTick{}).
		Once().
		Assert()

	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, core.MessageLocateResult{Name: name, Node: self})
	actor.SendMessage(gen.PID{}, core.MessageLocateResult{Name: gen.Atom("unknown"), Node: remote})
	actor.ShouldNotSend().Assert()

	actor.SendMessage(gen.PID{}, core.MessageLocateResult{Name: name, Node: remote})
	actor.ShouldSend().
		To(pid).
		Message(core.DuplicatePlacement{Name: name, Node: remote}).
		Once().
		Assert()

	actor.SendMessage(gen.PID{}, gen.MessageDownPID{})
	if _, ok := monitor.placements[name]; !ok {
		t.Fatal("zero down pid should be ignored")
	}
	actor.SendMessage(gen.PID{}, gen.MessageDownPID{PID: pid})
	if _, ok := monitor.placements[name]; ok {
		t.Fatal("down pid should remove placement")
	}
}
