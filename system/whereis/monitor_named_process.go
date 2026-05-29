package whereis

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
)

const PlacementMonitorProcessName = gen.Atom("extensions_whereis_placement_monitor")

const defaultPlacementMonitorInterval = 30 * time.Second

type messageMonitorPlacementTick struct{}

type placementMonitor struct {
	act.Actor
	interval time.Duration
	// placements records name -> named process PID.
	// MonitorPlacement must be sent by the local named process itself; this
	// actor is not a general-purpose placement subscription service.
	placements map[gen.Atom]gen.PID
}

func MonitorPlacementFactory(interval time.Duration) gen.ProcessFactory {
	if interval == 0 {
		interval = defaultPlacementMonitorInterval
	}
	return func() gen.ProcessBehavior {
		return &placementMonitor{
			interval:   interval,
			placements: make(map[gen.Atom]gen.PID),
		}
	}
}

func (p *placementMonitor) Init(args ...any) error {
	p.scheduleTick()
	return nil
}

func (p *placementMonitor) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case core.MonitorPlacement:
		p.monitorPlacement(from, msg.Name)
	case messageMonitorPlacementTick:
		p.locatePlacements()
		p.scheduleTick()
	case core.MessageLocateResult:
		p.handleLocateResult(msg)
	case gen.MessageDownPID:
		p.handleDownPID(msg.PID)
	}
	return nil
}

func (p *placementMonitor) monitorPlacement(pid gen.PID, name gen.Atom) {
	if name == "" || pid == (gen.PID{}) {
		return
	}
	if old, ok := p.placements[name]; ok {
		if old != pid {
			p.DemonitorPID(old)
			delete(p.placements, name)
		} else {
			return
		}
	}
	if err := p.MonitorPID(pid); err != nil {
		return
	}
	p.placements[name] = pid
}

func (p *placementMonitor) locatePlacements() {
	for name := range p.placements {
		_ = p.Send(ProcessName, core.MessageLocate{Name: name})
	}
}

func (p *placementMonitor) handleLocateResult(msg core.MessageLocateResult) {
	if msg.Name == "" || msg.Node == "" || msg.Node == p.Node().Name() {
		return
	}
	pid, ok := p.placements[msg.Name]
	if !ok {
		return
	}
	//lint:ignore S1016 keep this explicit so the two message types can evolve independently.
	_ = p.Send(pid, core.DuplicatePlacement{Name: msg.Name, Node: msg.Node})
}

func (p *placementMonitor) handleDownPID(pid gen.PID) {
	if pid == (gen.PID{}) {
		return
	}
	for name, owner := range p.placements {
		if owner == pid {
			delete(p.placements, name)
			p.DemonitorPID(pid)
			return
		}
	}
}

func (p *placementMonitor) scheduleTick() {
	p.SendAfter(p.PID(), messageMonitorPlacementTick{}, p.interval)
}
