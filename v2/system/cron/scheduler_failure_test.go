package cron

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

type schedulerRegistrar struct {
	cronTestRegistrar
	event    gen.Event
	eventErr error
}

func (r *schedulerRegistrar) Event() (gen.Event, error) { return r.event, r.eventErr }

type schedulerNetwork struct {
	gen.Network
	registrar gen.Registrar
	err       error
}

func (n *schedulerNetwork) Registrar() (gen.Registrar, error) { return n.registrar, n.err }

type schedulerNode struct {
	gen.Node
	network gen.Network
}

func (n schedulerNode) Network() gen.Network { return n.network }

func TestCronRecoversFromRegistrarAndRebalanceFailures(t *testing.T) {
	for _, stage := range []string{"registrar", "event", "monitor", "nodes"} {
		t.Run(stage, func(t *testing.T) {
			actor := spawnCronUnit(t, NewManagedSource(NewStaticSource(1), NewMemoryKVStore()))
			p := actor.Behavior().(*Process)
			p.options.ShardCount = 1
			failure := errors.New("registrar unavailable")
			reg := &schedulerRegistrar{event: gen.Event{Name: "cron", Node: p.Node().Name()}}
			network := &schedulerNetwork{registrar: reg}
			node := schedulerNode{Node: p.Node(), network: network}
			actor.OnNode(func() gen.Node { return node })
			monitor := actor.OnMonitorEvent(reg.event)
			switch stage {
			case "registrar":
				network.err = failure
			case "event":
				reg.eventErr = failure
			case "monitor":
				monitor.Fail(failure)
			case "nodes":
				reg.err = failure
			}
			actor.SendMessage(gen.PID{}, messageInit{})
			if len(p.owned) != 0 {
				t.Fatal("failed setup activated shards")
			}
			if stage == "nodes" {
				actor.SendMessage(gen.PID{}, messageRebalance{})
				if p.cancelRebalance == nil || p.cancelTick == nil {
					t.Fatal("failed rebalance lost timers")
				}
			}
			network.err = nil
			reg.eventErr = nil
			reg.err = nil
			monitor.Fail(nil)
			actor.SendMessage(gen.PID{}, messageInit{})
			if len(p.owned) != 1 || p.owned[0].state != shardStateActive || p.cancelTick == nil {
				t.Fatal("scheduler did not resume after discovery recovered")
			}
		})
	}
}

func TestCronPagedLoadSkipsInvalidJobsAndRestoresCheckpoint(t *testing.T) {
	store := NewMemoryKVStore()
	base := time.Now().UTC().Truncate(time.Minute)
	slot := slotKey(base, time.Minute)
	pages := 0
	provider := watchableProvider{scan: func(_ context.Context, req ScanShardsRequest) (ScanShardsResult, error) {
		pages++
		if pages == 1 {
			if req.Cursor != "" {
				t.Fatal("unexpected initial cursor")
			}
			return ScanShardsResult{Jobs: []JobSpec{{ID: "broken", Schedule: "bad", TriggerProcess: "worker"}}, NextCursor: "page2"}, nil
		}
		if req.Cursor != "page2" {
			t.Fatal("lost pagination cursor")
		}
		return ScanShardsResult{Jobs: []JobSpec{{ID: "healthy", Schedule: "* * * * *", TriggerProcess: "worker"}}, NextCursor: "watch-cursor", Done: true}, nil
	}}
	actor := spawnCronUnit(t, NewManagedSource(provider, store))
	p := actor.Behavior().(*Process)
	lease, err := p.backend().AcquireShardLease(context.Background(), 0, p.Node().Name(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.backend().AdvanceShardCheckpoint(context.Background(), 0, p.Node().Name(), lease.Epoch, slot-2); err != nil {
		t.Fatal(err)
	}
	runtime, err := p.prepareShardRuntime(0, 7, base, p.Log())
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 || len(runtime.jobs) != 1 || runtime.jobs["healthy"] == nil || runtime.cursor != "watch-cursor" || runtime.checkpoint != slot-2 {
		t.Fatalf("incomplete paged load: %+v", runtime)
	}
}

type failingWatchProcess struct {
	gen.Process
	spawnErr, monitorErr error
	child                gen.PID
}

func (p failingWatchProcess) Spawn(gen.ProcessFactory, gen.ProcessOptions, ...any) (gen.PID, error) {
	return p.child, p.spawnErr
}
func (p failingWatchProcess) MonitorPID(gen.PID) error { return p.monitorErr }

func TestCronWatcherFailureRollsBackShardActivation(t *testing.T) {
	for _, stage := range []string{"spawn", "monitor"} {
		t.Run(stage, func(t *testing.T) {
			actor := spawnCronUnit(t, NewManagedSource(watchableProvider{}, NewMemoryKVStore()))
			p := actor.Behavior().(*Process)
			base := time.Now().UTC().Truncate(time.Minute)
			runtime, err := p.prepareShardRuntime(1, 1, base, p.Log())
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("watch worker unavailable")
			child := gen.PID{Node: p.Node().Name(), ID: 900}
			process := failingWatchProcess{Process: p.Process, child: child}
			var killed gen.PID
			actor.Node().OnKill(func(pid gen.PID) error { killed = pid; return nil })
			if stage == "spawn" {
				process.spawnErr = failure
			} else {
				process.monitorErr = failure
			}
			p.Process = process
			if err := p.commitShardRuntime(runtime); !errors.Is(err, failure) {
				t.Fatal("watch failure hidden", err)
			}
			if len(p.owned) != 0 || runtime.state == shardStateActive {
				t.Fatal("unwatched shard was activated")
			}
			if stage == "monitor" && killed != child {
				t.Fatal("unmonitored worker was retained")
			}
		})
	}
}

func TestCronLoadFailuresLeaveShardAvailableForRetry(t *testing.T) {
	for _, stage := range []string{"lease", "owned", "scan", "checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			store := &faultStateStore{MemoryKVStore: NewMemoryKVStore()}
			failure := errors.New("storage unavailable")
			provider := watchableProvider{scan: func(context.Context, ScanShardsRequest) (ScanShardsResult, error) {
				if stage == "scan" {
					return ScanShardsResult{}, failure
				}
				return ScanShardsResult{Done: true}, nil
			}}
			actor := spawnCronUnit(t, NewManagedSource(provider, store))
			p := actor.Behavior().(*Process)
			if stage == "owned" {
				if _, err := p.backend().AcquireShardLease(context.Background(), 0, "other", time.Hour); err != nil {
					t.Fatal(err)
				}
			}
			store.get = func(c context.Context, k string) (KVEntry, error) {
				if stage == "lease" && k == leaseKey(0) || stage == "checkpoint" && k == checkpointKey(0) {
					return KVEntry{}, failure
				}
				return store.MemoryKVStore.Get(c, k)
			}
			err := p.loadShard(0)
			if err == nil || (stage != "owned" && !errors.Is(err, failure)) {
				t.Fatal("load failure hidden", err)
			}
			if len(p.owned) != 0 {
				t.Fatal("failed load published a shard")
			}
		})
	}
}

func TestCronAcknowledgementFailureRetainsDispatchUntilRetry(t *testing.T) {
	ctx := context.Background()
	store := &faultStateStore{MemoryKVStore: NewMemoryKVStore()}
	actor := spawnCronUnit(t, NewManagedSource(NewStaticSource(1), store))
	p := actor.Behavior().(*Process)
	runtime := newShardRuntime(0, 1)
	lease, err := p.backend().AcquireShardLease(ctx, 0, p.Node().Name(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runtime.lease = lease
	runtime.Activate()
	p.owned[0] = runtime
	scheduled := time.Now().UTC().Truncate(time.Minute)
	slot := slotKey(scheduled, time.Minute)
	runtime.checkpoint = slot - 1
	job, err := compileJob(JobSpec{ID: "job", Schedule: "* * * * *", TriggerProcess: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.collectShardSlot(runtime, scheduled, []*CompiledJob{job}); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("ack unavailable")
	store.swap = func(c context.Context, k string, v uint64, data []byte) (bool, error) {
		if strings.HasPrefix(k, "cron/dispatch/") {
			return false, failure
		}
		return store.MemoryKVStore.CompareAndSwap(c, k, v, data)
	}
	p.flushPending()
	if len(p.pending) != 1 || runtime.checkpoint != slot-1 {
		t.Fatal("failed acknowledgement discarded work or advanced checkpoint")
	}
	store.swap = nil
	p.flushPending()
	if len(p.pending) != 0 || runtime.checkpoint != slot {
		t.Fatal("retry did not confirm dispatch and advance checkpoint")
	}
	records, err := p.backend().ClaimDispatches(ctx, 0, lease.Owner, lease.Epoch, []DispatchClaim{{JobID: "job", ScheduledAt: scheduled}})
	if err != nil || len(records) != 1 || records[0].State != DispatchStateAcked {
		t.Fatal("acknowledgement was not durable", records, err)
	}
	if err := p.collectShardSlot(runtime, scheduled, []*CompiledJob{job}); err != nil {
		t.Fatal(err)
	}
	if len(p.pending) != 0 {
		t.Fatal("replay dispatched an acknowledged occurrence")
	}
}
