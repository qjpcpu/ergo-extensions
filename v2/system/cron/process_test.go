package cron

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

type stubTrigger struct {
	failed map[string]struct{}
	sent   []DispatchJob
}

func (s *stubTrigger) Fire(process gen.Process, jobs []DispatchJob) ([]DispatchJob, error) {
	var failed []DispatchJob
	for _, job := range jobs {
		if _, ok := s.failed[job.JobID]; ok {
			failed = append(failed, job)
			continue
		}
		s.sent = append(s.sent, job)
	}
	return failed, nil
}

func TestBootstrapLastTickUsesEarliestLoadedShard(t *testing.T) {
	process := &Process{
		options: SchedulerOptions{TickResolution: time.Minute}.WithDefaults(),
		owned:   map[uint32]*shardRuntime{},
	}

	process.owned[1] = &shardRuntime{loadedAt: time.Date(2026, 3, 27, 10, 1, 20, 0, time.UTC)}
	process.owned[2] = &shardRuntime{loadedAt: time.Date(2026, 3, 27, 10, 3, 10, 0, time.UTC)}

	lastTick := process.bootstrapLastTick(time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC))
	expected := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	if !lastTick.Equal(expected) {
		t.Fatalf("unexpected bootstrap last tick: got %s want %s", lastTick, expected)
	}
}

func TestFlushPendingRetainsBacklogAndFailedJobs(t *testing.T) {
	resolution := time.Minute
	now := time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC)

	job1, err := compileJob(JobSpec{ID: "job-1", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job1: %v", err)
	}
	job2, err := compileJob(JobSpec{ID: "job-2", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job2: %v", err)
	}
	job3, err := compileJob(JobSpec{ID: "job-3", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job3: %v", err)
	}

	runtime := newShardRuntime(1, 1)
	runtime.Activate()
	leaseExpiry := time.Now().UTC().Add(5 * time.Minute)
	runtime.lease = ShardLease{
		Shard:     1,
		Owner:     gen.Atom("node@local"),
		Epoch:     1,
		ExpiresAt: leaseExpiry,
		Acquired:  true,
	}
	store := NewMemoryKVStore()
	backend := newStateBackend(store)
	leaseData, err := marshalState(leaseRecord{
		Owner:     runtime.lease.Owner,
		Epoch:     runtime.lease.Epoch,
		ExpiresAt: runtime.lease.ExpiresAt,
	})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	if err := store.Put(context.Background(), leaseKey(1), leaseData); err != nil {
		t.Fatalf("put lease: %v", err)
	}
	claimed, err := backend.ClaimDispatches(context.Background(), 1, runtime.lease.Owner, runtime.lease.Epoch, []DispatchClaim{
		{JobID: "job-1", ScheduledAt: now.Add(-2 * time.Minute)},
		{JobID: "job-2", ScheduledAt: now.Add(-2 * time.Minute)},
		{JobID: "job-3", ScheduledAt: now.Add(-2 * time.Minute)},
	})
	if err != nil {
		t.Fatalf("seed dispatch claims: %v", err)
	}
	recordByJobID := make(map[string]DispatchRecord, len(claimed))
	for _, record := range claimed {
		recordByJobID[record.JobID] = record
	}

	process := &Process{
		options: SchedulerOptions{
			TickResolution:     resolution,
			MaxDispatchPerTick: 2,
		}.WithDefaults(),
		owned: map[uint32]*shardRuntime{
			1: runtime,
		},
		source:  &ManagedSource{StateKV: store},
		trigger: &stubTrigger{failed: map[string]struct{}{"job-1": {}}},
		pending: []pendingDispatch{
			{job: job1, shard: 1, scheduledAt: now.Add(-2 * time.Minute), dispatchKey: recordByJobID["job-1"].Key, slot: slotKey(now.Add(-2*time.Minute), resolution)},
			{job: job2, shard: 1, scheduledAt: now.Add(-2 * time.Minute), dispatchKey: recordByJobID["job-2"].Key, slot: slotKey(now.Add(-2*time.Minute), resolution)},
			{job: job3, shard: 1, scheduledAt: now.Add(-2 * time.Minute), dispatchKey: recordByJobID["job-3"].Key, slot: slotKey(now.Add(-2*time.Minute), resolution)},
		},
	}

	process.flushPending()

	if len(process.pending) != 2 {
		t.Fatalf("unexpected pending size: got %d want 2", len(process.pending))
	}

	pendingIDs := map[string]struct{}{}
	for _, item := range process.pending {
		pendingIDs[item.job.Spec.ID] = struct{}{}
	}
	if _, ok := pendingIDs["job-1"]; !ok {
		t.Fatalf("failed job should remain pending")
	}
	if _, ok := pendingIDs["job-3"]; !ok {
		t.Fatalf("backlog job should remain pending")
	}

	records, err := backend.ClaimDispatches(context.Background(), 1, runtime.lease.Owner, runtime.lease.Epoch, []DispatchClaim{{
		JobID:       "job-2",
		ScheduledAt: now.Add(-2 * time.Minute),
	}})
	if err != nil {
		t.Fatalf("claim dispatch after flush: %v", err)
	}
	if len(records) != 1 || records[0].State != DispatchStateAcked {
		t.Fatalf("expected acked dispatch for job-2, got %+v", records)
	}
}

func TestLoadShardResultsRunsConcurrentLoads(t *testing.T) {
	shards := []uint32{1, 2, 3, 4}
	var active int32
	var maxActive int32
	var entered sync.WaitGroup
	entered.Add(len(shards))
	release := make(chan struct{})
	done := make(chan []shardLoadResult, 1)
	go func() {
		done <- loadShardResults(shards, 4, func(shard uint32) shardLoadResult {
			current := atomic.AddInt32(&active, 1)
			defer atomic.AddInt32(&active, -1)
			for {
				prev := atomic.LoadInt32(&maxActive)
				if current <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, current) {
					break
				}
			}
			entered.Done()
			<-release
			return shardLoadResult{shard: shard}
		})
	}()

	entered.Wait()
	close(release)
	results := <-done

	if len(results) != len(shards) {
		t.Fatalf("unexpected results size: got %d want %d", len(results), len(shards))
	}
	if maxActive <= 1 {
		t.Fatalf("expected concurrent loads, max active=%d", maxActive)
	}
}

func TestCronOwnerRingIsDecoupledFromDirectoryRing(t *testing.T) {
	nodes := []gen.Atom{
		"node-a@127.0.0.1",
		"node-b@127.0.0.1",
		"node-c@127.0.0.1",
		"node-d@127.0.0.1",
		"node-e@127.0.0.1",
	}

	book := core.NewAddressBook()
	if err := book.SetAvailableNodes(core.NewNodeList(nodes...)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}

	process := &Process{
		options: SchedulerOptions{}.WithDefaults(),
		ring: &consistentState{
			prevMembers: make(map[gen.Atom]ringMember),
			ring:        makeRing(),
		},
	}
	for _, node := range nodes {
		member := process.ownerRingMember(node)
		process.ring.ring.Add(member)
		process.ring.prevMembers[node] = member
	}

	matches := 0
	total := 256
	for i := 0; i < total; i++ {
		token := "cron-shard:" + strconv.Itoa(i)
		dirOwner := book.PickCoordinatorNode(gen.Atom(token))
		cronOwner := shardOwner(process.ring.ring, uint32(i))
		if dirOwner == cronOwner {
			matches++
		}
	}

	if matches >= total/2 {
		t.Fatalf("expected cron owner ring to diverge from directory ring, matches=%d total=%d", matches, total)
	}
}

func TestFlushPendingCapsRetriedQueue(t *testing.T) {
	const maxPendingCapacity = 100000
	pending := make([]pendingDispatch, maxPendingCapacity+1)
	for i := range pending {
		pending[i] = pendingDispatch{
			job: &CompiledJob{Spec: JobSpec{ID: strconv.Itoa(i)}},
		}
	}

	process := &Process{
		options: SchedulerOptions{
			MaxDispatchPerTick: maxPendingCapacity * 2,
		}.WithDefaults(),
		pending: pending,
	}

	process.flushPending()

	if got := len(process.pending); got != maxPendingCapacity {
		t.Fatalf("unexpected pending size after cap: got %d want %d", got, maxPendingCapacity)
	}
	if got := process.pending[0].job.Spec.ID; got != "1" {
		t.Fatalf("expected oldest pending item to be dropped, got head job %q", got)
	}
	if got := process.pending[len(process.pending)-1].job.Spec.ID; got != strconv.Itoa(maxPendingCapacity) {
		t.Fatalf("expected newest pending item to remain, got tail job %q", got)
	}
}

func TestRebalanceRestoresAllDispatchesAcrossParallelShards(t *testing.T) {
	const shards = 8
	const jobsPerShard = 100
	var scans sync.WaitGroup
	scans.Add(shards)
	provider := watchableProvider{scan: func(_ context.Context, req ScanShardsRequest) (ScanShardsResult, error) {
		scans.Done()
		scans.Wait()
		jobs := make([]JobSpec, jobsPerShard)
		for i := range jobs {
			jobs[i] = JobSpec{ID: fmt.Sprintf("%d-%d", req.Shards[0], i), Schedule: "* * * * *", TriggerProcess: "worker"}
		}
		return ScanShardsResult{Jobs: jobs, Done: true}, nil
	}}
	actor := spawnCronUnit(t, NewManagedSource(provider, NewMemoryKVStore()))
	p := actor.Behavior().(*Process)
	p.options.ScanConcurrency = shards
	p.options.ShardCount = shards
	p.registrar = &cronTestRegistrar{}
	if err := p.rebalance(); err != nil {
		t.Fatal(err)
	}
	if len(p.owned) != shards {
		t.Fatalf("owned shards: got %d want %d", len(p.owned), shards)
	}
	if len(p.pending) != shards*jobsPerShard {
		t.Fatalf("pending dispatches: got %d want %d", len(p.pending), shards*jobsPerShard)
	}
	keys := make(map[string]bool)
	for _, item := range p.pending {
		if keys[item.dispatchKey] {
			t.Fatalf("duplicate dispatch: %s", item.dispatchKey)
		}
		keys[item.dispatchKey] = true
	}
	trigger := &stubTrigger{}
	p.trigger = trigger
	for len(p.pending) > 0 {
		p.flushPending()
	}
	if len(trigger.sent) != shards*jobsPerShard {
		t.Fatalf("sent dispatches: got %d", len(trigger.sent))
	}
}

func TestRecurringDispatchContinuesAcrossRetriesAndBacklog(t *testing.T) {
	for _, test := range []struct {
		name        string
		jobs, limit int
		failFirst   bool
	}{
		{name: "retry", jobs: 1, limit: 10, failFirst: true},
		{name: "backlog", jobs: 2, limit: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := spawnCronUnit(t, NewManagedSource(NewStaticSource(1), NewMemoryKVStore()))
			p := actor.Behavior().(*Process)
			p.options.MaxDispatchPerTick = test.limit
			now := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
			lease, err := p.backend().AcquireShardLease(context.Background(), 0, p.Node().Name(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			runtime := newShardRuntime(0, 1)
			runtime.lease = lease
			runtime.checkpoint = slotKey(now, time.Minute) - 1
			runtime.Activate()
			for i := 0; i < test.jobs; i++ {
				job, err := compileJob(JobSpec{ID: fmt.Sprint(i), Schedule: "* * * * *", TriggerProcess: "worker"})
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.Upsert(job, now, time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			p.owned[0] = runtime
			p.lastTick = now.Add(-time.Minute)
			trigger := &stubTrigger{}
			if test.failFirst {
				trigger.failed = map[string]struct{}{"0": {}}
			}
			p.trigger = trigger
			for tick := 0; tick < 3; tick++ {
				if err := p.handleTick(now.Add(time.Duration(tick) * time.Minute)); err != nil {
					t.Fatal(err)
				}
				trigger.failed = nil
			}
			for len(p.pending) > 0 {
				p.flushPending()
			}
			if len(trigger.sent) != 3*test.jobs {
				t.Fatalf("sent dispatches: got %d want %d", len(trigger.sent), 3*test.jobs)
			}
			seen := make(map[string]bool)
			for _, dispatch := range trigger.sent {
				if seen[dispatch.DispatchKey] {
					t.Fatalf("duplicate dispatch: %s", dispatch.DispatchKey)
				}
				seen[dispatch.DispatchKey] = true
			}
			for id := range runtime.jobs {
				for tick := 0; tick < 3; tick++ {
					key := dispatchKey(0, id, now.Add(time.Duration(tick)*time.Minute))
					if !seen[key] {
						t.Fatalf("missing occurrence: %s", key)
					}
				}
				if got, want := runtime.slots.nextSlotByJob[id], slotKey(now.Add(3*time.Minute), time.Minute); got != want {
					t.Fatalf("next occurrence for %s: got %d want %d", id, got, want)
				}
			}
			if got, want := runtime.checkpoint, slotKey(now.Add(2*time.Minute), time.Minute); got != want {
				t.Fatalf("checkpoint: got %d want %d", got, want)
			}
		})
	}
}
