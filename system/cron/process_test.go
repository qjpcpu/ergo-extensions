package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ergo.services/ergo/gen"
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

	slot := slotKey(now.Add(-time.Minute), resolution)
	if got, ok := runtime.slots.nextSlotByJob["job-2"]; !ok || got != slot {
		t.Fatalf("successful job should be rescheduled to slot %d, got %d present=%v", slot, got, ok)
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
