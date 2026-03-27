package cron

import (
	"context"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestStaticSourceScanShards(t *testing.T) {
	const shardCount = 16
	source := NewStaticSource(shardCount,
		JobSpec{ID: "job-a", ShardKey: "job-a", Schedule: "* * * * *", TriggerProcess: gen.Atom("a")},
		JobSpec{ID: "job-b", ShardKey: "job-b", Schedule: "* * * * *", TriggerProcess: gen.Atom("b")},
	)

	shardA := ShardFor("job-a", shardCount)
	result, err := source.ScanShards(context.Background(), ScanShardsRequest{
		Shards: []uint32{shardA},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("scan shards: %v", err)
	}
	if len(result.Jobs) != 1 || result.Jobs[0].ID != "job-a" {
		t.Fatalf("unexpected scan result: %+v", result.Jobs)
	}
}

func TestStaticSourceDisablesWatch(t *testing.T) {
	source := NewStaticSource(16)
	if source.SupportsWatch() {
		t.Fatalf("static source should not support watch")
	}
	ch, err := source.Watch(context.Background(), WatchRequest{Shards: []uint32{1}})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if ch != nil {
		t.Fatalf("static source watch should be nil")
	}
}

func TestStaticSourceDispatchLifecycle(t *testing.T) {
	store := NewMemoryKVStore()
	backend := newStateBackend(store)
	owner := gen.Atom("node@local")

	lease, err := backend.AcquireShardLease(context.Background(), 3, owner, 5*time.Second)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if !lease.Acquired || lease.Owner != owner || lease.Epoch != 1 {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	scheduledAt := time.Date(2026, 3, 27, 10, 5, 0, 0, time.UTC)
	records, err := backend.ClaimDispatches(context.Background(), 3, owner, lease.Epoch, []DispatchClaim{{
		JobID:       "job-1",
		ScheduledAt: scheduledAt,
	}})
	if err != nil {
		t.Fatalf("claim dispatches: %v", err)
	}
	if len(records) != 1 || records[0].State != DispatchStatePending {
		t.Fatalf("unexpected records: %+v", records)
	}

	reclaimed, err := backend.ClaimDispatches(context.Background(), 3, owner, lease.Epoch, []DispatchClaim{{
		JobID:       "job-1",
		ScheduledAt: scheduledAt,
	}})
	if err != nil {
		t.Fatalf("reclaim dispatches: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].Key != records[0].Key || reclaimed[0].State != DispatchStatePending {
		t.Fatalf("unexpected reclaimed records: %+v", reclaimed)
	}

	if err := backend.AckDispatches(context.Background(), 3, owner, lease.Epoch, []string{records[0].Key}); err != nil {
		t.Fatalf("ack dispatches: %v", err)
	}
	acked, err := backend.ClaimDispatches(context.Background(), 3, owner, lease.Epoch, []DispatchClaim{{
		JobID:       "job-1",
		ScheduledAt: scheduledAt,
	}})
	if err != nil {
		t.Fatalf("claim acked dispatch: %v", err)
	}
	if len(acked) != 1 || acked[0].State != DispatchStateAcked {
		t.Fatalf("expected acked record, got %+v", acked)
	}

	if err := backend.AdvanceShardCheckpoint(context.Background(), 3, owner, lease.Epoch, 42); err != nil {
		t.Fatalf("advance checkpoint: %v", err)
	}
	checkpoint, err := backend.GetShardCheckpoint(context.Background(), 3)
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if !checkpoint.Valid || checkpoint.Slot != 42 {
		t.Fatalf("unexpected checkpoint: %+v", checkpoint)
	}
}
