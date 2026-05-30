package cron

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"github.com/qjpcpu/registrar/events"
)

type cronTestRegistrar struct {
	nodes []gen.Atom
	err   error
}

func (r *cronTestRegistrar) Register(gen.NodeRegistrar, gen.RegisterRoutes) (gen.StaticRoutes, error) {
	return gen.StaticRoutes{}, nil
}
func (r *cronTestRegistrar) Resolver() gen.Resolver         { return nil }
func (r *cronTestRegistrar) RegisterProxy(gen.Atom) error   { return gen.ErrUnsupported }
func (r *cronTestRegistrar) UnregisterProxy(gen.Atom) error { return gen.ErrUnsupported }
func (r *cronTestRegistrar) RegisterApplicationRoute(gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}
func (r *cronTestRegistrar) UnregisterApplicationRoute(gen.Atom) error { return gen.ErrUnsupported }
func (r *cronTestRegistrar) Nodes() ([]gen.Atom, error)                { return r.nodes, r.err }
func (r *cronTestRegistrar) Config(...string) (map[string]any, error)  { return nil, gen.ErrUnsupported }
func (r *cronTestRegistrar) ConfigItem(string) (any, error)            { return nil, gen.ErrUnsupported }
func (r *cronTestRegistrar) Event() (gen.Event, error)                 { return gen.Event{}, nil }
func (r *cronTestRegistrar) Info() gen.RegistrarInfo                   { return gen.RegistrarInfo{} }
func (r *cronTestRegistrar) Terminate()                                {}
func (r *cronTestRegistrar) Version() gen.Version                      { return gen.Version{} }

type watchableProvider struct {
	scan  func(context.Context, ScanShardsRequest) (ScanShardsResult, error)
	watch func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error)
}

func (p watchableProvider) ScanShards(ctx context.Context, req ScanShardsRequest) (ScanShardsResult, error) {
	if p.scan != nil {
		return p.scan(ctx, req)
	}
	return ScanShardsResult{Done: true}, nil
}

func (p watchableProvider) Watch(ctx context.Context, req WatchRequest) (<-chan JobDeltaBatch, error) {
	if p.watch != nil {
		return p.watch(ctx, req)
	}
	return nil, nil
}

func spawnCronUnit(t *testing.T, source Source) *unit.TestActor {
	t.Helper()
	actor, err := unit.Spawn(t, Factory(source, SchedulerOptions{
		ShardCount:         8,
		TickResolution:     time.Minute,
		InitDelay:          time.Millisecond,
		RebalanceDelay:     time.Millisecond,
		LeaseTTL:           time.Minute,
		ScanConcurrency:    1,
		ScanPageSize:       2,
		OwnerRingSalt:      "test-cron",
		MaxDispatchPerTick: 10,
	}), unit.WithNodeName(gen.Atom("node-a@127.0.0.1")))
	if err != nil {
		t.Fatalf("spawn cron actor: %v", err)
	}
	actor.ClearEvents()
	return actor
}

func TestCronFactoryInitCallAndEventScheduling(t *testing.T) {
	source := NewManagedSource(NewStaticSource(8), NewMemoryKVStore())
	actor, err := unit.Spawn(t, Factory(source, SchedulerOptions{
		InitDelay:      time.Millisecond,
		RebalanceDelay: time.Millisecond,
	}), unit.WithNodeName(gen.Atom("node-a@127.0.0.1")))
	if err != nil {
		t.Fatalf("spawn cron actor: %v", err)
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageInit{}).
		Once().
		Assert()

	result := actor.Call(gen.PID{}, "inspect")
	if result.Error != nil {
		t.Fatalf("inspect call failed: %v", result.Error)
	}
	if _, ok := result.Response.(map[string]string); !ok {
		t.Fatalf("expected inspect map response, got %#v", result.Response)
	}
	result = actor.Call(gen.PID{}, struct{}{})
	if !errors.Is(result.Error, gen.ErrUnsupported) {
		t.Fatalf("expected unsupported call, got %v", result.Error)
	}

	actor.ClearEvents()
	process := actor.Behavior().(*Process)
	if err := process.HandleEvent(gen.MessageEvent{Message: events.EventNodeJoined{}}); err != nil {
		t.Fatalf("handle event: %v", err)
	}
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageRebalance{}).
		Once().
		Assert()
}

func TestCronHelperBranches(t *testing.T) {
	if got := slotKey(time.Unix(60, 0), 0); got != 1 {
		t.Fatalf("zero resolution should default to one minute slot, got %d", got)
	}

	pending := make([]pendingDispatch, 5)
	trimmed := trimPendingQueue(nil, pending, 3)
	if len(trimmed) != 3 {
		t.Fatalf("expected trimmed pending queue length 3, got %d", len(trimmed))
	}

	safeWarning(nil, "ignored")
	safeError(nil, "ignored")

	p := &Process{}
	if p.provider() != nil {
		t.Fatal("nil source should return nil provider")
	}
	if p.backend() != nil {
		t.Fatal("nil source should return nil backend")
	}
	if err := p.rebalance(); err != nil {
		t.Fatalf("nil source rebalance should be a no-op, got %v", err)
	}
}

func TestCronManagedSourceNilAndDefaults(t *testing.T) {
	if NewManagedSource(nil, nil).Provider() != nil {
		t.Fatal("nil provider should be returned as nil")
	}
	var source *ManagedSource
	if source.Provider() != nil || source.StateStore() != nil {
		t.Fatal("nil managed source should return nil dependencies")
	}

	opts := SchedulerOptions{}.WithDefaults()
	if opts.ShardCount == 0 || opts.TickResolution == 0 || opts.ScanConcurrency == 0 || opts.OwnerRingSalt == "" {
		t.Fatalf("defaults not populated: %+v", opts)
	}
}

func TestCronScheduleAndRingHelpers(t *testing.T) {
	actor := spawnCronUnit(t, NewManagedSource(NewStaticSource(8), NewMemoryKVStore()))
	process := actor.Behavior().(*Process)

	process.scheduleNextTick()
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageTick{}).
		Once().
		Assert()

	actor.ClearEvents()
	process.scheduleRebalance()
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageRebalance{}).
		Once().
		Assert()

	if err := process.refreshRing([]gen.Atom{"node-b@127.0.0.1"}); err != nil {
		t.Fatalf("refresh ring: %v", err)
	}
	if len(process.ring.prevMembers) != 2 {
		t.Fatalf("expected self and remote ring members, got %d", len(process.ring.prevMembers))
	}
}

func TestCronApplyWatchBatchUpsertDeleteAndStaleGeneration(t *testing.T) {
	process := &Process{
		options: SchedulerOptions{TickResolution: time.Minute}.WithDefaults(),
		owned:   map[uint32]*shardRuntime{},
	}
	runtime := newShardRuntime(3, 7)
	runtime.Activate()
	process.owned[3] = runtime

	process.applyWatchBatch(messageWatchBatch{
		shard:      3,
		generation: 6,
		batch: JobDeltaBatch{Deltas: []JobDelta{{
			Type: JobDeltaUpsert,
			Job:  JobSpec{ID: "ignored", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")},
		}}},
	})
	if len(runtime.jobs) != 0 {
		t.Fatal("stale generation should be ignored")
	}

	process.applyWatchBatch(messageWatchBatch{
		shard:      3,
		generation: 7,
		batch: JobDeltaBatch{
			Cursor: "cursor-1",
			Deltas: []JobDelta{{
				Type: JobDeltaUpsert,
				Job:  JobSpec{ID: "job-1", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")},
			}},
		},
	})
	if _, ok := runtime.jobs["job-1"]; !ok || runtime.cursor != "cursor-1" {
		t.Fatalf("expected upserted job and cursor, jobs=%#v cursor=%s", runtime.jobs, runtime.cursor)
	}

	process.applyWatchBatch(messageWatchBatch{
		shard:      3,
		generation: 7,
		batch: JobDeltaBatch{Deltas: []JobDelta{{
			Type:  JobDeltaDelete,
			JobID: "job-1",
		}}},
	})
	if _, ok := runtime.jobs["job-1"]; ok {
		t.Fatal("expected delete delta to remove job")
	}
}

func TestCronWatchDownStopsOrRestartsWatcher(t *testing.T) {
	watchCh := make(chan JobDeltaBatch)
	actor := spawnCronUnit(t, NewManagedSource(watchableProvider{
		watch: func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error) {
			return watchCh, nil
		},
	}, NewMemoryKVStore()))
	process := actor.Behavior().(*Process)
	runtime := newShardRuntime(2, 9)
	runtime.watchPID = gen.PID{Node: actor.Process().Node().Name(), ID: 42}
	process.owned[2] = runtime

	process.handleWatchDown(gen.MessageDownPID{PID: runtime.watchPID, Reason: gen.TerminateReasonNormal})
	if runtime.watchPID != (gen.PID{}) {
		t.Fatal("normal watcher down should clear watch pid")
	}

	runtime.watchPID = gen.PID{Node: actor.Process().Node().Name(), ID: 43}
	actor.ClearEvents()
	process.handleWatchDown(gen.MessageDownPID{PID: runtime.watchPID, Reason: errors.New("crash")})
	if runtime.watchPID == (gen.PID{}) {
		t.Fatal("unexpected watcher crash should restart watcher")
	}
	actor.ShouldSpawn().Once().Assert()
}

func TestCronRefreshShardLeasesRenewsAndDropsLostLease(t *testing.T) {
	store := NewMemoryKVStore()
	source := NewManagedSource(NewStaticSource(8), store)
	actor := spawnCronUnit(t, source)
	process := actor.Behavior().(*Process)
	self := actor.Process().Node().Name()
	backend := newStateBackend(store)

	runtime := newShardRuntime(1, 1)
	runtime.lease = ShardLease{Shard: 1, Owner: self, Epoch: 1, ExpiresAt: time.Now().UTC().Add(time.Millisecond), Acquired: true}
	process.owned[1] = runtime
	if _, err := backend.AcquireShardLease(context.Background(), 1, self, time.Millisecond); err != nil {
		t.Fatalf("seed lease: %v", err)
	}
	if err := process.refreshShardLeases(); err != nil {
		t.Fatalf("refresh lease: %v", err)
	}
	if process.owned[1].lease.ExpiresAt.Before(time.Now().UTC()) {
		t.Fatal("expected lease to be renewed")
	}

	other := gen.Atom("node-b@127.0.0.1")
	expired, err := marshalState(leaseRecord{Owner: other, Epoch: 2, ExpiresAt: time.Now().UTC().Add(time.Minute)})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	if err := store.Put(context.Background(), leaseKey(2), expired); err != nil {
		t.Fatalf("put lease: %v", err)
	}
	lostRuntime := newShardRuntime(2, 1)
	lostRuntime.watchPID = gen.PID{Node: self, ID: 99}
	lostRuntime.lease = ShardLease{Shard: 2, Owner: self, Epoch: 1, ExpiresAt: time.Now().UTC().Add(time.Millisecond), Acquired: true}
	process.owned[2] = lostRuntime
	actor.ClearEvents()
	if err := process.refreshShardLeases(); err != nil {
		t.Fatalf("refresh lost lease: %v", err)
	}
	if _, ok := process.owned[2]; ok {
		t.Fatal("lost lease should remove owned shard")
	}
}

func TestCronWatchActorDrainsBatchesAndTerminates(t *testing.T) {
	parent := gen.PID{Node: gen.Atom("parent@127.0.0.1"), ID: 10}
	ch := make(chan JobDeltaBatch, 1)
	ch <- JobDeltaBatch{Shard: 4, Cursor: "c1"}
	close(ch)
	provider := watchableProvider{
		watch: func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error) {
			return ch, nil
		},
	}
	actor, err := unit.Spawn(t, newWatchFactory(parent, provider, 4, 12, WatchRequest{Shards: []uint32{4}}))
	if err != nil {
		t.Fatalf("spawn watch actor: %v", err)
	}
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, messageWatchPoll{})
	actor.ShouldSend().
		To(parent).
		MessageMatching(func(message any) bool {
			msg, ok := message.(messageWatchBatch)
			return ok && msg.shard == 4 && msg.generation == 12 && msg.batch.Cursor == "c1"
		}).
		Once().
		Assert()
	if !actor.IsTerminated() {
		t.Fatal("closed watch channel should terminate actor")
	}
}

func TestCronWatchActorInitHandlesNilAndErrorWatch(t *testing.T) {
	_, err := unit.Spawn(t, newWatchFactory(gen.PID{}, watchableProvider{
		watch: func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error) {
			return nil, errors.New("watch failed")
		},
	}, 1, 1, WatchRequest{}))
	if err == nil {
		t.Fatal("expected watch init error")
	}

	_, err = unit.Spawn(t, newWatchFactory(gen.PID{}, watchableProvider{}, 1, 1, WatchRequest{}))
	if err == nil || !strings.Contains(err.Error(), gen.TerminateReasonNormal.Error()) {
		t.Fatalf("expected normal termination from nil watch channel, got %v", err)
	}
}

func TestCronRebalanceLoadsOwnedShardFromStaticSource(t *testing.T) {
	source := NewManagedSource(NewStaticSource(1,
		JobSpec{ID: "job-1", ShardKey: "job-1", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")},
	), NewMemoryKVStore())
	actor := spawnCronUnit(t, source)
	process := actor.Behavior().(*Process)
	process.options.ShardCount = 1
	process.registrar = &cronTestRegistrar{}

	if err := process.rebalance(); err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	runtime, ok := process.owned[0]
	if !ok {
		t.Fatal("expected shard 0 to be owned")
	}
	if _, ok := runtime.jobs["job-1"]; !ok {
		t.Fatalf("expected job loaded into runtime: %#v", runtime.jobs)
	}
	if runtime.state != shardStateActive {
		t.Fatalf("expected active runtime, got %v", runtime.state)
	}
}

func TestCronRebalanceErrorsAndRemovesUnwantedShard(t *testing.T) {
	actor := spawnCronUnit(t, NewManagedSource(NewStaticSource(1), NewMemoryKVStore()))
	process := actor.Behavior().(*Process)
	process.registrar = &cronTestRegistrar{err: errors.New("nodes failed")}
	if err := process.rebalance(); err == nil {
		t.Fatal("expected registrar nodes error")
	}

	process.registrar = &cronTestRegistrar{nodes: []gen.Atom{"node-b@127.0.0.1"}}
	process.options.ShardCount = 1
	process.ring = &consistentState{prevMembers: make(map[gen.Atom]ringMember), ring: makeRing()}
	runtime := newShardRuntime(0, 1)
	runtime.watchPID = gen.PID{Node: actor.Process().Node().Name(), ID: 50}
	process.owned[0] = runtime
	if err := process.rebalance(); err != nil {
		t.Fatalf("rebalance after registrar recovery: %v", err)
	}
}

func TestCronCollectShardSlotClaimAndReplay(t *testing.T) {
	store := NewMemoryKVStore()
	source := NewManagedSource(NewStaticSource(1), store)
	actor := spawnCronUnit(t, source)
	process := actor.Behavior().(*Process)
	self := actor.Process().Node().Name()
	backend := newStateBackend(store)
	lease, err := backend.AcquireShardLease(context.Background(), 0, self, time.Minute)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	runtime := newShardRuntime(0, 1)
	runtime.lease = lease
	runtime.Activate()
	process.owned[0] = runtime
	job, err := compileJob(JobSpec{ID: "job-1", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}
	slotTime := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)

	if err := process.collectShardSlot(runtime, slotTime, []*CompiledJob{job}); err != nil {
		t.Fatalf("collect slot: %v", err)
	}
	if len(process.pending) != 1 || process.pending[0].job.Spec.ID != "job-1" {
		t.Fatalf("expected pending dispatch, got %#v", process.pending)
	}
	slot := slotKey(slotTime, process.options.TickResolution)
	if _, ok := runtime.completedSlots[slot]; ok {
		t.Fatal("slot with pending dispatch should not be completed")
	}

	if err := process.collectShardSlot(runtime, slotTime.Add(time.Minute), nil); err != nil {
		t.Fatalf("collect empty slot: %v", err)
	}
	if _, ok := runtime.completedSlots[slotKey(slotTime.Add(time.Minute), process.options.TickResolution)]; !ok {
		t.Fatal("empty slot should be marked completed when checkpoint cannot yet advance")
	}
}

func TestCronHandleTickCollectsDueJobs(t *testing.T) {
	store := NewMemoryKVStore()
	source := NewManagedSource(NewStaticSource(1), store)
	actor := spawnCronUnit(t, source)
	process := actor.Behavior().(*Process)
	self := actor.Process().Node().Name()
	backend := newStateBackend(store)
	lease, err := backend.AcquireShardLease(context.Background(), 0, self, time.Minute)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	runtime := newShardRuntime(0, 1)
	runtime.lease = lease
	runtime.loadedAt = time.Date(2026, 3, 27, 9, 59, 0, 0, time.UTC)
	runtime.Activate()
	job, err := compileJob(JobSpec{ID: "job-1", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}
	if err := runtime.Upsert(job, runtime.loadedAt, time.Minute); err != nil {
		t.Fatalf("upsert job: %v", err)
	}
	runtime.slots.Put(job.Spec.ID, slotKey(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC), time.Minute))
	process.owned[0] = runtime
	process.lastTick = time.Date(2026, 3, 27, 9, 59, 0, 0, time.UTC)

	if err := process.handleTick(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("handle tick: %v", err)
	}
	if !process.lastTick.Equal(time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected last tick to advance, got %s", process.lastTick)
	}
}

func TestCronHandleMessageBranchesThroughMailbox(t *testing.T) {
	source := NewManagedSource(NewStaticSource(1), NewMemoryKVStore())
	actor := spawnCronUnit(t, source)
	process := actor.Behavior().(*Process)
	process.options.ShardCount = 1
	process.registrar = &cronTestRegistrar{}

	actor.SendMessage(gen.PID{}, messageInit{})
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageTick{}).
		Once().
		Assert()

	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, messageTick{})
	actor.ShouldSend().
		To(actor.Process().PID()).
		Message(messageTick{}).
		Once().
		Assert()

	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, messageRebalance{})
	actor.ShouldNotTerminate().Assert()

	runtime := newShardRuntime(2, 3)
	runtime.Activate()
	process.owned[2] = runtime
	actor.SendMessage(gen.PID{}, messageWatchBatch{
		shard:      2,
		generation: 3,
		batch: JobDeltaBatch{Deltas: []JobDelta{{
			Type: JobDeltaUpsert,
			Job:  JobSpec{ID: "job-mailbox", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")},
		}}},
	})
	if _, ok := runtime.jobs["job-mailbox"]; !ok {
		t.Fatal("expected watch batch to upsert job through mailbox")
	}

	runtime.watchPID = gen.PID{Node: actor.Process().Node().Name(), ID: 77}
	actor.SendMessage(gen.PID{}, gen.MessageDownPID{PID: runtime.watchPID, Reason: gen.TerminateReasonNormal})
	if runtime.watchPID != (gen.PID{}) {
		t.Fatal("expected down pid message to clear watcher")
	}
}

func TestCronKVDeleteScheduleDueAndSlotMarkers(t *testing.T) {
	store := NewMemoryKVStore()
	if err := store.Put(context.Background(), "key", []byte("value")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Delete(context.Background(), "key"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	entry, err := store.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if entry.Found {
		t.Fatal("deleted key should not be found")
	}
	if err := store.Delete(context.Background(), "missing"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}

	job, err := compileJob(JobSpec{ID: "job", Schedule: "* * * * *", TriggerProcess: gen.Atom("proc")})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}
	now := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	if !job.Schedule.IsDueAt(now) {
		t.Fatal("job should be due at minute boundary")
	}
	runtime := newShardRuntime(1, 1)
	runtime.jobs[job.Spec.ID] = job
	runtime.slots.Put(job.Spec.ID, slotKey(now, time.Minute))
	due := runtime.DueJobsAt(now, time.Minute)
	if len(due) != 1 || due[0].Spec.ID != job.Spec.ID {
		t.Fatalf("unexpected due jobs: %+v", due)
	}
	runtime.MarkSlotClaimed(10, 2)
	runtime.MarkSlotAcked(10)
	if runtime.pendingBySlot[10] != 1 {
		t.Fatalf("expected one pending dispatch, got %#v", runtime.pendingBySlot)
	}
	runtime.MarkSlotAcked(10)
	if _, ok := runtime.completedSlots[10]; !ok {
		t.Fatal("expected slot to be completed after final ack")
	}
}
