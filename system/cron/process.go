package cron

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"

	"github.com/buraksezer/consistent"
	"github.com/qjpcpu/registrar/events"
)

type messageInit struct{}
type messageTick struct{}
type messageRebalance struct{}
type messageWatchBatch struct {
	shard      uint32
	generation int64
	batch      JobDeltaBatch
}

type pendingDispatch struct {
	job         *CompiledJob
	shard       uint32
	scheduledAt time.Time
	dispatchKey string
	slot        int64
}

type Process struct {
	act.Actor
	source          Source
	options         SchedulerOptions
	trigger         Trigger
	registrar       gen.Registrar
	ring            *consistentState
	owned           map[uint32]*shardRuntime
	pending         []pendingDispatch
	nextGeneration  int64
	lastTick        time.Time
	cancelTick      gen.CancelFunc
	cancelRebalance gen.CancelFunc
}

type consistentState struct {
	prevNodes map[gen.Atom]struct{}
	ring      *consistent.Consistent
}

func Factory(source Source, options SchedulerOptions) gen.ProcessFactory {
	opts := options.WithDefaults()
	return func() gen.ProcessBehavior {
		return &Process{
			source:  source,
			options: opts,
			trigger: LocalTrigger{Batch: opts.EnableBatchTrigger},
			ring: &consistentState{
				prevNodes: make(map[gen.Atom]struct{}),
				ring:      makeRing(),
			},
			owned: make(map[uint32]*shardRuntime),
		}
	}
}

func (p *Process) Init(args ...any) error {
	if p.source == nil {
		return nil
	}
	_, err := p.SendAfter(p.PID(), messageInit{}, p.options.InitDelay)
	return err
}

func (p *Process) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case messageInit:
		if err := p.setupRegistrarMonitoring(); err != nil {
			_, _ = p.SendAfter(p.PID(), messageInit{}, p.options.RebalanceDelay)
			return nil
		}
		if err := p.rebalance(); err != nil {
			p.Log().Error("initial cron rebalance failed: %v", err)
			p.scheduleRebalance()
		}
		p.scheduleNextTick()
	case messageTick:
		p.scheduleNextTick()
		if err := p.handleTick(time.Now().UTC()); err != nil {
			p.Log().Error("cron tick failed: %v", err)
		}
	case messageRebalance:
		if err := p.rebalance(); err != nil {
			p.Log().Error("cron rebalance failed: %v", err)
			p.scheduleRebalance()
		}
	case messageWatchBatch:
		p.applyWatchBatch(msg)
	case gen.MessageDownPID:
		p.handleWatchDown(msg)
	}
	return nil
}

func (p *Process) HandleEvent(event gen.MessageEvent) error {
	switch event.Message.(type) {
	case events.EventNodeJoined, events.EventNodeLeft:
		p.scheduleRebalance()
	}
	return nil
}

func (p *Process) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch req := request.(type) {
	case string:
		if req == "inspect" {
			return p.inspect(), nil
		}
	case InspectRequest:
		return p.inspect(), nil
	}
	return nil, gen.ErrUnsupported
}

func (p *Process) setupRegistrarMonitoring() error {
	if p.registrar != nil {
		return nil
	}
	registrar, err := p.Node().Network().Registrar()
	if err != nil {
		return err
	}
	event, err := registrar.Event()
	if err != nil {
		return err
	}
	if _, err := p.MonitorEvent(event); err != nil {
		return err
	}
	p.registrar = registrar
	return nil
}

func (p *Process) provider() JobProvider {
	if p.source == nil {
		return nil
	}
	return p.source.Provider()
}

func (p *Process) backend() *stateBackend {
	if p.source == nil {
		return nil
	}
	return newStateBackend(p.source.StateStore())
}

func (p *Process) scheduleNextTick() {
	if p.cancelTick != nil {
		p.cancelTick()
		p.cancelTick = nil
	}
	now := time.Now().UTC()
	next := now.Truncate(p.options.TickResolution).Add(p.options.TickResolution)
	delay := time.Until(next)
	cancel, err := p.SendAfter(p.PID(), messageTick{}, delay)
	if err == nil {
		p.cancelTick = cancel
	}
}

func (p *Process) scheduleRebalance() {
	if p.cancelRebalance != nil {
		p.cancelRebalance()
		p.cancelRebalance = nil
	}
	cancel, err := p.SendAfter(p.PID(), messageRebalance{}, p.options.RebalanceDelay)
	if err == nil {
		p.cancelRebalance = cancel
	}
}

func (p *Process) rebalance() error {
	if p.source == nil {
		return nil
	}
	if p.registrar == nil {
		return errors.New("cron scheduler missing registrar")
	}
	nodes, err := p.registrar.Nodes()
	if err != nil {
		return err
	}
	if err := p.refreshRing(nodes); err != nil {
		return err
	}

	desired := make(map[uint32]struct{})
	self := p.Node().Name()
	for shard := uint32(0); shard < p.options.ShardCount; shard++ {
		if shardOwner(p.ring.ring, shard) == self {
			desired[shard] = struct{}{}
		}
	}

	for shard, runtime := range p.owned {
		if _, ok := desired[shard]; ok {
			continue
		}
		p.stopShardWatch(runtime)
		delete(p.owned, shard)
	}
	for shard := range desired {
		if _, ok := p.owned[shard]; ok {
			continue
		}
		if err := p.loadShard(shard); err != nil {
			return err
		}
	}
	return nil
}

func (p *Process) refreshRing(nodes []gen.Atom) error {
	nodesMap := make(map[gen.Atom]struct{}, len(nodes)+1)
	for _, node := range nodes {
		nodesMap[node] = struct{}{}
		if _, ok := p.ring.prevNodes[node]; !ok {
			p.ring.ring.Add(ringMember(node))
		}
	}
	self := p.Node().Name()
	nodesMap[self] = struct{}{}
	if _, ok := p.ring.prevNodes[self]; !ok {
		p.ring.ring.Add(ringMember(self))
	}
	for node := range p.ring.prevNodes {
		if _, ok := nodesMap[node]; ok {
			continue
		}
		p.ring.ring.Remove(string(node))
	}
	p.ring.prevNodes = nodesMap
	return nil
}

func (p *Process) loadShard(shard uint32) error {
	p.nextGeneration++
	runtime := newShardRuntime(shard, p.nextGeneration)
	base := time.Now().UTC()
	runtime.loadedAt = base
	provider := p.provider()
	if provider == nil {
		return errors.New("cron scheduler missing job provider")
	}
	backend := p.backend()
	if backend == nil {
		return errors.New("cron scheduler missing KV store")
	}
	lease, err := backend.AcquireShardLease(context.Background(), shard, p.Node().Name(), p.options.LeaseTTL)
	if err != nil {
		return fmt.Errorf("acquire lease for shard %d: %w", shard, err)
	}
	if !lease.Acquired || lease.Owner != p.Node().Name() {
		return fmt.Errorf("shard %d lease owned by %s", shard, lease.Owner)
	}
	runtime.lease = lease

	cursor := ""
	for {
		result, err := provider.ScanShards(context.Background(), ScanShardsRequest{
			Shards: []uint32{shard},
			Cursor: cursor,
			Limit:  p.options.ScanPageSize,
		})
		if err != nil {
			return fmt.Errorf("scan shard %d: %w", shard, err)
		}
		for _, jobSpec := range result.Jobs {
			job, err := compileJob(jobSpec)
			if err != nil {
				p.Log().Error("compile cron job %s failed: %v", jobSpec.ID, err)
				continue
			}
			if err := runtime.Upsert(job, base, p.options.TickResolution); err != nil {
				return err
			}
		}
		if result.Done {
			runtime.cursor = result.NextCursor
			break
		}
		cursor = result.NextCursor
		runtime.cursor = cursor
	}

	currentSlot := slotKey(base, p.options.TickResolution)
	checkpoint, err := backend.GetShardCheckpoint(context.Background(), shard)
	if err != nil {
		return fmt.Errorf("get checkpoint for shard %d: %w", shard, err)
	}
	if checkpoint.Valid {
		runtime.checkpoint = checkpoint.Slot
	} else {
		runtime.checkpoint = currentSlot - 1
	}
	if err := p.replayShard(runtime, runtime.checkpoint+1, currentSlot); err != nil {
		return err
	}

	p.owned[shard] = runtime

	if err := p.startShardWatch(runtime); err != nil {
		delete(p.owned, shard)
		return err
	}
	runtime.Activate()
	return nil
}

func (p *Process) startShardWatch(runtime *shardRuntime) error {
	provider := p.provider()
	if provider == nil {
		return errors.New("cron scheduler missing job provider")
	}
	if source, ok := provider.(OptionalWatchSource); ok && !source.SupportsWatch() {
		return nil
	}
	pid, err := p.Spawn(newWatchFactory(p.PID(), provider, runtime.id, runtime.generation, WatchRequest{
		Shards: []uint32{runtime.id},
		Since:  runtime.cursor,
	}), gen.ProcessOptions{LinkParent: true})
	if err != nil {
		return fmt.Errorf("start watcher for shard %d: %w", runtime.id, err)
	}
	if err := p.MonitorPID(pid); err != nil {
		p.Node().Kill(pid)
		return fmt.Errorf("monitor watcher for shard %d: %w", runtime.id, err)
	}
	runtime.watchPID = pid
	return nil
}

func (p *Process) stopShardWatch(runtime *shardRuntime) {
	if runtime.watchPID == (gen.PID{}) {
		return
	}
	pid := runtime.watchPID
	runtime.watchPID = gen.PID{}
	if err := p.SendExit(pid, gen.TerminateReasonShutdown); err != nil {
		p.Node().Kill(pid)
	}
}

func (p *Process) applyWatchBatch(msg messageWatchBatch) {
	runtime, ok := p.owned[msg.shard]
	if !ok || runtime.generation != msg.generation {
		return
	}
	base := time.Now().UTC()
	for _, delta := range msg.batch.Deltas {
		switch delta.Type {
		case JobDeltaDelete:
			runtime.Delete(delta.JobID)
		case JobDeltaUpsert:
			job, err := compileJob(delta.Job)
			if err != nil {
				p.Log().Error("compile cron delta job %s failed: %v", delta.Job.ID, err)
				continue
			}
			_ = runtime.Upsert(job, base, p.options.TickResolution)
		}
	}
	runtime.cursor = msg.batch.Cursor
}

func (p *Process) handleWatchDown(msg gen.MessageDownPID) {
	for shard, runtime := range p.owned {
		if runtime.watchPID != msg.PID || runtime.generation == 0 {
			continue
		}
		if msg.Reason == gen.TerminateReasonNormal || msg.Reason == gen.TerminateReasonShutdown {
			runtime.watchPID = gen.PID{}
			return
		}
		p.Log().Error("cron watcher for shard %d stopped unexpectedly: %v", shard, msg.Reason)
		runtime.watchPID = gen.PID{}
		if err := p.startShardWatch(runtime); err != nil {
			p.Log().Error("restart watcher for shard %d failed: %v", shard, err)
			p.scheduleRebalance()
		}
		return
	}
}

func (p *Process) handleTick(now time.Time) error {
	if err := p.refreshShardLeases(); err != nil {
		return err
	}
	if p.lastTick.IsZero() {
		p.lastTick = p.bootstrapLastTick(now)
	}
	current := now.Truncate(p.options.TickResolution)
	for next := p.lastTick.Add(p.options.TickResolution); !next.After(current); next = next.Add(p.options.TickResolution) {
		p.collectDue(next)
		p.flushPending()
	}
	p.lastTick = current
	return nil
}

func (p *Process) bootstrapLastTick(now time.Time) time.Time {
	lastTick := now.Truncate(p.options.TickResolution).Add(-p.options.TickResolution)
	for _, runtime := range p.owned {
		if runtime.loadedAt.IsZero() {
			continue
		}
		candidate := runtime.loadedAt.Truncate(p.options.TickResolution).Add(-p.options.TickResolution)
		if candidate.Before(lastTick) {
			lastTick = candidate
		}
	}
	return lastTick
}

func (p *Process) collectDue(slotTime time.Time) {
	for shard, runtime := range p.owned {
		if runtime.state != shardStateActive {
			continue
		}
		if err := p.collectShardSlot(runtime, slotTime, runtime.TakeDue(slotKey(slotTime, p.options.TickResolution))); err != nil {
			p.Log().Error("collect due for shard %d failed: %v", shard, err)
		}
	}
}

func (p *Process) flushPending() {
	if len(p.pending) == 0 {
		return
	}
	limit := p.options.MaxDispatchPerTick
	if limit <= 0 || limit > len(p.pending) {
		limit = len(p.pending)
	}

	dispatch := make([]DispatchJob, 0, limit)
	dispatchedItems := make([]pendingDispatch, 0, limit)
	rest := p.pending[:0]
	for i, item := range p.pending {
		if i < limit {
			dispatch = append(dispatch, DispatchJob{
				JobID:          item.job.Spec.ID,
				TriggerProcess: item.job.Spec.TriggerProcess,
				ScheduledAt:    item.scheduledAt,
				DispatchKey:    item.dispatchKey,
			})
			dispatchedItems = append(dispatchedItems, item)
			continue
		}
		rest = append(rest, item)
	}
	if len(dispatch) > 0 {
		backend := p.backend()
		if backend == nil {
			p.Log().Error("cron scheduler missing KV store")
			p.pending = append(rest, dispatchedItems...)
			return
		}
		failed, err := p.trigger.Fire(p, dispatch)
		failedByID := make(map[string]struct{}, len(failed))
		for _, job := range failed {
			failedByID[job.DispatchKey] = struct{}{}
		}
		ackedByShard := make(map[uint32][]string)
		for _, item := range dispatchedItems {
			if _, ok := failedByID[item.dispatchKey]; ok {
				rest = append(rest, item)
				continue
			}
			ackedByShard[item.shard] = append(ackedByShard[item.shard], item.dispatchKey)
		}
		ackedKeys := make(map[string]struct{})
		ackFailed := make(map[string]struct{})
		for shard, keys := range ackedByShard {
			runtime, ok := p.owned[shard]
			if !ok {
				for _, key := range keys {
					ackedKeys[key] = struct{}{}
				}
				continue
			}
			owner := runtime.lease.Owner
			if owner == "" {
				owner = p.Node().Name()
			}
			if ackErr := backend.AckDispatches(context.Background(), shard, owner, runtime.lease.Epoch, keys); ackErr != nil {
				p.Log().Error("ack cron dispatches for shard %d failed: %v", shard, ackErr)
				for _, key := range keys {
					ackFailed[key] = struct{}{}
				}
				continue
			}
			for _, key := range keys {
				ackedKeys[key] = struct{}{}
			}
			for _, item := range dispatchedItems {
				if item.shard != shard {
					continue
				}
				if item.dispatchKey == "" {
					continue
				}
				if _, ok := ackedKeys[item.dispatchKey]; !ok {
					continue
				}
				runtime.MarkSlotAcked(item.slot)
				if err := p.advanceCheckpoint(runtime); err != nil {
					p.Log().Error("advance checkpoint for shard %d failed: %v", shard, err)
				}
				runtime.Reschedule(item.job, item.scheduledAt, p.options.TickResolution)
			}
		}
		for _, item := range dispatchedItems {
			if _, ok := ackFailed[item.dispatchKey]; ok {
				rest = append(rest, item)
			}
		}
		if err != nil {
			p.Log().Error("dispatch cron jobs failed: %v", err)
		}
	}
	p.pending = rest
}

func (p *Process) refreshShardLeases() error {
	for shard, runtime := range p.owned {
		backend := p.backend()
		if backend == nil {
			return errors.New("cron scheduler missing KV store")
		}
		lease, err := backend.AcquireShardLease(context.Background(), shard, p.Node().Name(), p.options.LeaseTTL)
		if err != nil {
			return err
		}
		if !lease.Acquired || lease.Owner != p.Node().Name() {
			p.stopShardWatch(runtime)
			delete(p.owned, shard)
			p.scheduleRebalance()
			continue
		}
		runtime.lease = lease
	}
	return nil
}

func (p *Process) replayShard(runtime *shardRuntime, fromSlot int64, toSlot int64) error {
	if fromSlot > toSlot {
		return nil
	}
	unit := int64(p.options.TickResolution)
	for slot := fromSlot; slot <= toSlot; slot++ {
		slotTime := time.Unix(0, slot*unit).UTC()
		if err := p.collectShardSlot(runtime, slotTime, runtime.DueJobsAt(slotTime, p.options.TickResolution)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Process) collectShardSlot(runtime *shardRuntime, slotTime time.Time, jobs []*CompiledJob) error {
	slot := slotKey(slotTime, p.options.TickResolution)
	if len(jobs) == 0 {
		runtime.MarkSlotClaimed(slot, 0)
		return p.advanceCheckpoint(runtime)
	}

	claims := make([]DispatchClaim, 0, len(jobs))
	jobByID := make(map[string]*CompiledJob, len(jobs))
	for _, job := range jobs {
		claims = append(claims, DispatchClaim{
			JobID:       job.Spec.ID,
			ScheduledAt: slotTime,
		})
		jobByID[job.Spec.ID] = job
	}
	backend := p.backend()
	if backend == nil {
		return errors.New("cron scheduler missing KV store")
	}
	records, err := backend.ClaimDispatches(context.Background(), runtime.id, p.Node().Name(), runtime.lease.Epoch, claims)
	if err != nil {
		for _, job := range jobs {
			runtime.slots.Put(job.Spec.ID, slot)
		}
		return err
	}

	pendingCount := 0
	for _, record := range records {
		job := jobByID[record.JobID]
		if job == nil {
			continue
		}
		if record.State == DispatchStateAcked {
			runtime.Reschedule(job, slotTime, p.options.TickResolution)
			continue
		}
		p.pending = append(p.pending, pendingDispatch{
			job:         job,
			shard:       runtime.id,
			scheduledAt: slotTime,
			dispatchKey: record.Key,
			slot:        slot,
		})
		pendingCount++
	}
	runtime.MarkSlotClaimed(slot, pendingCount)
	return p.advanceCheckpoint(runtime)
}

func (p *Process) advanceCheckpoint(runtime *shardRuntime) error {
	owner := runtime.lease.Owner
	if owner == "" {
		owner = p.Node().Name()
	}
	backend := p.backend()
	if backend == nil {
		return errors.New("cron scheduler missing KV store")
	}
	for {
		next := runtime.checkpoint + 1
		if _, ok := runtime.completedSlots[next]; !ok {
			return nil
		}
		if err := backend.AdvanceShardCheckpoint(context.Background(), runtime.id, owner, runtime.lease.Epoch, next); err != nil {
			return err
		}
		delete(runtime.completedSlots, next)
		runtime.checkpoint = next
	}
}

func (p *Process) inspect() map[string]string {
	loaded := 0
	for _, runtime := range p.owned {
		loaded += len(runtime.jobs)
	}
	marshal := func(v any) string {
		data, _ := json.Marshal(v)
		return string(data)
	}
	return map[string]string{
		"owned_shards": marshal(len(p.owned)),
		"loaded_jobs":  marshal(loaded),
		"pending_jobs": marshal(len(p.pending)),
	}
}

func slotKey(t time.Time, resolution time.Duration) int64 {
	unit := int64(resolution)
	if unit <= 0 {
		unit = int64(time.Minute)
	}
	return t.UTC().UnixNano() / unit
}
