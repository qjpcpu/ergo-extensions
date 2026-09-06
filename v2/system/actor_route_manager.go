package system

import (
	"context"
	"sync"
	"time"

	"ergo.services/ergo/gen"
)

const (
	routeLeaseShardCount = 64
	routeLeaseWheelSlots = 256
)

type routeLease struct {
	key     gen.Atom
	pid     gen.PID
	next    int64
	jitter  uint64
	slot    uint16
	pending bool
}

type routeLeaseShard struct {
	mu      sync.Mutex
	entries map[gen.Atom]*routeLease
	wheel   [routeLeaseWheelSlots]map[*routeLease]struct{}
}

type routeLeaseJobKind uint8

const (
	routeLeaseRenew routeLeaseJobKind = iota + 1
	routeLeaseRelease
)

type routeLeaseJob struct {
	lease *routeLease
	kind  routeLeaseJobKind
	key   gen.Atom
	pid   gen.PID
}

type routeLeaseManager struct {
	router      *ActorRouter
	shards      []routeLeaseShard
	renewJobs   chan routeLeaseJob
	releaseJobs chan routeLeaseJob
	resolution  time.Duration
	started     time.Time
	cursor      int64
	stop        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	once        sync.Once
	wg          sync.WaitGroup
}

func newRouteLeaseManager(router *ActorRouter) *routeLeaseManager {
	manager := newRouteLeaseManagerState(router)
	manager.wg.Add(1 + router.options.RenewWorkers)
	go manager.schedule()
	for range router.options.RenewWorkers {
		go manager.work()
	}
	return manager
}

func newRouteLeaseManagerState(router *ActorRouter) *routeLeaseManager {
	resolution := routeSchedulerResolution(router.options.RenewInterval)
	started := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	manager := &routeLeaseManager{
		router:      router,
		shards:      make([]routeLeaseShard, routeLeaseShardCount),
		renewJobs:   make(chan routeLeaseJob, router.options.RenewQueueSize),
		releaseJobs: make(chan routeLeaseJob, router.options.ReleaseQueueSize),
		resolution:  resolution,
		started:     started,
		cursor:      routeLeaseTick(started, started, resolution),
		stop:        make(chan struct{}),
		ctx:         ctx, cancel: cancel,
	}
	for index := range manager.shards {
		manager.shards[index].entries = make(map[gen.Atom]*routeLease)
	}
	return manager
}

func (m *routeLeaseManager) track(key gen.Atom, pid gen.PID) {
	lease := &routeLease{key: key, pid: pid}
	delay := renewalDelay(key, pid, m.router.options.RenewInterval, &lease.jitter)
	lease.next = time.Now().Add(delay).Sub(m.started).Nanoseconds()
	shard := m.shard(key)
	shard.mu.Lock()
	if previous, found := shard.entries[key]; found {
		m.removeFromWheelLocked(shard, previous)
	}
	shard.entries[key] = lease
	m.addToWheelLocked(shard, lease)
	shard.mu.Unlock()
}

func (m *routeLeaseManager) untrack(key gen.Atom, pid gen.PID) {
	shard := m.shard(key)
	shard.mu.Lock()
	lease, found := shard.entries[key]
	if found && lease.pid == pid {
		m.removeFromWheelLocked(shard, lease)
		delete(shard.entries, key)
	}
	shard.mu.Unlock()
	if !found || lease.pid != pid {
		return
	}
	select {
	case m.releaseJobs <- routeLeaseJob{kind: routeLeaseRelease, key: key, pid: pid}:
	default:
		m.router.releaseDropped.Add(1)
		if m.router.shouldLogRenewFailure(time.Now()) {
			m.router.boundLogWarning("actor route release queue is full; the lease will expire: key=%s pid=%s", key, pid)
		}
	}
}

func (m *routeLeaseManager) close() {
	m.once.Do(func() {
		close(m.stop)
		m.cancel()
		m.wg.Wait()
		// Renewal has stopped; spend one operation budget draining queued releases.
		ctx, cancel := m.router.operationContext(context.Background())
		defer cancel()
		var drain sync.WaitGroup
		for i := 0; i < m.router.options.RenewWorkers; i++ {
			drain.Add(1)
			go func() {
				defer drain.Done()
				for ctx.Err() == nil {
					select {
					case job := <-m.releaseJobs:
						m.release(ctx, job)
					default:
						return
					}
				}
			}()
		}
		drain.Wait()
		m.router.releaseDropped.Add(uint64(len(m.releaseJobs)))
	})
}

func (m *routeLeaseManager) schedule() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.resolution)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			m.enqueueDue(now)
		case <-m.stop:
			return
		}
	}
}

func (m *routeLeaseManager) enqueueDue(now time.Time) {
	nowTick := routeLeaseTick(now, m.started, m.resolution)
	if nowTick <= m.cursor {
		return
	}
	first := m.cursor + 1
	if nowTick-first+1 > routeLeaseWheelSlots {
		first = nowTick - routeLeaseWheelSlots + 1
	}
	for tick := first; tick <= nowTick; tick++ {
		m.enqueueWheelSlot(int(uint64(tick)%routeLeaseWheelSlots), now)
	}
	m.cursor = nowTick
}

func (m *routeLeaseManager) enqueueWheelSlot(slot int, now time.Time) {
	nowNanos := now.Sub(m.started).Nanoseconds()
	for index := range m.shards {
		shard := &m.shards[index]
		shard.mu.Lock()
		bucket := shard.wheel[slot]
		for lease := range bucket {
			current, found := shard.entries[lease.key]
			if !found || current != lease || int(lease.slot) != slot {
				delete(bucket, lease)
				continue
			}
			if lease.pending || lease.next > nowNanos {
				continue
			}
			delete(bucket, lease)
			select {
			case m.renewJobs <- routeLeaseJob{kind: routeLeaseRenew, key: lease.key, pid: lease.pid, lease: lease}:
				lease.pending = true
			default:
				lease.next = now.Add(m.resolution).Sub(m.started).Nanoseconds()
				m.addToWheelLocked(shard, lease)
			}
		}
		shard.mu.Unlock()
	}
}

func (m *routeLeaseManager) work() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stop:
			return
		default:
		}

		// Give releases priority in bounded bursts, allowing renewals to progress.
		for i := 0; i < 8; i++ {
			select {
			case <-m.stop:
				return
			default:
			}
			select {
			case job := <-m.releaseJobs:
				m.executeSafely(job)
			default:
				i = 8
			}
		}

		select {
		case job := <-m.releaseJobs:
			m.executeSafely(job)
		case job := <-m.renewJobs:
			m.executeSafely(job)
		case <-m.stop:
			return
		}
	}
}

func (m *routeLeaseManager) executeSafely(job routeLeaseJob) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if job.kind == routeLeaseRenew {
				m.rescheduleRenew(job, time.Now())
			}
			if m.router.shouldLogRenewFailure(time.Now()) {
				m.router.boundLogWarning("actor route persistence panic recovered; worker remains available: %v", recovered)
			}
		}
	}()
	m.execute(job)
}

func (m *routeLeaseManager) execute(job routeLeaseJob) {
	switch job.kind {
	case routeLeaseRenew:
		m.renew(job)
	case routeLeaseRelease:
		ctx, cancel := m.router.operationContext(context.Background())
		defer cancel()
		m.release(ctx, job)
	}
}

func (m *routeLeaseManager) renew(job routeLeaseJob) {
	shard := m.shard(job.key)
	shard.mu.Lock()
	original, found := shard.entries[job.key]
	if !found || original.pid != job.pid || (job.lease != nil && original != job.lease) {
		shard.mu.Unlock()
		return
	}
	delay := time.Since(m.started).Nanoseconds() - original.next
	shard.mu.Unlock()
	for old := m.router.maxRenewDelay.Load(); delay > old; old = m.router.maxRenewDelay.Load() {
		if m.router.maxRenewDelay.CompareAndSwap(old, delay) {
			break
		}
	}
	ctx, cancel := m.router.operationContext(m.ctx)
	owned, err := m.router.persistence.Renew(ctx, job.key, job.pid, m.router.options.LeaseTTL)
	cancel()
	now := time.Now()

	shard.mu.Lock()
	lease, found := shard.entries[job.key]
	if !found || lease != original {
		shard.mu.Unlock()
		return
	}
	lease.pending = false
	if err == nil && !owned {
		m.router.leaseLosses.Add(1)
		delete(shard.entries, job.key)
		shard.mu.Unlock()
		if node, err := m.router.boundNode(); err == nil {
			_ = node.Kill(job.pid)
		}
		if m.router.shouldLogRenewFailure(now) {
			m.router.boundLogWarning("actor route lease was lost; other losses are rate limited: key=%s pid=%s", job.key, job.pid)
		}
		return
	}
	m.scheduleNextLocked(shard, job.key, lease, now)
	shard.mu.Unlock()
	if err != nil {
		m.router.renewFailures.Add(1)
	}
	if err != nil && m.router.shouldLogRenewFailure(now) {
		m.router.boundLogWarning("actor route renewal failed; other failures are rate limited: %v", err)
	}
}

func (m *routeLeaseManager) rescheduleRenew(job routeLeaseJob, now time.Time) {
	shard := m.shard(job.key)
	shard.mu.Lock()
	lease, found := shard.entries[job.key]
	if found && lease.pid == job.pid && (job.lease == nil || job.lease == lease) {
		lease.pending = false
		m.scheduleNextLocked(shard, job.key, lease, now)
	}
	shard.mu.Unlock()
}

func (m *routeLeaseManager) scheduleNextLocked(shard *routeLeaseShard, key gen.Atom, lease *routeLease, now time.Time) {
	delay := renewalDelay(key, lease.pid, m.router.options.RenewInterval, &lease.jitter)
	lease.next = now.Add(delay).Sub(m.started).Nanoseconds()
	m.addToWheelLocked(shard, lease)
}

func (m *routeLeaseManager) addToWheelLocked(shard *routeLeaseShard, lease *routeLease) {
	slot := routeLeaseSlot(lease.next, m.resolution)
	lease.slot = uint16(slot)
	if shard.wheel[slot] == nil {
		shard.wheel[slot] = make(map[*routeLease]struct{})
	}
	shard.wheel[slot][lease] = struct{}{}
}

func (m *routeLeaseManager) removeFromWheelLocked(shard *routeLeaseShard, lease *routeLease) {
	if lease.pending {
		return
	}
	delete(shard.wheel[lease.slot], lease)
}

func (m *routeLeaseManager) isTracked(key gen.Atom, pid gen.PID) bool {
	shard := m.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	lease, found := shard.entries[key]
	return found && lease.pid == pid
}

func (m *routeLeaseManager) trackedCount() int {
	total := 0
	for index := range m.shards {
		shard := &m.shards[index]
		shard.mu.Lock()
		total += len(shard.entries)
		shard.mu.Unlock()
	}
	return total
}

func (m *routeLeaseManager) shard(key gen.Atom) *routeLeaseShard {
	index := routeJitterSeed(key, gen.PID{}) % uint64(len(m.shards))
	return &m.shards[index]
}

func routeLeaseTick(at time.Time, started time.Time, resolution time.Duration) int64 {
	return at.Sub(started).Nanoseconds() / int64(resolution)
}

func routeLeaseSlot(nanos int64, resolution time.Duration) int {
	unit := int64(resolution)
	tick := nanos / unit
	if nanos%unit != 0 {
		tick++
	}
	return int(uint64(tick) % routeLeaseWheelSlots)
}

func routeSchedulerResolution(interval time.Duration) time.Duration {
	resolution := interval / 20
	if resolution > time.Second {
		resolution = time.Second
	}
	if resolution < time.Millisecond {
		resolution = time.Millisecond
	}
	if resolution >= interval {
		resolution = interval / 2
		if resolution <= 0 {
			resolution = time.Nanosecond
		}
	}
	return resolution
}

func (m *routeLeaseManager) release(ctx context.Context, job routeLeaseJob) {
	defer func() {
		if v := recover(); v != nil {
			m.router.releaseFailures.Add(1)
			m.router.boundLogWarning("actor route release panic: %v", v)
		}
	}()
	if err := m.router.persistence.Release(ctx, job.key, job.pid); err != nil {
		m.router.releaseFailures.Add(1)
		if m.router.shouldLogRenewFailure(time.Now()) {
			m.router.boundLogWarning("actor route release failed: %v", err)
		}
	}
}
