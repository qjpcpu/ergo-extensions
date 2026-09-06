package system

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"ergo.services/ergo/gen"
)

type routeLeaseManager struct {
	router          *ActorRouter
	jobs            chan func()
	stop            chan struct{}
	renewStop       chan struct{}
	once, renewOnce sync.Once
	started         time.Time
	resolution      time.Duration
	cursor          int64
	wheel           map[int64]map[*localRouteInstance]struct{}
}

func newRouteLeaseManager(r *ActorRouter) *routeLeaseManager {
	return &routeLeaseManager{router: r, jobs: make(chan func(), r.options.RouteChangeQueueSize), stop: make(chan struct{}), renewStop: make(chan struct{}), started: time.Now(), resolution: max(time.Nanosecond, min(100*time.Millisecond, r.options.SessionRenewInterval/10)), wheel: make(map[int64]map[*localRouteInstance]struct{})}
}
func (m *routeLeaseManager) start() {
	for range m.router.options.RouteChangeWorkers {
		go m.worker()
	}
	go m.heartbeat()
	go m.watchdog()
}
func (m *routeLeaseManager) close()     { m.once.Do(func() { m.stopRenew(); close(m.stop) }) }
func (m *routeLeaseManager) stopRenew() { m.renewOnce.Do(func() { close(m.renewStop) }) }
func routeWork[T any](r *ActorRouter, ctx context.Context, fn func() (T, error)) (value T, err error) {
	r.mu.Lock()
	m := r.manager
	closed := r.state == routerLost || r.state == routerClosed
	r.mu.Unlock()
	if closed {
		return value, ErrActorRouterClosed
	}
	if m == nil {
		return value, ErrActorRouterUnbound
	}
	if err := ctx.Err(); err != nil {
		return value, notApplied(err)
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	job := func() { v, e := safeRouteCall(fn); done <- result{v, e} }
	select {
	case m.jobs <- job:
	case <-ctx.Done():
		return value, notApplied(ctx.Err())
	default:
		return value, ErrActorRouterBusy
	}
	select {
	case got := <-done:
		return got.value, got.err
	case <-ctx.Done():
		return value, ctx.Err()
	case <-m.stop:
		return value, ErrActorRouterClosed
	}
}
func (m *routeLeaseManager) worker() {
	tick := time.NewTicker(m.resolution)
	defer tick.Stop()
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		// Give cleanup bounded priority without starving route admission.
		released := false
		for range 8 {
			if !m.releaseOne() {
				break
			}
			released = true
		}
		if released {
			select {
			case <-m.stop:
				return
			case job := <-m.jobs:
				job()
			default:
			}
			continue
		}
		select {
		case <-m.stop:
			return
		case job := <-m.jobs:
			job()
		case <-tick.C:
		}
	}
}
func (m *routeLeaseManager) releaseOne() bool {
	r := m.router
	r.mu.Lock()
	e := r.pending.Front()
	if e == nil {
		r.mu.Unlock()
		return false
	}
	i := e.Value.(*localRouteInstance)
	if time.Now().Before(i.retryAt) {
		r.pending.MoveToBack(e)
		r.mu.Unlock()
		return false
	}
	r.pending.Remove(e)
	i.release = nil
	i.releasing = true
	id := r.session
	lost := r.state == routerLost || r.state == routerClosed
	r.mu.Unlock()
	var err error
	if !lost {
		ctx, cancel := r.operationContext(context.Background())
		err = safeRouteError(func() error { return r.persistence.ReleaseRoute(ctx, id, i.key, i.pid) })
		cancel()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i.releasing = false
	if err != nil && r.state != routerLost && r.state != routerClosed {
		r.releaseFailures++
		i.retryAt = time.Now().Add(100 * time.Millisecond)
		i.release = r.pending.PushBack(i)
	} else {
		r.releaseCount--
		delete(r.instances, i.pid)
	}
	return true
}
func (m *routeLeaseManager) heartbeat() {
	r := m.router
	var jitter uint64
	for {
		timer := time.NewTimer(renewalDelay(gen.Atom(r.session), gen.PID{}, r.options.SessionRenewInterval, &jitter))
		select {
		case <-m.renewStop:
			timer.Stop()
			return
		case <-timer.C:
		}
		r.mu.Lock()
		active := (r.state == routerActive || r.state == routerDraining) && time.Now().Before(r.deadline)
		id := r.session
		r.mu.Unlock()
		if !active {
			r.lose()
			return
		}
		start := time.Now()
		ctx, cancel := r.operationContext(context.Background())
		lease, err := safeRouteCall(func() (SessionLease, error) { return r.persistence.RenewSession(ctx, id, r.options.SessionTTL) })
		cancel()
		now := time.Now()
		r.mu.Lock()
		expired := !now.Before(r.deadline)
		if err != nil {
			r.renewFailures++
		}
		active = r.state == routerActive || r.state == routerDraining
		if active && !expired && err == nil {
			deadline := start.Add(lease.ValidFor - r.options.LeaseSafetyMargin)
			if now.Before(deadline) {
				r.deadline = deadline
			} else {
				expired = true
			}
		}
		r.mu.Unlock()
		if expired || errors.Is(err, ErrSessionLost) {
			r.lose()
			return
		}
		if !active {
			return
		}
	}
}
func (m *routeLeaseManager) scheduleLocked(i *localRouteInstance) {
	m.removeLocked(i)
	slot := int64((i.deadline.Sub(m.started) + m.resolution - 1) / m.resolution)
	if slot <= m.cursor {
		slot = m.cursor + 1
	}
	i.slot = slot
	if m.wheel[slot] == nil {
		m.wheel[slot] = make(map[*localRouteInstance]struct{})
	}
	m.wheel[slot][i] = struct{}{}
}
func (m *routeLeaseManager) removeLocked(i *localRouteInstance) {
	if bucket := m.wheel[i.slot]; bucket != nil {
		delete(bucket, i)
		if len(bucket) == 0 {
			delete(m.wheel, i.slot)
		}
	}
	i.slot = 0
}
func (m *routeLeaseManager) expire(now time.Time) []gen.PID {
	r := m.router
	r.mu.Lock()
	defer r.mu.Unlock()
	pids := make([]gen.PID, 0, 128)
	target := int64(now.Sub(m.started) / m.resolution)
	for steps := 0; m.cursor < target && steps < 256; steps++ {
		slot := m.cursor + 1
		bucket := m.wheel[slot]
		for i := range bucket {
			delete(bucket, i)
			i.slot = 0
			if !i.stopped {
				i.stopped = true
				pids = append(pids, i.pid)
			}
			if len(pids) == cap(pids) {
				return pids
			}
		}
		delete(m.wheel, slot)
		m.cursor = slot
	}
	return pids
}
func (m *routeLeaseManager) watchdog() {
	tick := time.NewTicker(m.resolution)
	defer tick.Stop()
	r := m.router
	for {
		select {
		case <-m.stop:
			return
		case now := <-tick.C:
			r.mu.Lock()
			expired := !now.Before(r.deadline)
			active := r.state == routerActive || r.state == routerDraining
			node := r.node
			r.mu.Unlock()
			if active && expired {
				r.lose()
			}
			for {
				for _, pid := range m.expire(now) {
					_ = node.Kill(pid)
				}
				r.mu.Lock()
				caughtUp := m.cursor >= int64(now.Sub(m.started)/m.resolution)
				r.mu.Unlock()
				if caughtUp {
					break
				}
				select {
				case <-m.stop:
					return
				default:
				}
				runtime.Gosched()
			}
		}
	}
}
