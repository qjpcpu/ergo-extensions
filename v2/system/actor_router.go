package system

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

var (
	// ErrActorRouteTaken indicates that another live PID owns the route key.
	ErrActorRouteTaken = errors.New("actor route already taken")
	// ErrActorRouterUnbound indicates that the router is not attached to an Ergo node.
	ErrActorRouterUnbound = errors.New("actor router is not bound")
	// ErrActorRouterBound indicates an attempt to bind one router to different nodes.
	ErrActorRouterBound = errors.New("actor router is already bound to another node")
	// ErrActorRouterClosed indicates an attempt to route an actor after shutdown.
	ErrActorRouterClosed = errors.New("actor router is closed")
	// ErrActorRouteKeyEmpty indicates an empty route key.
	ErrActorRouteKeyEmpty = errors.New("actor route key is empty")
	// ErrActorRouteFactoryNil indicates a nil wrapped process factory.
	ErrActorRouteFactoryNil = errors.New("actor route process factory is nil")
	// ErrActorRouteBehaviorNil indicates a nil behavior instance.
	ErrActorRouteBehaviorNil = errors.New("actor route behavior is nil")
	// ErrActorRouteBehaviorMismatch indicates that a route decorator received
	// a behavior from another Ergo behavior family.
	ErrActorRouteBehaviorMismatch = errors.New("actor route behavior type mismatch")
	// ErrActorRoutePersistenceNil indicates a nil persistence implementation.
	ErrActorRoutePersistenceNil = errors.New("actor route persistence is nil")
)

// ActorRouterOptions controls the shared session and independent route lifetime.
type ActorRouterOptions struct {
	SessionTTL           time.Duration
	SessionRenewInterval time.Duration
	OperationTimeout     time.Duration
	LeaseSafetyMargin    time.Duration
	RouteTTL             time.Duration
	RouteChangeWorkers   int
	RouteChangeQueueSize int
	ReleaseQueueSize     int
}

func DefaultActorRouterOptions() ActorRouterOptions {
	return ActorRouterOptions{
		SessionTTL:           30 * time.Second,
		SessionRenewInterval: 10 * time.Second,
		OperationTimeout:     3 * time.Second,
		LeaseSafetyMargin:    3 * time.Second,
		RouteTTL:             24 * time.Hour,
		RouteChangeWorkers:   16,
		RouteChangeQueueSize: 65536,
		ReleaseQueueSize:     65536,
	}
}
func normalizeActorRouterOptions(o ActorRouterOptions) (ActorRouterOptions, error) {
	d := DefaultActorRouterOptions()
	for _, pair := range [][2]*time.Duration{{&o.SessionTTL, &d.SessionTTL}, {&o.SessionRenewInterval, &d.SessionRenewInterval}, {&o.OperationTimeout, &d.OperationTimeout}, {&o.LeaseSafetyMargin, &d.LeaseSafetyMargin}, {&o.RouteTTL, &d.RouteTTL}} {
		if *pair[0] == 0 {
			*pair[0] = *pair[1]
		}
		if *pair[0] < 0 {
			return o, errors.New("actor router durations must be positive")
		}
	}
	for _, pair := range [][2]*int{{&o.RouteChangeWorkers, &d.RouteChangeWorkers}, {&o.RouteChangeQueueSize, &d.RouteChangeQueueSize}, {&o.ReleaseQueueSize, &d.ReleaseQueueSize}} {
		if *pair[0] == 0 {
			*pair[0] = *pair[1]
		}
		if *pair[0] < 0 {
			return o, errors.New("actor router capacities must be positive")
		}
	}
	if o.SessionRenewInterval+o.SessionRenewInterval/10+o.OperationTimeout+o.LeaseSafetyMargin >= o.SessionTTL || o.RouteTTL <= o.LeaseSafetyMargin {
		return o, errors.New("actor router TTL must cover renewal, operation timeout and safety margin")
	}
	return o, nil
}

type routerState uint8

const (
	routerUnbound routerState = iota
	routerActive
	routerDraining
	routerLost
	routerClosed
)

// ActorRouter manages one node session and resolves route validity directly.
type ActorRouter struct {
	persistence                                 ActorRoutePersistence
	options                                     ActorRouterOptions
	bindMu                                      sync.Mutex
	mu                                          sync.Mutex
	node                                        gen.Node
	state                                       routerState
	session                                     SessionID
	deadline                                    time.Time
	manager                                     *routeLeaseManager
	instances                                   map[gen.PID]*localRouteInstance
	pending                                     list.List
	releaseCount                                int
	renewFailures, leaseLosses, releaseFailures uint64
	closeOnce                                   sync.Once
	sessionCloseOnce                            sync.Once
}
type localRouteInstance struct {
	done                                           chan struct{}
	key                                            gen.Atom
	pid                                            gen.PID
	deadline                                       time.Time
	acquiring, writing, acquired, cleanup, stopped bool
	releasing                                      bool
	release                                        *list.Element
	retryAt                                        time.Time
	slot                                           int64
}

func NewActorRouter(p ActorRoutePersistence, o ActorRouterOptions) (*ActorRouter, error) {
	if p == nil {
		return nil, ErrActorRoutePersistenceNil
	}
	o, err := normalizeActorRouterOptions(o)
	if err != nil {
		return nil, err
	}
	return &ActorRouter{persistence: p, options: o, instances: make(map[gen.PID]*localRouteInstance)}, nil
}

// Bind opens a fresh session for one node instance. Binding that same instance is idempotent.
func (r *ActorRouter) Bind(node gen.Node) error {
	if node == nil {
		return errors.New("actor router node is nil")
	}
	r.bindMu.Lock()
	defer r.bindMu.Unlock()
	r.mu.Lock()
	if r.state == routerLost || r.state == routerClosed || r.state == routerDraining {
		r.mu.Unlock()
		return ErrActorRouterClosed
	}
	if r.node != nil {
		same := r.node == node
		r.mu.Unlock()
		if !same {
			return ErrActorRouterBound
		}
		return nil
	}
	r.mu.Unlock()
	ctx, cancel := r.operationContext(context.Background())
	defer cancel()
	start := time.Now()
	lease, err := safeRouteCall(func() (SessionLease, error) { return r.persistence.OpenSession(ctx, node.Name(), r.options.SessionTTL) })
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.node = node
	r.session = lease.SessionID
	r.deadline = start.Add(lease.ValidFor - r.options.LeaseSafetyMargin)
	if r.state != routerUnbound || ctx.Err() != nil || !time.Now().Before(r.deadline) {
		if r.state == routerUnbound {
			r.state = routerLost
		}
		r.mu.Unlock()
		r.closeSession()
		return ErrSessionLost
	}
	r.state = routerActive
	r.manager = newRouteLeaseManager(r)
	m := r.manager
	r.mu.Unlock()
	m.start()
	return nil
}
func (r *ActorRouter) boundNode() (gen.Node, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.node == nil {
		return nil, ErrActorRouterUnbound
	}
	return r.node, nil
}
func (r *ActorRouter) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, r.options.OperationTimeout)
}
func notApplied(err error) error { return errors.Join(ErrRouteNotApplied, err) }
func safeRouteCall[T any](f func() (T, error)) (v T, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("actor route persistence panic: %v", p)
		}
	}()
	return f()
}
func safeRouteError(f func() error) error {
	_, err := safeRouteCall(func() (struct{}, error) { return struct{}{}, f() })
	return err
}

// Drain stops admitting new routed actors while existing cleanup keeps its session.
func (r *ActorRouter) Drain() {
	r.mu.Lock()
	if r.state == routerActive || r.state == routerUnbound {
		r.state = routerDraining
	}
	r.mu.Unlock()
}

// Close stops local route management and closes the shared session.
// Call after stopping the node; business callbacks must finish cooperatively.
func (r *ActorRouter) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		m := r.manager
		r.state = routerClosed
		r.mu.Unlock()
		if m != nil {
			m.close()
		}
		r.closeSession()
	})
}

// Session closure is independent of actor callbacks and route workers.
func (r *ActorRouter) closeSession() {
	r.mu.Lock()
	id, node := r.session, r.node
	r.mu.Unlock()
	if id == "" {
		return
	}
	r.sessionCloseOnce.Do(func() {
		ctx, cancel := r.operationContext(context.Background())
		defer cancel()
		if err := safeRouteError(func() error { return r.persistence.CloseSession(ctx, id) }); err != nil {
			node.Log().Warning("close actor route session %s failed: %v", id, err)
		}
	})
}

func (r *ActorRouter) lose() {
	r.mu.Lock()
	if r.state == routerLost || r.state == routerClosed {
		r.mu.Unlock()
		return
	}
	r.state = routerLost
	r.leaseLosses++
	m := r.manager
	r.mu.Unlock()
	if m != nil {
		m.stopRenew()
	}
	go r.closeSession()
	// Walk in bounded batches so a large node never holds the router lock while killing.
	for {
		pids := make([]gen.PID, 0, 128)
		r.mu.Lock()
		for _, i := range r.instances {
			if !i.stopped {
				i.stopped = true
				pids = append(pids, i.pid)
				if len(pids) == cap(pids) {
					break
				}
			}
		}
		node := r.node
		r.mu.Unlock()
		for _, pid := range pids {
			_ = node.Kill(pid)
		}
		if len(pids) < cap(pids) {
			return
		}
	}
}
func (r *ActorRouter) valid(snapshot RouteSnapshot) (bool, error) {
	if snapshot.ValidFor <= 0 || !snapshot.SessionValid {
		return false, nil
	}
	node, err := r.boundNode()
	if err != nil {
		return false, err
	}
	if snapshot.Owner.PID.Node == node.Name() {
		return node.IsAlive(), nil
	}
	network := node.Network()
	if network == nil {
		return false, gen.ErrNoRoute
	}
	registrar, err := network.Registrar()
	if err != nil {
		return false, err
	}
	nodes, err := registrar.Nodes()
	if err != nil {
		return false, err
	}
	for _, name := range nodes {
		if name == snapshot.Owner.PID.Node {
			return true, nil
		}
	}
	return false, nil
}
func (r *ActorRouter) lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if key == "" {
		return gen.PID{}, false, ErrActorRouteKeyEmpty
	}
	if _, err := r.boundNode(); err != nil {
		return gen.PID{}, false, err
	}
	r.mu.Lock()
	closed := r.state == routerLost || r.state == routerClosed
	r.mu.Unlock()
	if closed {
		return gen.PID{}, false, ErrActorRouterClosed
	}
	op, cancel := r.operationContext(ctx)
	defer cancel()
	if err := op.Err(); err != nil {
		return gen.PID{}, false, err
	}
	type result struct {
		pid   gen.PID
		found bool
	}
	value, err := safeRouteCall(func() (result, error) {
		snapshot, found, err := r.persistence.ReadRoute(op, key)
		if err != nil || !found {
			return result{}, err
		}
		valid, err := r.valid(snapshot)
		return result{snapshot.Owner.PID, valid}, err
	})
	if err != nil || !value.found {
		return gen.PID{}, false, err
	}
	return value.pid, true, nil
}
func (r *ActorRouter) acquire(ctx context.Context, i *localRouteInstance) error {
	defer func() {
		r.mu.Lock()
		i.acquiring, i.writing = false, false
		r.finishLocked(i)
		r.mu.Unlock()
	}()
	return func() error {
		for {
			if err := ctx.Err(); err != nil {
				return notApplied(err)
			}
			snapshot, found, err := r.persistence.ReadRoute(ctx, i.key)
			if err != nil {
				return notApplied(err)
			}
			var expected *RouteOwner
			if found {
				valid, err := r.valid(snapshot)
				if err != nil {
					return notApplied(err)
				}
				if valid {
					released, err := r.releaseCompletedLocal(ctx, snapshot)
					if err != nil {
						return notApplied(err)
					}
					if released {
						continue
					}
					return ErrActorRouteTaken
				}
				owner := snapshot.Owner
				expected = &owner
			}
			r.mu.Lock()
			active := r.state == routerActive && time.Now().Before(r.deadline) && ctx.Err() == nil
			i.writing = active
			id := r.session
			r.mu.Unlock()
			if !active {
				if err := ctx.Err(); err != nil {
					return notApplied(err)
				}
				return notApplied(ErrSessionLost)
			}
			start := time.Now()
			result, err := safeRouteCall(func() (AcquireRouteResult, error) {
				return r.persistence.AcquireRoute(ctx, id, i.key, i.pid, expected, r.options.RouteTTL)
			})
			if err != nil {
				if !errors.Is(err, ErrRouteNotApplied) || errors.Is(err, ErrSessionLost) {
					r.lose()
				}
				return err
			}
			r.mu.Lock()
			i.writing = false
			if result.Status != RouteAcquired {
				r.mu.Unlock()
				continue
			}
			i.acquired = true
			i.deadline = start.Add(result.ValidFor - r.options.LeaseSafetyMargin)
			if r.state == routerActive || r.state == routerDraining {
				r.manager.scheduleLocked(i)
			}
			active = r.state == routerActive && time.Now().Before(r.deadline) && time.Now().Before(i.deadline)
			r.mu.Unlock()
			if !active {
				if err := r.instanceError(i); err != nil {
					return err
				}
				return ErrActorRouterClosed
			}
			return nil
		}
	}()
}

// A supervisor can receive an exit before Ergo invokes business Terminate.
// Finish the owner's exact cleanup before allowing its replacement to acquire.
func (r *ActorRouter) releaseCompletedLocal(ctx context.Context, snapshot RouteSnapshot) (bool, error) {
	r.mu.Lock()
	id, node := r.session, r.node
	previous := r.instances[snapshot.Owner.PID]
	r.mu.Unlock()
	if snapshot.Owner.SessionID != id {
		return false, nil
	}
	if previous == nil {
		// Release may have completed between ReadRoute and the local lookup.
		current, found, err := r.persistence.ReadRoute(ctx, snapshot.Key)
		return !found || current.Owner != snapshot.Owner, err
	}
	if previous.key != snapshot.Key {
		return false, nil
	}
	state, err := node.ProcessState(snapshot.Owner.PID)
	if err == nil && state != gen.ProcessStateTerminated && state != gen.ProcessStateZombee {
		return false, nil
	}
	if err != nil && !errors.Is(err, gen.ErrProcessUnknown) {
		return false, err
	}
	select {
	case <-previous.done:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	err = safeRouteError(func() error { return r.persistence.ReleaseRoute(ctx, id, snapshot.Key, snapshot.Owner.PID) })
	return err == nil, err
}

func (r *ActorRouter) finishLocked(i *localRouteInstance) {
	if !i.cleanup {
		return
	}
	if r.state == routerLost || r.state == routerClosed {
		r.manager.removeLocked(i)
		delete(r.instances, i.pid)
		return
	}
	if i.acquiring {
		return
	}
	r.manager.removeLocked(i)
	if !i.acquired || r.state == routerLost || r.state == routerClosed {
		delete(r.instances, i.pid)
		return
	}
	if i.release == nil && !i.releasing {
		i.release = r.pending.PushBack(i)
		r.releaseCount++
	}
}
func (r *ActorRouter) releaseExitedRoute(ctx context.Context, key gen.Atom, pid gen.PID) error {
	r.mu.Lock()
	id := r.session
	r.mu.Unlock()
	op, cancel := r.operationContext(ctx)
	defer cancel()
	_, err := routeWork(r, op, func() (struct{}, error) {
		return struct{}{}, r.persistence.ReleaseRoute(op, id, key, pid)
	})
	return err
}
func (r *ActorRouter) instanceError(i *localRouteInstance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if r.state == routerLost || r.state == routerClosed || !now.Before(r.deadline) {
		return ErrSessionLost
	}
	if i.stopped || !now.Before(i.deadline) {
		return ErrRouteExpired
	}
	return nil
}

// ActorRouterStats reports current work and cumulative storage failures.
type ActorRouterStats struct {
	Tracked, RouteQueued, ReleaseQueued         int
	RenewFailures, LeaseLosses, ReleaseFailures uint64
}

func (r *ActorRouter) Stats() ActorRouterStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := ActorRouterStats{Tracked: len(r.instances), ReleaseQueued: r.releaseCount, RenewFailures: r.renewFailures, LeaseLosses: r.leaseLosses, ReleaseFailures: r.releaseFailures}
	if r.manager != nil {
		s.RouteQueued = len(r.manager.jobs)
	}
	return s
}

// Preserve the concrete behavior so Ergo can discover its optional callbacks.
type behaviorPreservingProcess struct {
	gen.Process
	behavior gen.ProcessBehavior
	router   *ActorRouter
	instance *localRouteInstance
}

func (p behaviorPreservingProcess) Behavior() gen.ProcessBehavior { return p.behavior }
func (p behaviorPreservingProcess) State() gen.ProcessState {
	if p.router.instanceError(p.instance) != nil {
		return gen.ProcessStateZombee
	}
	return p.Process.State()
}

type routeLifecycle struct {
	router      *ActorRouter
	key         gen.Atom
	behavior    gen.ProcessBehavior
	instance    *localRouteInstance
	initialized bool
	initErr     error
}

func newRouteLifecycle(router *ActorRouter, key gen.Atom, b gen.ProcessBehavior) routeLifecycle {
	r := routeLifecycle{router: router, key: key, behavior: b}
	if key == "" {
		r.initErr = ErrActorRouteKeyEmpty
	} else if isNilBehavior(b) {
		r.initErr = ErrActorRouteBehaviorNil
	}
	return r
}
func isNilBehavior(b gen.ProcessBehavior) bool {
	if b == nil {
		return true
	}
	v := reflect.ValueOf(b)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}
func (l *routeLifecycle) init(p gen.Process, args ...any) (err error) {
	defer func() { l.initErr = err }()
	if l.initErr != nil {
		return l.initErr
	}
	if l.router == nil {
		return ErrActorRoutePersistenceNil
	}
	r := l.router
	if err = r.Bind(p.Node()); err != nil {
		return err
	}
	r.mu.Lock()
	if r.state != routerActive {
		r.mu.Unlock()
		return ErrActorRouterClosed
	}
	if r.releaseCount >= r.options.ReleaseQueueSize {
		r.mu.Unlock()
		return ErrActorRouterBusy
	}
	i := &localRouteInstance{key: l.key, pid: p.PID(), acquiring: true, done: make(chan struct{})}
	l.instance = i
	r.instances[i.pid] = i
	r.mu.Unlock()
	ctx, cancel := r.operationContext(context.Background())
	defer cancel()
	_, err = routeWork(r, ctx, func() (struct{}, error) { return struct{}{}, r.acquire(ctx, i) })
	if err != nil {
		r.mu.Lock()
		if errors.Is(err, ErrActorRouterBusy) || errors.Is(err, ErrRouteNotApplied) {
			i.acquiring = false
		}
		if i.writing {
			r.mu.Unlock()
			r.lose()
		} else {
			r.mu.Unlock()
		}
		return err
	}
	if err := r.instanceError(i); err != nil {
		return err
	}
	l.initialized = true
	err = l.behavior.ProcessInit(behaviorPreservingProcess{Process: p, behavior: l.behavior, router: r, instance: i}, args...)
	if err == nil {
		if err = r.instanceError(i); err != nil {
			return err
		}
		if p.State() == gen.ProcessStateZombee || p.State() == gen.ProcessStateTerminated {
			return gen.TerminateReasonKill
		}
	}
	return err
}
func (l *routeLifecycle) terminate(reason error) {
	defer func() {
		if l.instance != nil {
			r := l.router
			r.mu.Lock()
			if !l.instance.cleanup {
				l.instance.cleanup = true
				close(l.instance.done)
			}
			r.finishLocked(l.instance)
			r.mu.Unlock()
		}
	}()
	if l.initialized {
		l.behavior.ProcessTerminate(reason)
	}
}

// IActor is an Actor behavior instance with the Process API promoted by an
// embedded act.Actor.
type IActor interface {
	gen.Process
	act.ActorBehavior
}

// ISupervisor is a Supervisor behavior instance with the Process API promoted
// by an embedded act.Supervisor.
type ISupervisor interface {
	gen.Process
	act.SupervisorBehavior
}

// IPool is a Pool behavior instance with the Process API promoted by an
// embedded act.Pool.
type IPool interface {
	gen.Process
	act.PoolBehavior
}

// WithActorRoute adds route lease management to an Actor behavior instance.
// Call it inside a ProcessFactory so every spawn receives a fresh instance.
func (r *ActorRouter) WithActorRoute(key gen.Atom, actor IActor) IActor {
	return &routedActorBehavior{
		IActor: actor,
		route:  newRouteLifecycle(r, key, actor),
	}
}

// WithSupervisorRoute adds route lease management to a Supervisor behavior
// instance. Call it inside a ProcessFactory.
func (r *ActorRouter) WithSupervisorRoute(key gen.Atom, supervisor ISupervisor) ISupervisor {
	return &routedSupervisorBehavior{
		ISupervisor: supervisor,
		route:       newRouteLifecycle(r, key, supervisor),
	}
}

// WithPoolRoute adds route lease management to a Pool behavior instance.
// Call it inside a ProcessFactory.
func (r *ActorRouter) WithPoolRoute(key gen.Atom, pool IPool) IPool {
	return &routedPoolBehavior{
		IPool: pool,
		route: newRouteLifecycle(r, key, pool),
	}
}

func (r *ActorRouter) routeFactory(key gen.Atom, factory gen.ProcessFactory) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		if factory == nil {
			return routeErrorBehavior{err: ErrActorRouteFactoryNil}
		}
		behavior := factory()
		switch value := behavior.(type) {
		case IActor:
			return r.WithActorRoute(key, value)
		case ISupervisor:
			return r.WithSupervisorRoute(key, value)
		case IPool:
			return r.WithPoolRoute(key, value)
		default:
			return routeErrorBehavior{err: fmt.Errorf("%w: got %T", ErrActorRouteBehaviorMismatch, behavior)}
		}
	}
}

type routedActorBehavior struct {
	IActor
	route routeLifecycle
}

func (r *routedActorBehavior) ProcessInit(process gen.Process, args ...any) error {
	return r.route.init(process, args...)
}

func (r *routedActorBehavior) ProcessRun() error {
	if r.route.initErr != nil {
		return r.route.initErr
	}
	return r.IActor.ProcessRun()
}

func (r *routedActorBehavior) ProcessTerminate(reason error) {
	r.route.terminate(reason)
}

type routedSupervisorBehavior struct {
	ISupervisor
	route routeLifecycle
}

func (r *routedSupervisorBehavior) ProcessInit(process gen.Process, args ...any) error {
	return r.route.init(process, args...)
}

func (r *routedSupervisorBehavior) ProcessRun() error {
	if r.route.initErr != nil {
		return r.route.initErr
	}
	return r.ISupervisor.ProcessRun()
}

func (r *routedSupervisorBehavior) ProcessTerminate(reason error) {
	r.route.terminate(reason)
}

type routedPoolBehavior struct {
	IPool
	route routeLifecycle
}

func (r *routedPoolBehavior) ProcessInit(process gen.Process, args ...any) error {
	return r.route.init(process, args...)
}

func (r *routedPoolBehavior) ProcessRun() error {
	if r.route.initErr != nil {
		return r.route.initErr
	}
	return r.IPool.ProcessRun()
}

func (r *routedPoolBehavior) ProcessTerminate(reason error) {
	r.route.terminate(reason)
}

type routeErrorBehavior struct {
	err error
}

func (r routeErrorBehavior) ProcessInit(gen.Process, ...any) error { return r.err }
func (r routeErrorBehavior) ProcessRun() error                     { return r.err }
func (routeErrorBehavior) ProcessTerminate(error)                  {}

func renewalDelay(key gen.Atom, owner gen.PID, interval time.Duration, state *uint64) time.Duration {
	jitter := interval / 10
	if jitter <= 0 {
		return interval
	}
	if *state == 0 {
		*state = routeJitterSeed(key, owner)
	}
	value := *state
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	*state = value
	span := uint64(jitter)*2 + 1
	offset := time.Duration(value%span) - jitter
	return interval + offset
}

func routeJitterSeed(key gen.Atom, owner gen.PID) uint64 {
	const offset64 = uint64(1469598103934665603)
	const prime64 = uint64(1099511628211)
	value := offset64
	for _, char := range []byte(key) {
		value ^= uint64(char)
		value *= prime64
	}
	for _, char := range []byte(owner.Node) {
		value ^= uint64(char)
		value *= prime64
	}
	value ^= uint64(owner.ID)
	value *= prime64
	value ^= uint64(owner.Creation)
	if value == 0 {
		return offset64
	}
	return value
}
