package system

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
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

const routeLogInterval = 30 * time.Second

// ActorRoutePersistence stores leased route-key-to-PID mappings.
//
// Implementations must be safe for concurrent use. Acquire, Renew, and Release
// must compare both the key and PID atomically so an old actor incarnation can
// never modify a newer owner's route.
type ActorRoutePersistence interface {
	Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error)
	// Replace atomically changes an existing exact owner; false means the owner changed or disappeared.
	Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error)
	Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key gen.Atom, pid gen.PID) error
	Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error)
}

// ActorRouterOptions controls route lease timing.
type ActorRouterOptions struct {
	// LeaseTTL is the lifetime of a successfully acquired or renewed route.
	LeaseTTL time.Duration
	// RenewInterval is the target interval between route renewals.
	RenewInterval time.Duration
	// OperationTimeout bounds each persistence operation.
	OperationTimeout time.Duration
	// RenewWorkers bounds concurrent renewal and release operations.
	RenewWorkers int
	// RenewQueueSize bounds queued renewal operations.
	RenewQueueSize int
	// ReleaseQueueSize bounds the higher-priority release queue.
	ReleaseQueueSize int
}

// DefaultActorRouterOptions returns balanced defaults for route leases.
func DefaultActorRouterOptions() ActorRouterOptions {
	workers := runtime.GOMAXPROCS(0)
	if workers < 4 {
		workers = 4
	}
	return ActorRouterOptions{
		LeaseTTL:         30 * time.Second,
		RenewInterval:    10 * time.Second,
		OperationTimeout: 3 * time.Second,
		RenewWorkers:     workers,
		RenewQueueSize:   65536,
		ReleaseQueueSize: 65536,
	}
}

func normalizeActorRouterOptions(options ActorRouterOptions) (ActorRouterOptions, error) {
	defaults := DefaultActorRouterOptions()
	if options.LeaseTTL == 0 {
		options.LeaseTTL = defaults.LeaseTTL
	}
	if options.RenewInterval == 0 {
		options.RenewInterval = defaults.RenewInterval
	}
	if options.OperationTimeout == 0 {
		options.OperationTimeout = defaults.OperationTimeout
	}
	if options.RenewWorkers == 0 {
		options.RenewWorkers = defaults.RenewWorkers
	}
	if options.RenewQueueSize == 0 {
		options.RenewQueueSize = defaults.RenewQueueSize
	}
	if options.ReleaseQueueSize == 0 {
		options.ReleaseQueueSize = defaults.ReleaseQueueSize
	}
	if options.LeaseTTL < 0 || options.RenewInterval < 0 || options.OperationTimeout < 0 || options.RenewWorkers < 0 || options.RenewQueueSize < 0 || options.ReleaseQueueSize < 0 {
		return ActorRouterOptions{}, errors.New("actor router options must be non-negative")
	}
	if options.RenewInterval >= options.LeaseTTL {
		return ActorRouterOptions{}, errors.New("actor route renew interval must be shorter than lease TTL")
	}
	// Leave room for the latest jittered renewal and timing-wheel rounding.
	margin := options.LeaseTTL - options.RenewInterval
	if margin <= options.RenewInterval/10+routeSchedulerResolution(options.RenewInterval) {
		return ActorRouterOptions{}, errors.New("actor route lease TTL must cover renewal jitter and scheduler resolution")
	}
	return options, nil
}

// ActorRouter decorates behavior instances with route leases and resolves keys
// directly through an ActorRoutePersistence implementation.
type ActorRouter struct {
	persistence ActorRoutePersistence
	options     ActorRouterOptions

	mu   sync.RWMutex
	node gen.Node

	lastRenewLog    atomic.Int64
	renewFailures   atomic.Uint64
	leaseLosses     atomic.Uint64
	releaseFailures atomic.Uint64
	releaseDropped  atomic.Uint64
	maxRenewDelay   atomic.Int64
	instances       sync.Map // PID -> *localRouteInstance

	managerMu sync.Mutex
	manager   *routeLeaseManager
	closed    bool
	closeDone chan struct{}
}

type localRouteInstance struct {
	mu      sync.Mutex
	key     gen.Atom
	stopped bool
	done    chan struct{}
}

// Close stops route renewal workers. StartSimpleNode closes its router after
// the node stops; custom node bootstrap must do the same.
func (r *ActorRouter) Close() {
	r.managerMu.Lock()
	if r.closed {
		done := r.closeDone
		r.managerMu.Unlock()
		if done != nil {
			<-done
		}
		return
	}
	r.closed = true
	r.closeDone = make(chan struct{})
	manager := r.manager
	r.managerMu.Unlock()
	// Ergo's node wait can finish before business termination callbacks return.
	ctx, cancel := r.operationContext(context.Background())
	node, _ := r.boundNode()
	if node != nil && !node.IsAlive() {
		r.instances.Range(func(_, value any) bool {
			select {
			case <-value.(*localRouteInstance).done:
				return true
			case <-ctx.Done():
				return false
			}
		})
	}
	cancel()
	if manager != nil {
		manager.close()
	}
	r.managerMu.Lock()
	r.manager = nil
	close(r.closeDone)
	r.managerMu.Unlock()
}

func (r *ActorRouter) trackRoute(key gen.Atom, pid gen.PID) error {
	r.managerMu.Lock()
	defer r.managerMu.Unlock()
	if r.closed {
		return ErrActorRouterClosed
	}
	if r.manager == nil {
		r.manager = newRouteLeaseManager(r)
	}
	r.manager.track(key, pid)
	r.instances.LoadOrStore(pid, &localRouteInstance{key: key, done: make(chan struct{})})
	return nil
}

func (r *ActorRouter) untrackRoute(key gen.Atom, pid gen.PID) {
	if value, ok := r.instances.Load(pid); ok {
		instance := value.(*localRouteInstance)
		instance.mu.Lock()
		defer instance.mu.Unlock()
		instance.stopped = true
		defer func() {
			if instance.done != nil {
				close(instance.done)
			}
			r.instances.Delete(pid)
		}()
	}

	r.managerMu.Lock()
	manager := r.manager
	r.managerMu.Unlock()
	if manager != nil {
		manager.untrack(key, pid)
	}
}

// NewActorRouter creates an unbound actor router.
func NewActorRouter(persistence ActorRoutePersistence, options ActorRouterOptions) (*ActorRouter, error) {
	if persistence == nil {
		return nil, ErrActorRoutePersistenceNil
	}
	normalized, err := normalizeActorRouterOptions(options)
	if err != nil {
		return nil, err
	}
	return &ActorRouter{persistence: persistence, options: normalized}, nil
}

// Bind attaches the router to one Ergo node. Rebinding the same node is idempotent.
func (r *ActorRouter) Bind(node gen.Node) error {
	if node == nil {
		return errors.New("actor router node is nil")
	}
	r.managerMu.Lock()
	defer r.managerMu.Unlock()
	if r.closed {
		return ErrActorRouterClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.node == nil {
		r.node = node
		return nil
	}
	if r.node.Name() != node.Name() {
		return fmt.Errorf("%w: have %s, got %s", ErrActorRouterBound, r.node.Name(), node.Name())
	}
	return nil
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

// acquire reclaims exited local owners and nodes absent from the registrar.
func (r *ActorRouter) acquire(ctx context.Context, key gen.Atom, pid gen.PID) (bool, error) {
	acquired, err := r.persistence.Acquire(ctx, key, pid, r.options.LeaseTTL)
	if err != nil || acquired {
		return acquired, err
	}
	owner, found, err := r.persistence.Lookup(ctx, key)
	if err != nil {
		return false, err
	}
	if !found {
		return r.persistence.Acquire(ctx, key, pid, r.options.LeaseTTL)
	}
	node, err := r.boundNode()
	if err != nil {
		return false, err
	}
	if owner.Node == pid.Node {
		_, err = node.ProcessState(owner)
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, gen.ErrProcessUnknown) {
			return false, err
		}
		// Ergo removes the PID before invoking business Terminate.
		if value, ok := r.instances.Load(owner); ok {
			instance := value.(*localRouteInstance)
			select {
			case <-instance.done:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
	} else {
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
			if name == owner.Node {
				return false, nil
			}
		}
	}
	replaced, err := r.persistence.Replace(ctx, key, owner, pid, r.options.LeaseTTL)
	if err != nil || replaced {
		return replaced, err
	}
	return r.persistence.Acquire(ctx, key, pid, r.options.LeaseTTL)
}

// restoreRoute is used by daemon recovery for an existing routed local instance.
func (r *ActorRouter) restoreRoute(ctx context.Context, key gen.Atom, pid gen.PID) (bool, error) {
	value, ok := r.instances.Load(pid)
	if !ok {
		return false, nil
	}
	instance := value.(*localRouteInstance)
	instance.mu.Lock()
	defer instance.mu.Unlock()
	if instance.stopped || instance.key != key {
		return false, nil
	}
	node, err := r.boundNode()
	if err != nil {
		return false, err
	}
	if _, err := node.ProcessState(pid); err != nil {
		return false, err
	}
	opctx, cancel := r.operationContext(ctx)
	defer cancel()
	acquired, err := r.acquire(opctx, key, pid)
	if err != nil {
		return false, err
	}
	if !acquired {
		return false, ErrActorRouteTaken
	}
	if err := r.trackRoute(key, pid); err != nil {
		_ = r.persistence.Release(opctx, key, pid)
		return false, err
	}
	return true, nil
}

// releaseExitedRoute is used only after the daemon's termination hook completes.
func (r *ActorRouter) releaseExitedRoute(ctx context.Context, key gen.Atom, pid gen.PID) error {
	opctx, cancel := r.operationContext(ctx)
	defer cancel()
	return r.persistence.Release(opctx, key, pid)
}

func (r *ActorRouter) lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if key == "" {
		return gen.PID{}, false, ErrActorRouteKeyEmpty
	}
	if _, err := r.boundNode(); err != nil {
		return gen.PID{}, false, err
	}
	opctx, cancel := r.operationContext(ctx)
	defer cancel()
	pid, found, err := r.persistence.Lookup(opctx, key)
	if err != nil || !found {
		return gen.PID{}, false, err
	}
	return pid, found, nil
}

func (r *ActorRouter) boundNode() (gen.Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
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

func (r *ActorRouter) shouldLogRenewFailure(now time.Time) bool {
	next := now.Add(-routeLogInterval).UnixNano()
	for {
		last := r.lastRenewLog.Load()
		if last > next {
			return false
		}
		if r.lastRenewLog.CompareAndSwap(last, now.UnixNano()) {
			return true
		}
	}
}

func (r *ActorRouter) boundLogWarning(format string, args ...any) {
	node, err := r.boundNode()
	if err != nil {
		return
	}
	defer func() { _ = recover() }()
	node.Log().Warning(format, args...)
}

// behaviorPreservingProcess makes Ergo's built-in Actor, Supervisor, and Pool
// discover the original concrete behavior during ProcessInit. Returning the
// route wrapper here would hide their optional callback methods.
type behaviorPreservingProcess struct {
	gen.Process
	behavior gen.ProcessBehavior
}

func (p behaviorPreservingProcess) Behavior() gen.ProcessBehavior {
	return p.behavior
}

type routeLifecycle struct {
	router      *ActorRouter
	key         gen.Atom
	behavior    gen.ProcessBehavior
	pid         gen.PID
	acquired    bool
	initialized bool
	initErr     error
}

func newRouteLifecycle(router *ActorRouter, key gen.Atom, behavior gen.ProcessBehavior) routeLifecycle {
	lifecycle := routeLifecycle{router: router, key: key, behavior: behavior}
	switch {
	case key == "":
		lifecycle.initErr = ErrActorRouteKeyEmpty
	case isNilBehavior(behavior):
		lifecycle.initErr = ErrActorRouteBehaviorNil
	}
	return lifecycle
}

func isNilBehavior(behavior gen.ProcessBehavior) bool {
	if behavior == nil {
		return true
	}
	value := reflect.ValueOf(behavior)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (r *routeLifecycle) init(process gen.Process, args ...any) error {
	r.pid = process.PID()
	if r.initErr != nil {
		return r.initErr
	}
	if r.router == nil {
		return ErrActorRoutePersistenceNil
	}
	if err := r.router.Bind(process.Node()); err != nil {
		return err
	}
	ctx, cancel := r.router.operationContext(context.Background())
	acquired, err := r.router.acquire(ctx, r.key, r.pid)
	cancel()
	if err != nil {
		return fmt.Errorf("acquire actor route %s: %w", r.key, err)
	}
	if !acquired {
		return fmt.Errorf("%w: %s", ErrActorRouteTaken, r.key)
	}
	r.acquired = true
	if err := r.router.trackRoute(r.key, r.pid); err != nil {
		ctx, cancel := r.router.operationContext(context.Background())
		_ = r.router.persistence.Release(ctx, r.key, r.pid)
		cancel()
		r.acquired = false
		return err
	}
	r.initialized = true
	view := behaviorPreservingProcess{Process: process, behavior: r.behavior}
	return r.behavior.ProcessInit(view, args...)
}

func (r *routeLifecycle) terminate(reason error) {
	defer func() {
		if r.acquired {
			r.router.untrackRoute(r.key, r.pid)
			r.acquired = false
		}
	}()
	if r.initialized {
		r.behavior.ProcessTerminate(reason)
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

// ActorRouterStats describes lease work and cumulative failures on this node.
type ActorRouterStats struct {
	Tracked, RenewQueued, ReleaseQueued                         int
	RenewFailures, LeaseLosses, ReleaseFailures, ReleaseDropped uint64
	MaxRenewDelay                                               time.Duration
}

func (r *ActorRouter) Stats() ActorRouterStats {
	stats := ActorRouterStats{RenewFailures: r.renewFailures.Load(), LeaseLosses: r.leaseLosses.Load(), ReleaseFailures: r.releaseFailures.Load(), ReleaseDropped: r.releaseDropped.Load(), MaxRenewDelay: time.Duration(r.maxRenewDelay.Load())}
	r.managerMu.Lock()
	manager := r.manager
	r.managerMu.Unlock()
	if manager != nil {
		stats.Tracked = manager.trackedCount()
		stats.RenewQueued = len(manager.renewJobs)
		stats.ReleaseQueued = len(manager.releaseJobs)
	}
	return stats
}
