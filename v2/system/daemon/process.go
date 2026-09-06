package daemon

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"github.com/qjpcpu/registrar/constants"
	"github.com/qjpcpu/registrar/events"
)

const ProcessName = gen.Atom("extensions_daemon")

var ErrNoAvailableNodes = errors.New("no available nodes")

type messageInit struct{}
type messageDaemonLaunchTimeout struct {
	Name  gen.Atom
	Epoch int64
}

const (
	defaultDaemonLaunchTimeout = 3 * time.Second
	defaultDaemonRunningGrace  = 2 * time.Second
	defaultDaemonRetryMaxDelay = 60 * time.Second
)

type Options struct {
	ScanBatchSize int
	// ScanBatchInterval spaces recovery batches to bound persistence load.
	ScanBatchInterval     time.Duration
	MaxInFlight           int
	InitialRecoveryDelay  time.Duration
	LeaderRecoveryDelay   time.Duration
	NodeLeftRecoveryDelay time.Duration
	FullRecoveryInterval  time.Duration
	LaunchTimeout         time.Duration
	// Deprecated: successful launches now release capacity immediately.
	RunningGrace      time.Duration
	RetryInitialDelay time.Duration
	RetryMaxDelay     time.Duration
	RecoveryJitterMax time.Duration
	RetryJitterMax    time.Duration
}

func DefaultOptions() Options {
	return Options{
		ScanBatchSize:         32,
		ScanBatchInterval:     50 * time.Millisecond,
		MaxInFlight:           64,
		InitialRecoveryDelay:  500 * time.Millisecond,
		LeaderRecoveryDelay:   500 * time.Millisecond,
		NodeLeftRecoveryDelay: 500 * time.Millisecond,
		FullRecoveryInterval:  15 * time.Minute,
		LaunchTimeout:         defaultDaemonLaunchTimeout,
		RunningGrace:          defaultDaemonRunningGrace,
		RetryInitialDelay:     500 * time.Millisecond,
		RetryMaxDelay:         defaultDaemonRetryMaxDelay,
		RecoveryJitterMax:     250 * time.Millisecond,
		RetryJitterMax:        500 * time.Millisecond,
	}
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.ScanBatchSize <= 0 {
		opts.ScanBatchSize = defaults.ScanBatchSize
	}
	if opts.ScanBatchInterval <= 0 {
		opts.ScanBatchInterval = defaults.ScanBatchInterval
	}
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = defaults.MaxInFlight
	}
	if opts.InitialRecoveryDelay <= 0 {
		opts.InitialRecoveryDelay = defaults.InitialRecoveryDelay
	}
	if opts.LeaderRecoveryDelay <= 0 {
		opts.LeaderRecoveryDelay = defaults.LeaderRecoveryDelay
	}
	if opts.NodeLeftRecoveryDelay <= 0 {
		opts.NodeLeftRecoveryDelay = defaults.NodeLeftRecoveryDelay
	}
	if opts.FullRecoveryInterval <= 0 {
		opts.FullRecoveryInterval = defaults.FullRecoveryInterval
	}
	if opts.LaunchTimeout <= 0 {
		opts.LaunchTimeout = defaults.LaunchTimeout
	}
	if opts.RunningGrace <= 0 {
		opts.RunningGrace = defaults.RunningGrace
	}
	if opts.RetryInitialDelay <= 0 {
		opts.RetryInitialDelay = defaults.RetryInitialDelay
	}
	if opts.RetryMaxDelay <= 0 {
		opts.RetryMaxDelay = defaults.RetryMaxDelay
	}
	if opts.RecoveryJitterMax < 0 {
		opts.RecoveryJitterMax = 0
	} else if opts.RecoveryJitterMax == 0 {
		opts.RecoveryJitterMax = defaults.RecoveryJitterMax
	}
	if opts.RetryJitterMax < 0 {
		opts.RetryJitterMax = 0
	} else if opts.RetryJitterMax == 0 {
		opts.RetryJitterMax = defaults.RetryJitterMax
	}
	return opts
}

var (
	daemonLaunchStarted   = gen.Atom("started")
	daemonLaunchTaken     = gen.Atom("already_taken")
	daemonLaunchFailed    = gen.Atom("failed")
	daemonLaunchNotNeeded = gen.Atom("not_needed")
)

type daemonLaunchPhase uint8

const (
	daemonLaunchPhaseLaunching daemonLaunchPhase = iota + 1
	daemonLaunchPhaseChecking
)

type daemonLaunchState struct {
	Launcher   gen.Atom
	Process    core.DaemonProcess
	TargetNode gen.Atom
	Epoch      int64
	Attempt    int
	Phase      daemonLaunchPhase
	StartedAt  time.Time
	Exited     gen.PID
	Cancel     gen.CancelFunc
}

type daemon struct {
	act.Actor
	book             core.IAddressBook
	decorate         RouteDecorator
	release          func(context.Context, gen.Atom, gen.PID) error
	ioPool           gen.PID
	pendingReplies   int
	lastScanDuration time.Duration
	wantRecovery     bool
	launchPool       gen.PID
	registrar        gen.Registrar
	isLeader         bool
	cancelLaunchAll  gen.CancelFunc
	cancelScanRetry  gen.CancelFunc
	recovered        map[gen.Atom]struct{}
	launching        map[gen.Atom]daemonLaunchState
	nextEpoch        int64
	options          Options
	scan             *recoveryScan
	fetching         bool
	pendingLaunch    map[gen.Atom]struct{}
	retries          map[gen.Atom]gen.CancelFunc
}

// RouteDecorator adds route lifecycle management to a daemon factory.
type RouteDecorator func(key gen.Atom, factory gen.ProcessFactory) gen.ProcessFactory

func Factory(book core.IAddressBook, decorate RouteDecorator) gen.ProcessFactory {
	return FactoryWithOptions(book, decorate, Options{})
}

func FactoryWithOptions(book core.IAddressBook, decorate RouteDecorator, opts Options) gen.ProcessFactory {
	opts = normalizeOptions(opts)
	return func() gen.ProcessBehavior {
		return &daemon{
			book:          book,
			decorate:      decorate,
			recovered:     make(map[gen.Atom]struct{}),
			launching:     make(map[gen.Atom]daemonLaunchState),
			nextEpoch:     time.Now().UnixNano(),
			options:       opts,
			pendingLaunch: make(map[gen.Atom]struct{}),
			retries:       make(map[gen.Atom]gen.CancelFunc),
		}
	}
}

// FactoryWithRouteCleanup also allows recovery to release an exact exited PID.
func FactoryWithRouteCleanup(book core.IAddressBook, decorate RouteDecorator, opts Options, release func(context.Context, gen.Atom, gen.PID) error) gen.ProcessFactory {
	factory := FactoryWithOptions(book, decorate, opts)
	return func() gen.ProcessBehavior { w := factory().(*daemon); w.release = release; return w }
}

func (w *daemon) Init(args ...any) error {
	w.SendAfter(w.PID(), messageInit{}, time.Second*1)
	return nil
}

func (w *daemon) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageInit:
		if err := w.setupRegistrarMonitoring(); err != nil {
			w.SendAfter(w.PID(), messageInit{}, time.Second*1)
		} else {
			w.launchAllAfter(w.options.InitialRecoveryDelay)
		}
	case core.MessageLaunchAllDaemon:
		if err := w.leaderShouldRecoverDaemon(); err != nil {
			w.launchAllAfter(time.Second * 60)
		} else if w.scan == nil {
			w.launchAllAfter(w.options.FullRecoveryInterval)
		}
	case core.MessageTopologyUpdated:
		if w.isLeader {
			w.recovered = make(map[gen.Atom]struct{})
			w.requestRecovery()
		}
	case messageScanStep:
		e.scan.scheduled = false
		w.scanStep(e.scan)
	case messageScanPage:
		w.fetching = false
		if w.scan == e.scan {
			if e.err != nil {
				w.Log().Warning("daemon scanner failed: %v", e.err)
				e.scan.failed = true
				e.scan.launchers = e.scan.launchers[1:]
				e.scan.iterator = nil
				if len(e.scan.launchers) > 0 {
					e.scan.iterator = e.scan.iterators[e.scan.launchers[0].Name]
				}
				e.scan.loaded = false
			} else {
				e.scan.iterator, e.scan.page = e.iterator, e.page
				e.scan.more, e.scan.loaded = e.more, true
			}
		}
		if w.scan != nil {
			w.scheduleScan(0)
		}
	case messageLaunchFinished:
		delete(w.pendingLaunch, e.result.Name)
		w.sendLaunchResult(e.owner, e.result)
	case messageIOResult:
		w.handleIOResult(e)
	case messageReplyFinished:
		if w.pendingReplies > 0 {
			w.pendingReplies--
		}
		w.requestPendingRecovery()
	case messageScanRetry:
		w.cancelScanRetry = nil
		w.requestRecovery()
	case messageRetry:
		if state, ok := w.launching[e.Name]; ok && state.Epoch == e.Epoch {
			delete(w.retries, e.Name)
			state.Cancel = nil
			w.launching[e.Name] = state
			w.startCheck(e.Name)
		}
	case core.MessageDaemonExited:
		if err := w.handleDaemonExit(e); err != nil {
			w.requestRecovery()
		}
	case core.MessageEnsureDaemon:
		if err := w.handleEnsureDaemon(e); err != nil {
			w.requestRecovery()
		}
	case core.MessageLaunchOneDaemon:
		return w.handleLaunchOneDaemon(e)
	case core.MessageDaemonLaunchResult:
		return w.handleDaemonLaunchResult(e)
	case messageDaemonLaunchTimeout:
		return w.handleDaemonLaunchTimeout(e)
	case gen.MessageDownPID:
		if e.PID == w.ioPool {
			w.ioPool = gen.PID{}
			w.pendingReplies = 0
			for name, state := range w.launching {
				if state.Phase == daemonLaunchPhaseChecking {
					w.retryTask(name)
				}
			}
		}
		if e.PID == w.launchPool {
			w.launchPool = gen.PID{}
			w.pendingLaunch = make(map[gen.Atom]struct{})
		}
	}
	return nil
}

func (w *daemon) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
	case events.EventNodeSwitchedToLeader:
		if e.Name == w.Node().Name() {
			w.isLeader = true
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(w.options.LeaderRecoveryDelay)
			return nil
		}
	case events.EventNodeSwitchedToFollower:
		if e.Name == w.Node().Name() {
			w.isLeader = false
			w.scan = nil
			w.recovered = make(map[gen.Atom]struct{})
			return nil
		}
	}
	return nil
}

func (w *daemon) launchAllAfter(duration time.Duration) {
	if cancel := w.cancelLaunchAll; cancel != nil {
		cancel()
		w.cancelLaunchAll = nil
	}
	// Add jitter to avoid synchronized leader recovery in case of registrar issues
	// or multiple nodes triggering at once.
	if w.options.RecoveryJitterMax > 0 {
		duration += time.Duration(rand.Int63n(int64(w.options.RecoveryJitterMax)))
	}
	if duration <= 0 {
		w.Send(w.PID(), core.MessageLaunchAllDaemon{})
	} else {
		if c, err := w.SendAfter(w.PID(), core.MessageLaunchAllDaemon{}, duration); err == nil {
			w.cancelLaunchAll = c
		}
	}
}

func (w *daemon) setupRegistrarMonitoring() error {
	if w.registrar != nil {
		return nil
	}
	registrar, err := w.Node().Network().Registrar()
	if err != nil {
		return err
	}
	event, err := registrar.Event()
	if err != nil {
		return err
	}
	if _, err := w.MonitorEvent(event); err != nil {
		return err
	}
	leader, err := registrar.ConfigItem(constants.LeaderNodeConfigItem)
	if err != nil {
		_ = w.DemonitorEvent(event)
		return err
	}
	if node, ok := leader.(gen.Atom); ok {
		w.isLeader = node == w.Node().Name()
	}
	w.registrar = registrar
	return nil
}

func (w *daemon) leaderShouldRecoverDaemon() error {
	if !w.isLeader {
		return nil
	}
	if w.scan != nil {
		w.scan.again = true
		return nil
	}
	var launchers []core.Launcher
	core.RangeLaunchers(func(_ gen.Atom, l core.Launcher) bool {
		if l.RecoveryScanner != nil {
			launchers = append(launchers, l)
		}
		return true
	})
	w.scan = &recoveryScan{launchers: launchers, iterators: make(map[gen.Atom]core.DaemonIterator), started: time.Now()}
	w.scheduleScan(0)
	return nil
}

func (w *daemon) ensureDaemon(launcher gen.Atom, proc core.DaemonProcess, attempt int) error {
	return w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: launcher, Process: proc, Attempt: attempt})
}

func (w *daemon) handleEnsureDaemon(msg core.MessageEnsureDaemon) error {
	return w.admit(msg, gen.PID{})
}

func (w *daemon) admit(msg core.MessageEnsureDaemon, exited gen.PID) error {
	key := msg.Process.ProcessName
	if state, ok := w.launching[key]; ok {
		// A confirmed exit must survive an older in-flight lookup or launch result.
		if exited != (gen.PID{}) {
			state.Exited = exited
			w.launching[key] = state
		}
		return nil
	}
	if len(w.launching)+w.pendingReplies >= w.options.MaxInFlight {
		return errLaunchBusy
	}
	if w.book.PickNode(key) == "" {
		return ErrNoAvailableNodes
	}
	w.launching[key] = daemonLaunchState{Launcher: msg.Launcher, Process: msg.Process, Attempt: msg.Attempt, Exited: exited}
	w.startCheck(key)
	return nil
}

func (w *daemon) startCheck(key gen.Atom) {
	state := w.launching[key]
	state.Epoch = w.nextLaunchEpoch()
	state.TargetNode = w.book.PickNode(key)
	state.Phase = daemonLaunchPhaseChecking
	state.StartedAt = time.Now()
	w.launching[key] = state
	if state.TargetNode == "" {
		w.retryTask(key)
		return
	}
	if err := w.dispatchIO(messageIO{key: key, state: state}); err != nil {
		w.retryTask(key)
	}
}

func (w *daemon) handleIOResult(msg messageIOResult) {
	state, ok := w.launching[msg.key]
	if !ok || state.Epoch != msg.epoch {
		return
	}
	if msg.err != nil {
		w.retryTask(msg.key)
		return
	}
	if state.Exited != msg.exited {
		w.startCheck(msg.key)
		return
	}
	state.Exited = gen.PID{}
	w.launching[msg.key] = state
	if msg.running {
		w.completeTask(msg.key)
		return
	}
	state.Phase = daemonLaunchPhaseLaunching
	state.Cancel, _ = w.SendAfter(w.PID(), messageDaemonLaunchTimeout{Name: msg.key, Epoch: state.Epoch}, w.options.LaunchTimeout)
	w.launching[msg.key] = state
}

func (w *daemon) retryTask(key gen.Atom) {
	state, ok := w.launching[key]
	if !ok {
		return
	}
	// Scanner-backed tasks can be retried without retaining their admission slot.
	// Keep exact-owner cleanup pending until it succeeds.
	if launcher, ok := core.GetLauncher(state.Launcher); ok && launcher.RecoveryScanner != nil && state.Exited == (gen.PID{}) {
		w.completeTask(key)
		if w.scan != nil {
			w.scan.failed = true
		} else if w.cancelScanRetry == nil {
			w.cancelScanRetry, _ = w.SendAfter(w.PID(), messageScanRetry{}, w.options.RetryMaxDelay)
		}
		return
	}
	if state.Cancel != nil {
		state.Cancel()
	}
	state.Attempt++
	state.Epoch = w.nextLaunchEpoch()
	state.Cancel, _ = w.SendAfter(w.PID(), messageRetry{Name: key, Epoch: state.Epoch}, w.retryDelay(state.Attempt))
	w.launching[key] = state
	w.retries[key] = state.Cancel
}

func (w *daemon) completeTask(key gen.Atom) {
	if state, ok := w.launching[key]; ok && state.Cancel != nil {
		state.Cancel()
	}
	delete(w.launching, key)
	delete(w.retries, key)
	w.requestPendingRecovery()
	if w.scan != nil {
		w.scheduleScan(0)
	}
}

func (w *daemon) handleLaunchOneDaemon(msg core.MessageLaunchOneDaemon) error {
	launcher, ok := core.GetLauncher(msg.Launcher)
	if !ok {
		if msg.Owner != "" {
			w.sendLaunchResult(msg.Owner, core.MessageDaemonLaunchResult{
				Name:  msg.Process.ProcessName,
				Node:  w.Node().Name(),
				Epoch: msg.Epoch,
				State: daemonLaunchFailed,
				Err:   fmt.Sprintf("can't find launcher by %s", msg.Launcher),
			})
		}
		return nil
	}

	err := w.dispatchLaunch(messageLaunch{launcher: launcher, request: msg})
	if err != nil {
		w.sendLaunchResult(msg.Owner, core.MessageDaemonLaunchResult{
			Name:  msg.Process.ProcessName,
			Node:  w.Node().Name(),
			Epoch: msg.Epoch,
			State: daemonLaunchFailed,
			Err:   err.Error(),
		})
	}
	return nil
}

func (w *daemon) dispatchLaunch(msg messageLaunch) error {
	if _, ok := w.pendingLaunch[msg.request.Process.ProcessName]; ok {
		return nil
	}
	if len(w.pendingLaunch) >= daemonLaunchWorkers {
		return errLaunchBusy
	}
	if w.launchPool == (gen.PID{}) {
		pid, err := w.Spawn(func() gen.ProcessBehavior {
			return &daemonLaunchPool{decorate: w.decorate}
		}, gen.ProcessOptions{LinkParent: true})
		if err != nil {
			return err
		}
		if err := w.MonitorPID(pid); err != nil {
			w.Node().Kill(pid)
			return err
		}
		w.launchPool = pid
	}
	if err := w.Send(w.launchPool, msg); err != nil {
		return err
	}
	w.pendingLaunch[msg.request.Process.ProcessName] = struct{}{}
	return nil
}

func (w *daemon) handleDaemonLaunchResult(msg core.MessageDaemonLaunchResult) error {
	state, ok := w.launching[msg.Name]
	if !ok || state.Epoch != msg.Epoch || state.TargetNode != msg.Node {
		return nil
	}
	if state.Exited != (gen.PID{}) {
		w.retryTask(msg.Name)
		return nil
	}
	switch msg.State {
	case daemonLaunchStarted, daemonLaunchTaken, daemonLaunchNotNeeded:
		w.completeTask(msg.Name)
	default:
		// Older targets report a normal Init refusal as a failed launch.
		if msg.Err == gen.TerminateReasonNormal.Error() {
			w.completeTask(msg.Name)
			return nil
		}
		w.retryTask(msg.Name)
	}
	return nil
}

func (w *daemon) handleDaemonLaunchTimeout(msg messageDaemonLaunchTimeout) error {
	if state, ok := w.launching[msg.Name]; ok && state.Epoch == msg.Epoch {
		w.retryTask(msg.Name)
	}
	return nil
}

func (w *daemon) nextLaunchEpoch() int64 {
	w.nextEpoch++
	return w.nextEpoch
}

func (w *daemon) retryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := w.options.RetryInitialDelay
	for i := 0; i < attempt && delay < w.options.RetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > w.options.RetryMaxDelay {
		delay = w.options.RetryMaxDelay
	}
	if w.options.RetryJitterMax > 0 {
		delay += time.Duration(rand.Int63n(int64(w.options.RetryJitterMax)))
	}
	return delay
}

// Coalesce overflow notifications into a full recovery request instead of
// retaining an unbounded set of per-key retries outside the admission limit.
func (w *daemon) requestRecovery() {
	w.wantRecovery = true
	w.requestPendingRecovery()
}
func (w *daemon) requestPendingRecovery() {
	if !w.wantRecovery {
		return
	}
	if w.isLeader {
		w.wantRecovery = false
		w.launchAllAfter(w.options.NodeLeftRecoveryDelay)
		return
	}
	if len(w.launching)+w.pendingReplies >= w.options.MaxInFlight || w.registrar == nil {
		return
	}
	leader, err := w.registrar.ConfigItem(constants.LeaderNodeConfigItem)
	if err != nil {
		return
	}
	name, ok := leader.(gen.Atom)
	if !ok || name == "" {
		return
	}
	if err := w.dispatchIO(messageIO{owner: name, recoverAll: true}); err == nil {
		w.pendingReplies++
		w.wantRecovery = false
	}
}

func (w *daemon) sendLaunchResult(owner gen.Atom, result core.MessageDaemonLaunchResult) {
	if owner == "" {
		return
	}
	if owner == w.Node().Name() {
		w.handleDaemonLaunchResult(result)
		return
	}
	// If reply capacity is exhausted the sender's existing launch timeout retries.
	if len(w.launching)+w.pendingReplies >= w.options.MaxInFlight {
		return
	}
	if err := w.dispatchIO(messageIO{owner: owner, reply: &result}); err == nil {
		w.pendingReplies++
	}
}

func (w *daemon) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	return nil, nil
}

func (w *daemon) HandleInspect(from gen.PID, item ...string) map[string]string {
	stats := map[string]string{
		"is_leader":          strconv.FormatBool(w.isLeader),
		"recovered_count":    strconv.Itoa(len(w.recovered)),
		"launching_count":    strconv.Itoa(len(w.launching)),
		"retry_count":        strconv.Itoa(len(w.retries)),
		"pending_launches":   strconv.Itoa(len(w.pendingLaunch)),
		"pending_replies":    strconv.Itoa(w.pendingReplies),
		"last_scan_duration": w.lastScanDuration.String(),
	}
	if w.scan != nil {
		stats["scan_pending"] = strconv.Itoa(len(w.scan.page))
		stats["scan_duration"] = time.Since(w.scan.started).String()
	}
	var daemonNames []string
	core.RangeLaunchers(func(name gen.Atom, _ core.Launcher) bool {
		daemonNames = append(daemonNames, string(name))
		return true
	})
	if len(daemonNames) > 0 {
		stats["daemons"] = strings.Join(daemonNames, ",")
	}
	if r := w.registrar; r != nil {
		if n, err := r.ConfigItem(constants.LeaderNodeConfigItem); err == nil {
			if node, ok := n.(gen.Atom); ok {
				stats["leader"] = string(node)
			}
		}
	}
	if info, err := w.Node().Info(); err == nil {
		stats["uptime"] = strconv.Itoa(int(info.Uptime))
		stats["process_running"] = strconv.Itoa(int(info.ProcessesRunning))
		stats["process_total"] = strconv.Itoa(int(info.ProcessesTotal))
		stats["process_zombee"] = strconv.Itoa(int(info.ProcessesZombee))
		stats["memory_alloc"] = strconv.Itoa(int(info.MemoryAlloc))
		stats["memory_used"] = strconv.Itoa(int(info.MemoryUsed))
	}
	stats["gorountine"] = strconv.Itoa(runtime.NumGoroutine())
	return stats
}

const daemonLaunchWorkers = 8

type messageLaunch struct {
	launcher core.Launcher
	request  core.MessageLaunchOneDaemon
}

type daemonLaunchPool struct {
	act.Pool
	decorate RouteDecorator
	stopped  chan struct{}
}

func (p *daemonLaunchPool) Init(...any) (act.PoolOptions, error) {
	p.stopped = make(chan struct{})
	return act.PoolOptions{
		PoolSize: daemonLaunchWorkers,
		WorkerFactory: func() gen.ProcessBehavior {
			return &daemonLaunchWorker{decorate: p.decorate, stopped: p.stopped}
		},
	}, nil
}

func (p *daemonLaunchPool) Terminate(error) {
	if p.stopped != nil {
		close(p.stopped)
	}
}

type daemonLaunchWorker struct {
	act.Actor
	stopped  <-chan struct{}
	decorate RouteDecorator
}

func (w *daemonLaunchWorker) Init(...any) error {
	// A daemon's linked exit must not terminate its long-lived launch worker.
	w.SetTrapExit(true)
	return nil
}

func (w *daemonLaunchWorker) HandleMessage(from gen.PID, message any) error {
	msg, ok := message.(messageLaunch)
	if !ok {
		return nil
	}
	result := core.MessageDaemonLaunchResult{
		Name:  msg.request.Process.ProcessName,
		Node:  w.Node().Name(),
		Epoch: msg.request.Epoch,
		State: daemonLaunchStarted,
	}
	defer func() {
		if v := recover(); v != nil {
			result.State = daemonLaunchFailed
			result.Err = fmt.Sprintf("launch panic: %v", v)
		}
		if err := w.Send(ProcessName, messageLaunchFinished{owner: msg.request.Owner, result: result}); err != nil {
			w.Log().Warning("report daemon launch completion: %v", err)
		}
	}()

	err := w.launch(msg)
	if err != nil {
		if errors.Is(err, gen.TerminateReasonNormal) {
			result.State = daemonLaunchNotNeeded
		} else if err == gen.ErrTaken {
			result.State = daemonLaunchTaken
		} else {
			result.State = daemonLaunchFailed
			result.Err = err.Error()
		}
	}
	return nil
}

func (w *daemonLaunchWorker) launch(msg messageLaunch) error {
	name := msg.request.Process.ProcessName
	factory := msg.launcher.Factory
	if w.decorate != nil {
		factory = w.decorate(name, factory)
	}
	factory = core.WithDaemonRecovery(factory, msg.launcher.Name, msg.request.Process)
	init := &daemonLaunchInit{done: make(chan struct{})}
	_, err := w.SpawnRegister(name, func() gen.ProcessBehavior {
		behavior := factory()
		if behavior == nil {
			return nil
		}
		init.ProcessBehavior = behavior
		return init
	}, msg.launcher.Option, msg.request.Process.Args...)
	// Spawn timeout does not stop Ergo's Init goroutine. Keep this worker
	// occupied until the callback returns, or the launch pool stops.
	if err == gen.ErrTimeout && !init.state.CompareAndSwap(0, 2) {
		select {
		case <-init.done:
		case <-w.stopped:
		}
	}
	return err
}

// daemonLaunchInit tracks the callback lifetime independently of Spawn's timeout.
type daemonLaunchInit struct {
	gen.ProcessBehavior
	state atomic.Int32
	done  chan struct{}
}

func (b *daemonLaunchInit) ProcessInit(process gen.Process, args ...any) error {
	if !b.state.CompareAndSwap(0, 1) {
		return gen.ErrTimeout
	}
	defer close(b.done)
	return b.ProcessBehavior.ProcessInit(daemonLaunchProcess{Process: process, behavior: b.ProcessBehavior}, args...)
}

type daemonLaunchProcess struct {
	gen.Process
	behavior gen.ProcessBehavior
}

func (p daemonLaunchProcess) Behavior() gen.ProcessBehavior { return p.behavior }

var errLaunchBusy = errors.New("daemon launch capacity reached")

type messageLaunchFinished struct {
	owner  gen.Atom
	result core.MessageDaemonLaunchResult
}
type recoveryScan struct {
	launchers                   []core.Launcher
	iterator                    core.DaemonIterator
	page                        []core.DaemonProcess
	loaded, more, again, failed bool
	iterators                   map[gen.Atom]core.DaemonIterator
	started                     time.Time
	scheduled                   bool
	nextBatchAt                 time.Time
}
type messageScanRetry struct{}

type messageScanStep struct{ scan *recoveryScan }
type messageScanPage struct {
	scan     *recoveryScan
	iterator core.DaemonIterator
	page     []core.DaemonProcess
	more     bool
	err      error
}

func (w *daemon) scheduleScan(delay time.Duration) {
	if w.scan == nil || w.scan.scheduled {
		return
	}
	w.scan.scheduled = true
	var err error
	if delay == 0 {
		err = w.Send(w.PID(), messageScanStep{w.scan})
	} else {
		_, err = w.SendAfter(w.PID(), messageScanStep{w.scan}, delay)
	}
	if err != nil {
		w.scan.scheduled = false
	}
}

func (w *daemon) scanStep(scan *recoveryScan) {
	if scan != w.scan || !w.isLeader || w.fetching {
		return
	}
	if len(scan.launchers) == 0 {
		w.finishScan(scan.failed)
		return
	}
	if delay := time.Until(scan.nextBatchAt); delay > 0 {
		w.scheduleScan(delay)
		return
	}
	if !scan.loaded {
		w.fetching = true
		node, pid := w.Node(), w.PID()
		iterator, factory := scan.iterator, scan.launchers[0].RecoveryScanner
		go func() {
			result := messageScanPage{scan: scan, iterator: iterator}
			defer func() {
				if v := recover(); v != nil {
					result.err = fmt.Errorf("scanner panic: %v", v)
				}
				_ = node.Send(pid, result)
			}()
			if result.iterator == nil {
				result.iterator = factory()
			}
			result.page, result.more, result.err = result.iterator()
		}()
		return
	}
	scan.nextBatchAt = time.Now().Add(w.options.ScanBatchInterval)
	processed := 0
	for len(scan.page) > 0 && processed < w.options.ScanBatchSize {
		if len(w.launching)+w.pendingReplies >= w.options.MaxInFlight {
			w.scheduleScan(50 * time.Millisecond)
			return
		}
		proc := scan.page[0]
		scan.page = scan.page[1:]
		processed++
		if _, ok := w.recovered[proc.ProcessName]; ok {
			continue
		}
		if err := w.ensureDaemon(scan.launchers[0].Name, proc, 0); err != nil {
			scan.page = append([]core.DaemonProcess{proc}, scan.page...)
			w.scheduleScan(50 * time.Millisecond)
			return
		} else {
			w.recovered[proc.ProcessName] = struct{}{}
		}
	}
	if len(scan.page) == 0 {
		scan.loaded = false
		if scan.iterators == nil {
			scan.iterators = make(map[gen.Atom]core.DaemonIterator)
		}
		current := scan.launchers[0]
		scan.launchers = scan.launchers[1:]
		if scan.more {
			scan.iterators[current.Name] = scan.iterator
			scan.launchers = append(scan.launchers, current)
		} else {
			delete(scan.iterators, current.Name)
		}
		scan.iterator = nil
		if len(scan.launchers) > 0 {
			scan.iterator = scan.iterators[scan.launchers[0].Name]
		}
	}
	w.scheduleScan(0)
}
func (w *daemon) finishScan(failed bool) {
	again := w.scan != nil && w.scan.again
	w.lastScanDuration = time.Since(w.scan.started)
	w.scan = nil
	w.recovered = make(map[gen.Atom]struct{})
	if again {
		w.launchAllAfter(w.options.NodeLeftRecoveryDelay)
	} else if failed {
		w.launchAllAfter(w.options.RetryMaxDelay)
	} else {
		w.launchAllAfter(w.options.FullRecoveryInterval)
	}
}
func (w *daemon) handleDaemonExit(msg core.MessageDaemonExited) error {
	return w.admit(msg.Ensure, msg.PID)
}
func (w *daemon) Terminate(error) {
	if w.cancelScanRetry != nil {
		w.cancelScanRetry()
	}
	if w.cancelLaunchAll != nil {
		w.cancelLaunchAll()
	}
	for _, state := range w.launching {
		if state.Cancel != nil {
			state.Cancel()
		}
	}
}
