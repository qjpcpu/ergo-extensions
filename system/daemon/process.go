package daemon

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
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
	daemonLaunchTimeout = 30 * time.Second
	daemonRunningGrace  = 15 * time.Second
	daemonRetryMaxDelay = 60 * time.Second
)

var (
	daemonLaunchStarted = gen.Atom("started")
	daemonLaunchTaken   = gen.Atom("already_taken")
	daemonLaunchFailed  = gen.Atom("failed")
)

type daemonLaunchPhase uint8

const (
	daemonLaunchPhaseLaunching daemonLaunchPhase = iota + 1
	daemonLaunchPhaseRunningGrace
)

type daemonLaunchState struct {
	Launcher   gen.Atom
	Process    core.DaemonProcess
	TargetNode gen.Atom
	Epoch      int64
	Attempt    int
	Phase      daemonLaunchPhase
	StartedAt  time.Time // when this launch attempt started, for stale cleanup
}

type daemon struct {
	act.Actor
	book            core.IAddressBook
	registrar       gen.Registrar
	isLeader        bool
	cancelLaunchAll gen.CancelFunc
	recovered       map[gen.Atom]struct{}
	launching       map[gen.Atom]daemonLaunchState
	nextEpoch       int64
}

func Factory(book core.IAddressBook) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		return &daemon{
			book:      book,
			recovered: make(map[gen.Atom]struct{}),
			launching: make(map[gen.Atom]daemonLaunchState),
		}
	}
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
			w.launchAllAfter(time.Second * 10)
		}
	case core.MessageLaunchAllDaemon:
		if err := w.leaderShouldRecoverDaemon(); err != nil {
			w.launchAllAfter(time.Second * 60)
		} else {
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(time.Minute * 15)
		}
	case core.MessageEnsureDaemon:
		return w.handleEnsureDaemon(e)
	case core.MessageLaunchOneDaemon:
		return w.handleLaunchOneDaemon(e)
	case core.MessageDaemonLaunchResult:
		return w.handleDaemonLaunchResult(e)
	case messageDaemonLaunchTimeout:
		return w.handleDaemonLaunchTimeout(e)
	}
	return nil
}

func (w *daemon) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
	case events.EventNodeSwitchedToLeader:
		if e.Name == w.Node().Name() {
			w.isLeader = true
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(time.Second * 10)
			return nil
		}
	case events.EventNodeSwitchedToFollower:
		if e.Name == w.Node().Name() {
			w.isLeader = false
			w.recovered = make(map[gen.Atom]struct{})
			return nil
		}
	case events.EventNodeLeft:
		if w.isLeader {
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(time.Second * 10)
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
	duration += time.Duration(rand.Intn(1000)) * time.Millisecond
	if duration <= 0 {
		w.Send(w.PID(), core.MessageLaunchAllDaemon{})
	} else {
		if c, err := w.SendAfter(w.PID(), core.MessageLaunchAllDaemon{}, duration); err == nil {
			w.cancelLaunchAll = c
		}
	}
}

func (w *daemon) setupRegistrarMonitoring() error {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return err
		} else {
			w.registrar = registrar
		}
		event, err := registrar.Event()
		if err != nil {
			return err
		}
		if _, err := w.MonitorEvent(event); err != nil {
			return err
		} else {
			if n, err := registrar.ConfigItem(constants.LeaderNodeConfigItem); err != nil {
				return err
			} else if node, ok := n.(gen.Atom); ok {
				w.isLeader = node == w.Node().Name()
			}
		}
	}
	return nil
}

func (w *daemon) leaderShouldRecoverDaemon() (err error) {
	if !w.isLeader {
		return nil
	}
	// Clean up stale launching entries that have been in launching phase for too long.
	// This is a defensive cleanup to prevent memory leaks if timeout messages
	// never get processed (e.g. due to message drop or extreme backpressure).
	// Full recovery runs every ~15 minutes, so checking for 10x max timeout ensures
	// we only catch genuinely stale entries that will never complete.
	now := time.Now().UTC()
	maxStaleAge := 10 * daemonLaunchTimeout
	staleCount := 0
	for name, state := range w.launching {
		if now.Sub(state.StartedAt) > maxStaleAge {
			delete(w.launching, name)
			staleCount++
		}
	}
	if staleCount > 0 {
		w.Log().Debug("cleaned up %d stale launching entries", staleCount)
	}

	core.RangeLaunchers(func(_ gen.Atom, launcher core.Launcher) bool {
		if err0 := w.recoverDaemon(launcher); err0 != nil {
			err = err0
		}
		return true
	})
	return
}

func (w *daemon) recoverDaemon(launcher core.Launcher) error {
	fac := launcher.RecoveryScanner
	if fac == nil {
		return nil
	}
	next := fac()
	var retErr error
	for {
		processList, hasMore, err := next()
		if err != nil {
			return err
		}
		for _, proc := range processList {
			if _, ok := w.recovered[proc.ProcessName]; ok {
				continue
			}
			if err = w.ensureDaemon(launcher.Name, proc, 0); err != nil {
				retErr = err
			} else {
				w.recovered[proc.ProcessName] = struct{}{}
			}
		}
		if !hasMore {
			break
		}
		if len(processList) == 0 && hasMore {
			w.Log().Error("launcher %s fetch empty process list but hasMore=true", launcher.Name)
			break
		}
	}
	return retErr
}

func (w *daemon) ensureDaemon(launcher gen.Atom, proc core.DaemonProcess, attempt int) error {
	owner := w.book.PickDirectoryNode(proc.ProcessName)
	if owner == "" {
		return ErrNoAvailableNodes
	}
	msg := core.MessageEnsureDaemon{Launcher: launcher, Process: proc, Attempt: attempt}
	if owner == w.Node().Name() {
		return w.handleEnsureDaemon(msg)
	}
	if err := w.SendImportant(gen.ProcessID{Name: ProcessName, Node: owner}, msg); err != nil {
		return err
	}
	return nil
}

func (w *daemon) handleEnsureDaemon(msg core.MessageEnsureDaemon) error {
	owner := w.book.PickDirectoryNode(msg.Process.ProcessName)
	if owner == "" {
		return ErrNoAvailableNodes
	}
	if owner != w.Node().Name() {
		if err := w.SendImportant(gen.ProcessID{Name: ProcessName, Node: owner}, msg); err != nil {
			w.Log().Warning("forward ensure daemon %s to %s failed: %v", msg.Process.ProcessName, owner, err)
			return err
		}
		return nil
	}

	if runningNode, ok := w.book.LocateLocal(msg.Process.ProcessName); ok && runningNode != "" {
		delete(w.launching, msg.Process.ProcessName)
		return nil
	}

	if state, ok := w.launching[msg.Process.ProcessName]; ok {
		switch state.Phase {
		case daemonLaunchPhaseLaunching:
			return nil
		case daemonLaunchPhaseRunningGrace:
			if runningNode, ok := w.book.LocateLocal(msg.Process.ProcessName); ok && runningNode != "" {
				delete(w.launching, msg.Process.ProcessName)
			}
			return nil
		}
	}

	target := w.book.PickNode(msg.Process.ProcessName)
	if target == "" {
		return ErrNoAvailableNodes
	}

	epoch := w.nextLaunchEpoch()
	state := daemonLaunchState{
		Launcher:   msg.Launcher,
		Process:    msg.Process,
		TargetNode: target,
		Epoch:      epoch,
		Attempt:    msg.Attempt,
		Phase:      daemonLaunchPhaseLaunching,
		StartedAt:  time.Now().UTC(),
	}
	w.launching[msg.Process.ProcessName] = state
	w.scheduleLaunchTimeout(msg.Process.ProcessName, epoch, daemonLaunchTimeout)

	launchMsg := core.MessageLaunchOneDaemon{
		Launcher: msg.Launcher,
		Process:  msg.Process,
		Owner:    owner,
		Epoch:    epoch,
	}
	if target == w.Node().Name() {
		return w.handleLaunchOneDaemon(launchMsg)
	}
	if err := w.SendImportant(gen.ProcessID{Name: ProcessName, Node: target}, launchMsg); err != nil {
		delete(w.launching, msg.Process.ProcessName)
		w.Log().Warning("launch daemon %s on %s dispatch failed: %v", msg.Process.ProcessName, target, err)
		w.SendAfter(w.PID(), core.MessageEnsureDaemon{
			Launcher: msg.Launcher,
			Process:  msg.Process,
			Attempt:  msg.Attempt + 1,
		}, w.retryDelay(msg.Attempt+1))
		return err
	}
	return nil
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

	_, err := w.Spawn(func() gen.ProcessBehavior {
		return &daemonLaunchWorker{
			launcher: launcher,
			request:  msg,
		}
	}, gen.ProcessOptions{})
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

func (w *daemon) handleDaemonLaunchResult(msg core.MessageDaemonLaunchResult) error {
	owner := w.book.PickDirectoryNode(msg.Name)
	if owner != w.Node().Name() {
		delete(w.launching, msg.Name)
		return nil
	}

	state, ok := w.launching[msg.Name]
	if !ok || state.Epoch != msg.Epoch {
		return nil
	}

	switch msg.State {
	case daemonLaunchStarted, daemonLaunchTaken:
		state.Phase = daemonLaunchPhaseRunningGrace
		w.launching[msg.Name] = state
		w.scheduleLaunchTimeout(msg.Name, msg.Epoch, daemonRunningGrace)
		return nil
	default:
		delete(w.launching, msg.Name)
		w.Log().Warning("daemon process %s launch on %s failed: %s", msg.Name, msg.Node, msg.Err)
		w.SendAfter(w.PID(), core.MessageEnsureDaemon{
			Launcher: state.Launcher,
			Process:  state.Process,
			Attempt:  state.Attempt + 1,
		}, w.retryDelay(state.Attempt+1))
		return nil
	}
}

func (w *daemon) handleDaemonLaunchTimeout(msg messageDaemonLaunchTimeout) error {
	state, ok := w.launching[msg.Name]
	if !ok || state.Epoch != msg.Epoch {
		return nil
	}
	owner := w.book.PickDirectoryNode(msg.Name)
	if owner != w.Node().Name() {
		delete(w.launching, msg.Name)
		return nil
	}

	if runningNode, ok := w.book.LocateLocal(msg.Name); ok && runningNode != "" {
		delete(w.launching, msg.Name)
		return nil
	}

	delete(w.launching, msg.Name)
	w.SendAfter(w.PID(), core.MessageEnsureDaemon{
		Launcher: state.Launcher,
		Process:  state.Process,
		Attempt:  state.Attempt + 1,
	}, w.retryDelay(state.Attempt+1))
	return nil
}

func (w *daemon) nextLaunchEpoch() int64 {
	w.nextEpoch++
	return w.nextEpoch
}

func (w *daemon) scheduleLaunchTimeout(name gen.Atom, epoch int64, delay time.Duration) {
	w.SendAfter(w.PID(), messageDaemonLaunchTimeout{Name: name, Epoch: epoch}, delay)
}

func (w *daemon) retryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := min(time.Second<<min(attempt, 5), daemonRetryMaxDelay)
	return delay + time.Duration(rand.Intn(500))*time.Millisecond
}

func (w *daemon) sendLaunchResult(owner gen.Atom, result core.MessageDaemonLaunchResult) {
	if owner == "" {
		return
	}
	if err := w.SendImportant(gen.ProcessID{Name: ProcessName, Node: owner}, result); err != nil {
		w.Log().Warning("send launch result for %s to %s failed: %v", result.Name, owner, err)
	}
}

func (w *daemon) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	return nil, nil
}

func (w *daemon) HandleInspect(from gen.PID, item ...string) map[string]string {
	stats := map[string]string{
		"is_leader":       strconv.FormatBool(w.isLeader),
		"recovered_count": strconv.Itoa(len(w.recovered)),
		"launching_count": strconv.Itoa(len(w.launching)),
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

type daemonLaunchWorker struct {
	act.Actor
	launcher core.Launcher
	request  core.MessageLaunchOneDaemon
}

func (w *daemonLaunchWorker) Init(args ...any) error {
	w.Send(w.PID(), messageInit{})
	return nil
}

func (w *daemonLaunchWorker) HandleMessage(from gen.PID, message any) error {
	if _, ok := message.(messageInit); !ok {
		return nil
	}

	result := core.MessageDaemonLaunchResult{
		Name:  w.request.Process.ProcessName,
		Node:  w.Node().Name(),
		Epoch: w.request.Epoch,
		State: daemonLaunchStarted,
	}

	_, err := w.SpawnRegister(w.request.Process.ProcessName, w.launcher.Factory, w.launcher.Option, w.request.Process.Args...)
	if err != nil {
		if err == gen.ErrTaken {
			result.State = daemonLaunchTaken
		} else {
			result.State = daemonLaunchFailed
			result.Err = err.Error()
		}
	}

	if w.request.Owner != "" {
		if err := w.SendImportant(gen.ProcessID{Name: ProcessName, Node: w.request.Owner}, result); err != nil {
			w.Log().Warning("send launch result for %s to %s failed: %v", result.Name, w.request.Owner, err)
		}
	}
	return gen.TerminateReasonNormal
}
