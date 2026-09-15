package daemon

import (
	"fmt"
	"slices"

	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

const daemonLaunchQueued = gen.Atom("queued")

type messageDeliveryFinished struct {
	node    gen.Atom
	message any
	err     error
}

type messageLaunchReservationTimeout struct {
	entry *launchEntry
	offer core.MessageDaemonLaunchOffer
}

type launchReply struct {
	result core.MessageDaemonLaunchResult
	cancel gen.CancelFunc
}

type messageLaunchResultTimeout struct {
	offer core.MessageDaemonLaunchOffer
	reply *launchReply
}

// Entries retain only request identities. Launch arguments stay at the initiator.
type launchEntry struct {
	name     gen.Atom
	waiters  []core.MessageDaemonLaunchOffer
	selected core.MessageDaemonLaunchOffer
	worker   gen.PID
	running  bool
	cancel   gen.CancelFunc
}

type launchWorkerSlot struct {
	entry   *launchEntry
	stopped chan struct{}
}

type peerWatch struct {
	node   gen.Atom
	worker gen.PID
	offers []core.MessageDaemonLaunchOffer
}

type messagePeerMonitor struct {
	watch  *peerWatch
	worker gen.PID
	ready  bool
	err    error
}

type messagePeerDown struct{ watch *peerWatch }

func (w *daemon) watchPeer(node gen.Atom, offer *core.MessageDaemonLaunchOffer) bool {
	if node == w.Node().Name() || w.peers[node] {
		return true
	}
	watch := w.peerWatches[node]
	fresh := watch == nil
	if fresh {
		watch = &peerWatch{node: node}
		w.peerWatches[node] = watch
	}
	if offer != nil && !slices.Contains(watch.offers, *offer) {
		watch.offers = append(watch.offers, *offer)
	}
	if fresh {
		if err := w.dispatchIO(messageIO{watch: watch}); err != nil {
			w.peerDown(node)
		}
	}
	return false
}

func (w *daemon) handlePeerMonitor(msg messagePeerMonitor) {
	watch := msg.watch
	if w.peerWatches[watch.node] != watch {
		return
	}
	if msg.err != nil {
		w.peerDown(watch.node)
		return
	}
	if !msg.ready {
		// Only a local monitor runs on the control actor. The worker owns the
		// potentially blocking remote monitor and forwards its down notification.
		if err := w.MonitorPID(msg.worker); err != nil && err != gen.ErrTargetExist {
			w.peerDown(watch.node)
			return
		}
		watch.worker = msg.worker
		return
	}
	w.peers[watch.node] = true
	offers := watch.offers
	watch.offers = nil
	for key, state := range w.launching {
		if state.TargetNode == watch.node && state.Phase == daemonLaunchPhaseMonitoring {
			w.offerTask(key)
		}
	}
	for _, offer := range offers {
		w.handleLaunchOffer(offer)
	}
}

func (w *daemon) sendProtocol(node gen.Atom, message any) {
	if node == w.Node().Name() {
		if err := w.Send(w.PID(), message); err != nil {
			w.protocolFailed(node, message, err)
		}
		return
	}
	if err := w.dispatchIO(messageIO{owner: node, message: message}); err != nil {
		w.protocolFailed(node, message, err)
		return
	}
	w.pendingReplies++
}

func (w *daemon) handleDeliveryFinished(msg messageDeliveryFinished) {
	if w.pendingReplies > 0 {
		w.pendingReplies--
	}
	if msg.err != nil {
		w.protocolFailed(msg.node, msg.message, msg.err)
	}
	w.requestPendingRecovery()
}

func (w *daemon) protocolFailed(node gen.Atom, message any, err error) {
	switch msg := message.(type) {
	case core.MessageDaemonLaunchOffer:
		if state, ok := w.launching[msg.Name]; ok && state.Epoch == msg.Epoch && state.TargetNode == node && state.Phase == daemonLaunchPhaseOffering {
			w.retryTask(msg.Name)
		}
	case core.MessageLaunchOneDaemon:
		if state, ok := w.launching[msg.Process.ProcessName]; ok && state.Epoch == msg.Epoch && state.TargetNode == node && state.Phase == daemonLaunchPhaseLaunching {
			w.retryTask(msg.Process.ProcessName)
		}
	case core.MessageDaemonLaunchPull:
		entry := w.pendingLaunch[msg.Name]
		if entry != nil && !entry.running && entry.selected.Owner == node && entry.selected.Epoch == msg.Epoch {
			w.failLaunch(entry, err)
			w.pullLaunches()
		}
	}
}

func (w *daemon) offerTask(key gen.Atom) {
	state := w.launching[key]
	state.Phase = daemonLaunchPhaseMonitoring
	w.launching[key] = state
	if !w.watchPeer(state.TargetNode, nil) {
		return
	}
	if state.Cancel != nil {
		state.Cancel()
	}
	state.Phase = daemonLaunchPhaseOffering
	state.Cancel, _ = w.SendAfter(w.PID(), messageDaemonLaunchTimeout{Name: key, Epoch: state.Epoch, Phase: daemonLaunchPhaseOffering}, w.options.LaunchTimeout)
	w.launching[key] = state
	w.sendProtocol(state.TargetNode, core.MessageDaemonLaunchOffer{Name: key, Owner: w.Node().Name(), Epoch: state.Epoch})
}

func (w *daemon) cancelOffer(key gen.Atom, state daemonLaunchState) {
	switch state.Phase {
	case daemonLaunchPhaseOffering, daemonLaunchPhaseWaiting, daemonLaunchPhaseLaunching:
		w.sendProtocol(state.TargetNode, core.MessageDaemonLaunchWithdraw{Name: key, Owner: w.Node().Name(), Epoch: state.Epoch})
	}
}

func (w *daemon) handleLaunchPull(msg core.MessageDaemonLaunchPull) {
	state, ok := w.launching[msg.Name]
	if !ok || state.TargetNode != msg.Node || state.Epoch != msg.Epoch {
		w.sendProtocol(msg.Node, core.MessageDaemonLaunchWithdraw{Name: msg.Name, Owner: w.Node().Name(), Epoch: msg.Epoch})
		return
	}
	if state.Phase != daemonLaunchPhaseOffering && state.Phase != daemonLaunchPhaseWaiting {
		return
	}
	if state.Exited != (gen.PID{}) {
		w.startCheck(msg.Name)
		return
	}
	if state.Cancel != nil {
		state.Cancel()
	}
	state.Phase = daemonLaunchPhaseLaunching
	state.Cancel, _ = w.SendAfter(w.PID(), messageDaemonLaunchTimeout{Name: msg.Name, Epoch: state.Epoch, Phase: daemonLaunchPhaseLaunching}, w.options.LaunchTimeout)
	w.launching[msg.Name] = state
	w.sendProtocol(msg.Node, core.MessageLaunchOneDaemon{Launcher: state.Launcher, Process: state.Process, Owner: w.Node().Name(), Epoch: state.Epoch})
}

func (w *daemon) ensureLaunchWorkers() error {
	for len(w.launchWorkers) < daemonLaunchWorkers {
		stopped := make(chan struct{})
		pid, err := w.Spawn(func() gen.ProcessBehavior {
			return &daemonLaunchWorker{decorate: w.decorate, stopped: stopped}
		}, gen.ProcessOptions{LinkParent: true})
		if err != nil {
			return err
		}
		if err := w.MonitorPID(pid); err != nil {
			close(stopped)
			w.Node().Kill(pid)
			return err
		}
		w.launchWorkers[pid] = &launchWorkerSlot{stopped: stopped}
		w.idleWorkers = append(w.idleWorkers, pid)
	}
	return nil
}

func (w *daemon) handleLaunchOffer(msg core.MessageDaemonLaunchOffer) {
	if reply := w.launchReplies[msg]; reply != nil {
		w.sendProtocol(msg.Owner, reply.result)
		return
	}
	if !w.watchPeer(msg.Owner, &msg) {
		return
	}
	if err := w.ensureLaunchWorkers(); err != nil {
		w.sendLaunchResult(msg.Owner, core.MessageDaemonLaunchResult{Name: msg.Name, Node: w.Node().Name(), Epoch: msg.Epoch, State: daemonLaunchFailed, Err: err.Error()})
		return
	}
	entry := w.pendingLaunch[msg.Name]
	if entry == nil {
		entry = &launchEntry{name: msg.Name}
		w.pendingLaunch[msg.Name] = entry
		w.launchQueue = append(w.launchQueue, entry)
	}
	if !slices.Contains(entry.waiters, msg) {
		entry.waiters = append(entry.waiters, msg)
	}
	w.sendLaunchResult(msg.Owner, core.MessageDaemonLaunchResult{Name: msg.Name, Node: w.Node().Name(), Epoch: msg.Epoch, State: daemonLaunchQueued})
	w.pullLaunches()
}

func (w *daemon) pullLaunches() {
	for len(w.idleWorkers) > 0 && len(w.launchQueue) > 0 {
		entry := w.launchQueue[0]
		w.launchQueue = slices.Delete(w.launchQueue, 0, 1)
		pid := w.idleWorkers[0]
		w.idleWorkers = slices.Delete(w.idleWorkers, 0, 1)
		entry.worker, entry.selected = pid, entry.waiters[0]
		w.launchWorkers[pid].entry = entry
		entry.cancel, _ = w.SendAfter(w.PID(), messageLaunchReservationTimeout{entry: entry, offer: entry.selected}, w.options.LaunchTimeout)
		w.sendProtocol(entry.selected.Owner, core.MessageDaemonLaunchPull{Name: entry.name, Node: w.Node().Name(), Epoch: entry.selected.Epoch})
	}
}

func (w *daemon) handleLaunchOneDaemon(msg core.MessageLaunchOneDaemon) error {
	entry := w.pendingLaunch[msg.Process.ProcessName]
	if entry == nil || entry.worker == (gen.PID{}) || entry.running || entry.selected.Owner != msg.Owner || entry.selected.Epoch != msg.Epoch {
		return nil
	}
	launcher, ok := core.GetLauncher(msg.Launcher)
	if !ok {
		w.failLaunch(entry, fmt.Errorf("can't find launcher by %s", msg.Launcher))
		w.pullLaunches()
		return nil
	}
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
	entry.running = true
	if err := w.Send(entry.worker, messageLaunch{launcher: launcher, request: msg}); err != nil {
		w.failLaunch(entry, err)
		w.pullLaunches()
	}
	return nil
}

func (w *daemon) releaseLaunch(entry *launchEntry) {
	if entry.cancel != nil {
		entry.cancel()
		entry.cancel = nil
	}
	if slot := w.launchWorkers[entry.worker]; slot != nil && slot.entry == entry {
		slot.entry = nil
		w.idleWorkers = append(w.idleWorkers, entry.worker)
	}
	entry.worker = gen.PID{}
}

func (w *daemon) finishLaunch(worker gen.PID, msg messageLaunchFinished) {
	entry := w.pendingLaunch[msg.result.Name]
	if entry == nil || !entry.running || entry.worker != worker || entry.selected.Owner != msg.owner || entry.selected.Epoch != msg.result.Epoch {
		return
	}
	w.completeLaunch(entry, msg.result)
	w.pullLaunches()
}

func (w *daemon) completeLaunch(entry *launchEntry, result core.MessageDaemonLaunchResult) {
	w.releaseLaunch(entry)
	delete(w.pendingLaunch, entry.name)
	w.launchQueue = slices.DeleteFunc(w.launchQueue, func(e *launchEntry) bool { return e == entry })
	for _, waiter := range entry.waiters {
		result.Epoch = waiter.Epoch
		w.sendLaunchResult(waiter.Owner, result)
	}
}

func (w *daemon) failLaunch(entry *launchEntry, err error) {
	w.completeLaunch(entry, core.MessageDaemonLaunchResult{Name: entry.name, Node: w.Node().Name(), State: daemonLaunchFailed, Err: err.Error()})
}

func (w *daemon) reservationTimeout(msg messageLaunchReservationTimeout) {
	entry := w.pendingLaunch[msg.offer.Name]
	if entry != nil && entry == msg.entry && !entry.running && entry.worker != (gen.PID{}) && entry.selected == msg.offer {
		w.failLaunch(entry, gen.ErrTimeout)
		w.pullLaunches()
	}
}

func (w *daemon) withdrawLaunch(msg core.MessageDaemonLaunchWithdraw) {
	if watch := w.peerWatches[msg.Owner]; watch != nil {
		watch.offers = slices.DeleteFunc(watch.offers, func(offer core.MessageDaemonLaunchOffer) bool {
			return offer.Name == msg.Name && offer.Epoch == msg.Epoch
		})
	}
	w.clearLaunchReply(core.MessageDaemonLaunchOffer{Name: msg.Name, Owner: msg.Owner, Epoch: msg.Epoch})
	entry := w.pendingLaunch[msg.Name]
	if entry == nil {
		return
	}
	entry.waiters = slices.DeleteFunc(entry.waiters, func(offer core.MessageDaemonLaunchOffer) bool {
		return offer.Owner == msg.Owner && offer.Epoch == msg.Epoch
	})
	if entry.running {
		return // The actual Init must return before this worker can pull another task.
	}
	if entry.selected.Owner == msg.Owner && entry.selected.Epoch == msg.Epoch {
		w.releaseLaunch(entry)
		entry.selected = core.MessageDaemonLaunchOffer{}
		if len(entry.waiters) > 0 {
			w.launchQueue = append(w.launchQueue, entry)
		}
	}
	if len(entry.waiters) == 0 {
		delete(w.pendingLaunch, entry.name)
		w.launchQueue = slices.DeleteFunc(w.launchQueue, func(e *launchEntry) bool { return e == entry })
	}
	w.pullLaunches()
}

func (w *daemon) launchWorkerDown(pid gen.PID, reason error) {
	slot := w.launchWorkers[pid]
	if slot == nil {
		return
	}
	close(slot.stopped)
	delete(w.launchWorkers, pid)
	w.idleWorkers = slices.DeleteFunc(w.idleWorkers, func(id gen.PID) bool { return id == pid })
	if slot.entry != nil {
		w.failLaunch(slot.entry, reason)
	}
	if err := w.ensureLaunchWorkers(); err != nil {
		for _, entry := range slices.Clone(w.launchQueue) {
			w.failLaunch(entry, err)
		}
	}
	w.pullLaunches()
}

func (w *daemon) peerDown(node gen.Atom) {
	watch := w.peerWatches[node]
	delete(w.peerWatches, node)
	delete(w.peers, node)
	if watch != nil {
		for _, offer := range watch.offers {
			w.sendLaunchResult(node, core.MessageDaemonLaunchResult{Name: offer.Name, Node: w.Node().Name(), Epoch: offer.Epoch, State: daemonLaunchFailed, Err: gen.ErrNoConnection.Error()})
		}
	}
	for offer := range w.launchReplies {
		if offer.Owner == node {
			w.clearLaunchReply(offer)
		}
	}
	for key, state := range w.launching {
		if state.TargetNode == node && state.Phase != daemonLaunchPhaseRetrying {
			w.retryTask(key)
		}
	}
	var withdrawals []core.MessageDaemonLaunchWithdraw
	for _, entry := range w.pendingLaunch {
		for _, waiter := range entry.waiters {
			if waiter.Owner == node {
				withdrawals = append(withdrawals, core.MessageDaemonLaunchWithdraw{Name: waiter.Name, Owner: node, Epoch: waiter.Epoch})
			}
		}
	}
	for _, msg := range withdrawals {
		w.withdrawLaunch(msg)
	}
}

// Withdraw also acknowledges a terminal result, including a duplicate result
// received after the initiator has already released its task.
func (w *daemon) acknowledgeLaunchResult(result core.MessageDaemonLaunchResult) {
	if result.State != daemonLaunchQueued {
		w.sendProtocol(result.Node, core.MessageDaemonLaunchWithdraw{Name: result.Name, Owner: w.Node().Name(), Epoch: result.Epoch})
	}
}

func (w *daemon) clearLaunchReply(offer core.MessageDaemonLaunchOffer) {
	if reply := w.launchReplies[offer]; reply != nil {
		if reply.cancel != nil {
			reply.cancel()
		}
		delete(w.launchReplies, offer)
	}
}

func (w *daemon) retryLaunchResult(msg messageLaunchResultTimeout) {
	if w.launchReplies[msg.offer] != msg.reply {
		return
	}
	msg.reply.cancel, _ = w.SendAfter(w.PID(), msg, w.options.LaunchTimeout)
	w.sendProtocol(msg.offer.Owner, msg.reply.result)
}

func (w *daemon) retargetTasks() {
	for key, state := range w.launching {
		if w.book.PickNode(key) != state.TargetNode {
			delete(w.retries, key)
			w.startCheck(key)
		}
	}
}
