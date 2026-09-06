package daemon

import (
	"context"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"errors"
	"fmt"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"reflect"
	"testing"
	"time"
)

func TestRemoteRecoveryCapacityIncludesRetries(t *testing.T) {
	self, remote := gen.Atom("capacity-a@localhost"), gen.Atom("capacity-b@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.options.ScanBatchInterval = 0
	w.options.MaxInFlight = 64
	w.options.ScanBatchSize = 32
	w.isLeader = true
	scan := &recoveryScan{launchers: []core.Launcher{{Name: "l"}}, loaded: true, started: time.Now()}
	for i := 0; i < 1000; i++ {
		scan.page = append(scan.page, core.DaemonProcess{ProcessName: gen.Atom(fmt.Sprint(i))})
	}
	w.scan = scan
	w.scanStep(scan)
	w.scanStep(scan)
	w.scanStep(scan)
	if len(w.launching) != 64 || len(scan.page) != 936 || len(w.retries) != 0 {
		t.Fatal(len(w.launching), len(scan.page), len(w.retries))
	}
	for key, state := range w.launching {
		w.handleIOResult(messageIOResult{key: key, epoch: state.Epoch, err: errors.New("slow store")})
	}
	w.scanStep(scan)
	if len(w.launching) != 64 || len(w.retries) != 64 || len(scan.page) != 936 {
		t.Fatal("retries escaped capacity")
	}
	for len(scan.page) > 0 {
		for key, state := range w.launching {
			w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: key, Node: remote, Epoch: state.Epoch, State: daemonLaunchStarted})
		}
		w.scanStep(scan)
		if len(w.launching) > 64 {
			t.Fatal("capacity exceeded")
		}
	}
	for key := range w.launching {
		w.completeTask(key)
	}
	if len(w.retries) != 0 {
		t.Fatal("retry timers retained")
	}
}

func TestRecoveryRotatesLauncherPages(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("fair@localhost")
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	var order []string
	big := core.Launcher{Name: "big", RecoveryScanner: func() core.DaemonIterator {
		i := 0
		return func() ([]core.DaemonProcess, bool, error) {
			i++
			order = append(order, "big")
			return []core.DaemonProcess{{ProcessName: gen.Atom(fmt.Sprint(i))}}, i < 3, nil
		}
	}}
	small := core.Launcher{Name: "small", RecoveryScanner: func() core.DaemonIterator {
		return func() ([]core.DaemonProcess, bool, error) { order = append(order, "small"); return nil, false, nil }
	}}
	w.scan = &recoveryScan{launchers: []core.Launcher{big, small}, started: time.Now()}
	w.Send(w.PID(), messageScanStep{w.scan})
	driveRecoveryScan(t, actor, w)
	if !reflect.DeepEqual(order, []string{"big", "small", "big", "big"}) {
		t.Fatal(order)
	}
}

type blockingBook struct {
	core.IAddressBook
	entered chan struct{}
	finish  chan struct{}
}

func (b blockingBook) Locate(context.Context, gen.Atom) (gen.PID, bool, error) {
	close(b.entered)
	<-b.finish
	return gen.PID{}, false, errors.New("slow store")
}
func TestSlowLookupLeavesDaemonResponsive(t *testing.T) {
	self := gen.Atom("responsive@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(self))
	slow := blockingBook{IAddressBook: book, entered: make(chan struct{}), finish: make(chan struct{})}
	actor := spawnDaemonUnit(t, slow, self)
	w := actor.Behavior().(*daemon)
	w.handleEnsureDaemon(core.MessageEnsureDaemon{Launcher: "l", Process: core.DaemonProcess{ProcessName: "key"}})
	var job messageIO
	for _, e := range actor.Events() {
		if s, ok := e.(unit.SendEvent); ok {
			if m, ok := s.Message.(messageIO); ok {
				job = m
			}
		}
	}
	worker, err := unit.Spawn(t, func() gen.ProcessBehavior { return &daemonIOWorker{book: slow, parent: actor.PID()} })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { worker.SendMessage(actor.PID(), job); close(done) }()
	<-slow.entered
	if w.HandleInspect(actor.PID())["launching_count"] != "1" {
		t.Fatal("daemon control state unavailable")
	}
	w.HandleMessage(actor.PID(), core.MessageTopologyUpdated{})
	close(slow.finish)
	<-done
}

func TestRecoveryPacesBatchesAfterCompletion(t *testing.T) {
	self, remote := gen.Atom("paced-a@localhost"), gen.Atom("paced-b@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	w.options.ScanBatchSize = 2
	w.options.ScanBatchInterval = 100 * time.Millisecond
	scan := &recoveryScan{launchers: []core.Launcher{{Name: "l"}}, loaded: true, started: time.Now(), page: []core.DaemonProcess{{ProcessName: "a"}, {ProcessName: "b"}, {ProcessName: "c"}}}
	w.scan = scan
	w.scanStep(scan)
	for key, state := range w.launching {
		w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: key, Node: remote, Epoch: state.Epoch, State: daemonLaunchStarted})
	}
	for i := 0; i < 100; i++ {
		w.scanStep(scan)
	}
	if len(scan.page) != 1 {
		t.Fatal("completion bypassed batch interval")
	}
	time.Sleep(time.Until(scan.nextBatchAt))
	w.scanStep(scan)
	if len(scan.page) != 0 {
		t.Fatal("next batch did not progress")
	}
}

func TestFailedScannerTasksReleaseCapacityForOtherLaunchers(t *testing.T) {
	self, remote := gen.Atom("failure-a@localhost"), gen.Atom("failure-b@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(remote))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	w.options.ScanBatchInterval = 0
	launcher := gen.Atom(t.Name())
	if err := core.RegisterLauncher(launcher, core.Launcher{Factory: func() gen.ProcessBehavior { return &daemonTestProc{} }, RecoveryScanner: core.SingletonDaemon("failed", nil)}); err != nil {
		t.Fatal(err)
	}
	defer core.UnregisterLauncher(launcher)
	scan := &recoveryScan{launchers: []core.Launcher{{Name: launcher}}, loaded: true, started: time.Now()}
	for i := 0; i < 1000; i++ {
		scan.page = append(scan.page, core.DaemonProcess{ProcessName: gen.Atom(fmt.Sprint(i))})
	}
	w.scan = scan
	for len(scan.page) > 0 {
		w.scanStep(scan)
		if len(w.launching) > w.options.MaxInFlight {
			t.Fatal("admission exceeded capacity")
		}
		for key, state := range w.launching {
			w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: key, Node: remote, Epoch: state.Epoch, State: daemonLaunchFailed, Err: "business dependency unavailable"})
		}
		if len(w.launching) != 0 || len(w.retries) != 0 {
			t.Fatal("failed scanner tasks retained capacity")
		}
		if err := w.ensureDaemon("healthy", core.DaemonProcess{ProcessName: "healthy"}, 0); err != nil {
			t.Fatal("healthy launcher blocked:", err)
		}
		state := w.launching["healthy"]
		w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "healthy", Node: remote, Epoch: state.Epoch, State: daemonLaunchStarted})
	}
	if !scan.failed {
		t.Fatal("failed scan must be retried")
	}
	w.finishScan(scan.failed)
	if w.cancelLaunchAll == nil {
		t.Fatal("next scan was not scheduled")
	}
}

func TestScannerFailureAfterScanCoalescesRecovery(t *testing.T) {
	self := gen.Atom("late-failure@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	launcher := gen.Atom(t.Name())
	if err := core.RegisterLauncher(launcher, core.Launcher{Factory: func() gen.ProcessBehavior { return &daemonTestProc{} }, RecoveryScanner: core.SingletonDaemon("failed", nil)}); err != nil {
		t.Fatal(err)
	}
	defer core.UnregisterLauncher(launcher)
	for i := 0; i < 100; i++ {
		key := gen.Atom(fmt.Sprint(i))
		if err := w.ensureDaemon(launcher, core.DaemonProcess{ProcessName: key}, 0); err != nil {
			t.Fatal(err)
		}
		state := w.launching[key]
		w.handleIOResult(messageIOResult{key: key, epoch: state.Epoch, err: errors.New("lookup failed")})
	}
	timers := 0
	for _, event := range actor.Events() {
		if e, ok := event.(unit.SendEvent); ok {
			if _, ok := e.Message.(messageScanRetry); ok {
				timers++
			}
		}
	}
	if timers != 1 || len(w.launching) != 0 {
		t.Fatalf("timers=%d tasks=%d", timers, len(w.launching))
	}
	w.HandleMessage(actor.PID(), messageScanRetry{})
	if !w.wantRecovery {
		t.Fatal("follower must retain request until leader is available")
	}
}

func TestScannerTaskRetainsExactExitCleanupUntilSuccess(t *testing.T) {
	self := gen.Atom("cleanup-capacity@localhost")
	book := core.NewAddressBook()
	book.SetAvailableNodes(core.NewNodeList(self))
	actor := spawnDaemonUnit(t, book, self)
	w := actor.Behavior().(*daemon)
	launcher := gen.Atom(t.Name())
	if err := core.RegisterLauncher(launcher, core.Launcher{Factory: func() gen.ProcessBehavior { return &daemonTestProc{} }, RecoveryScanner: core.SingletonDaemon("key", nil)}); err != nil {
		t.Fatal(err)
	}
	defer core.UnregisterLauncher(launcher)
	old := gen.PID{Node: self, ID: 1000, Creation: 1}
	if err := w.admit(core.MessageEnsureDaemon{Launcher: launcher, Process: core.DaemonProcess{ProcessName: "key"}}, old); err != nil {
		t.Fatal(err)
	}
	state := w.launching["key"]
	w.handleIOResult(messageIOResult{key: "key", epoch: state.Epoch, exited: old, err: errors.New("release failed")})
	state, ok := w.launching["key"]
	if !ok || state.Exited != old || len(w.retries) != 1 {
		t.Fatal("exact-owner cleanup was lost")
	}
	w.HandleMessage(actor.PID(), messageRetry{Name: "key", Epoch: state.Epoch})
	state = w.launching["key"]
	w.handleIOResult(messageIOResult{key: "key", epoch: state.Epoch, exited: old})
	w.handleDaemonLaunchResult(core.MessageDaemonLaunchResult{Name: "key", Epoch: state.Epoch, Node: self, State: daemonLaunchFailed})
	if len(w.launching) != 0 || w.cancelScanRetry == nil {
		t.Fatal("cleaned task did not yield to scan retry")
	}
}
