package daemon

import (
	"errors"
	"strings"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

func scanJobs(actor *unit.Subject) []*scanPageRequest {
	var jobs []*scanPageRequest
	for _, record := range actor.Records() {
		if send, ok := record.(check.Send); ok {
			if job, ok := send.Message.(messageIO); ok && job.scanPage != nil {
				jobs = append(jobs, job.scanPage)
			}
		}
	}
	return jobs
}

func TestScannerPagesRunSeriallyInIOWorker(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "scanner@localhost")
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	factories, pages := 0, 0
	scan := &recoveryScan{launchers: []core.Launcher{{Name: "scanner", RecoveryScanner: func() core.DaemonIterator {
		factories++
		return func() ([]core.DaemonProcess, bool, error) {
			pages++
			return nil, pages < 2, nil
		}
	}}}}
	w.scan = scan
	w.scanStep(scan)
	w.scanStep(scan)
	jobs := scanJobs(actor)
	if len(jobs) != 1 || !w.fetching || factories != 0 {
		t.Fatal("daemon should dispatch one page task and await the worker")
	}
	worker := &daemonIOWorker{parent: actor.PID()}
	worker.Process = w.Process
	for page := 1; page <= 2; page++ {
		mark := actor.Mark()
		worker.HandleMessage(actor.PID(), messageIO{scanPage: jobs[page-1]})
		results := protocolMessages[messageScanPage](actor, mark)
		if len(results) != 1 || factories != 1 || pages != page {
			t.Fatal("worker did not reuse the iterator for successive pages")
		}
		w.handleScanPage(results[0])
		if w.fetching || !scan.loaded || !scan.scheduled {
			t.Fatal("page result did not resume daemon scheduling")
		}
		w.scanStep(scan)
		w.scanStep(scan)
		jobs = scanJobs(actor)
	}
	if w.scan != nil || len(jobs) != 2 {
		t.Fatal("two-page scan did not finish")
	}
}

func TestScannerWorkerReportsErrorsAndSurvivesPanics(t *testing.T) {
	scanErr := errors.New("scanner unavailable")
	for _, stage := range []string{"factory panic", "iterator panic", "iterator error"} {
		t.Run(stage, func(t *testing.T) {
			actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
				return &daemonIOWorker{parent: gen.PID{Node: "daemon@localhost", ID: 1}}
			}, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := &scanPageRequest{scan: &recoveryScan{}, factory: func() core.DaemonIterator {
				if stage == "factory panic" {
					panic(scanErr)
				}
				return func() ([]core.DaemonProcess, bool, error) {
					if stage == "iterator panic" {
						panic(scanErr)
					}
					return nil, false, scanErr
				}
			}}
			actor.SendMessage(gen.PID{}, messageIO{scanPage: request})
			results := protocolMessages[messageScanPage](actor, 0)
			if len(results) != 1 || results[0].request != request || results[0].scan != request.scan || results[0].err == nil {
				t.Fatalf("scanner failure was not returned as a page result: %+v", results)
			}
			if stage == "iterator error" && !errors.Is(results[0].err, scanErr) {
				t.Fatal("scanner error was not preserved")
			}
			if stage != "iterator error" && !strings.Contains(results[0].err.Error(), "scanner panic") {
				t.Fatal("panic was not reported")
			}
			mark := actor.Mark()
			actor.SendMessage(gen.PID{}, messageIO{scanPage: &scanPageRequest{iterator: func() ([]core.DaemonProcess, bool, error) {
				return []core.DaemonProcess{{ProcessName: "healthy"}}, false, nil
			}}})
			results = protocolMessages[messageScanPage](actor, mark)
			if len(results) != 1 || results[0].err != nil || len(results[0].page) != 1 {
				t.Fatal("worker did not process the next page task after failure")
			}
		})
	}
}

func TestScannerPoolExitContinuesWithNextLauncher(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "scanner@localhost")
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	scan := &recoveryScan{launchers: []core.Launcher{{Name: "first"}, {Name: "second"}}}
	w.scan = scan
	w.scanStep(scan)
	old := w.scanFetch
	w.HandleMessage(actor.PID(), gen.MessageDownPID{PID: w.ioPool, Reason: gen.TerminateReasonKill})
	if w.fetching || !scan.failed || scan.launchers[0].Name != "second" || !scan.scheduled {
		t.Fatal("pool exit did not release the failed fetch and continue recovery")
	}
	w.scanStep(scan)
	current := w.scanFetch
	w.handleScanPage(messageScanPage{request: old, scan: scan, page: []core.DaemonProcess{{ProcessName: "old-page"}}})
	if !w.fetching || w.scanFetch != current || scan.loaded {
		t.Fatal("late result replaced the current page task")
	}
	w.handleScanPage(messageScanPage{request: current, scan: scan, page: []core.DaemonProcess{{ProcessName: "new-page"}}})
	if w.fetching || !scan.loaded || scan.page[0].ProcessName != "new-page" {
		t.Fatal("current page result did not resume recovery")
	}
}

func TestScannerDispatchFailureReleasesFetch(t *testing.T) {
	actor := spawnDaemonUnit(t, core.NewAddressBook(), "scanner@localhost")
	w := actor.Behavior().(*daemon)
	w.isLeader = true
	w.ioPool = gen.PID{Node: "scanner@localhost", ID: 123}
	actor.OnSend(w.ioPool).Fail(errors.New("pool unavailable"))
	scan := &recoveryScan{launchers: []core.Launcher{{Name: "scanner"}}}
	w.scan = scan
	w.scanStep(scan)
	if w.fetching || w.scanFetch != nil || !scan.failed || !scan.scheduled {
		t.Fatal("failed dispatch left scanner waiting for a result")
	}
}
