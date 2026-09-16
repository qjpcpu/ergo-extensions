package core

import (
	"errors"
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/unit"
)

type recoveryLifecycleActor struct {
	act.Actor
	cleaned      bool
	panicCleanup bool
	initError    error
}

func (a *recoveryLifecycleActor) Init(...any) error { return a.initError }
func (a *recoveryLifecycleActor) Terminate(error) {
	a.cleaned = true
	if a.panicCleanup {
		panic("cleanup failed")
	}
}

func TestDaemonRecoveryNotifiesAfterCleanup(t *testing.T) {
	for _, mode := range []string{"normal", "shutdown", "cleanup panic", "init failure"} {
		t.Run(mode, func(t *testing.T) {
			b := &recoveryLifecycleActor{panicCleanup: mode == "cleanup panic"}
			if mode == "init failure" {
				b.initError = errors.New("load failed")
			}
			process := DaemonProcess{ProcessName: "worker", Args: []any{"persisted-argument"}}
			wrapped := WithDaemonRecovery(func() gen.ProcessBehavior { return b }, "launcher", process)()
			parent, err := unit.Spawn(t, func() gen.ProcessBehavior { return &spawnerParentProc{} }, gen.ProcessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			err = wrapped.ProcessInit(parent.Behavior().(gen.Process))
			if !errors.Is(err, b.initError) {
				t.Fatal(err)
			}
			reason := gen.TerminateReasonNormal
			if mode == "shutdown" {
				reason = gen.TerminateReasonShutdown
			}
			parent.Node().OnSend(gen.Atom("extensions_daemon")).FailFunc(func() error {
				if !b.cleaned {
					t.Error("recovery preceded cleanup")
				}
				return nil
			})
			var caught any
			func() { defer func() { caught = recover() }(); wrapped.ProcessTerminate(reason) }()
			if (caught != nil) != b.panicCleanup {
				t.Fatal("unexpected cleanup result", caught)
			}
			var messages []MessageDaemonExited
			for _, r := range parent.Records() {
				if s, ok := r.(check.Send); ok {
					if m, ok := s.Message.(MessageDaemonExited); ok {
						messages = append(messages, m)
					}
				}
			}
			want := 1
			if mode == "shutdown" || mode == "init failure" {
				want = 0
			}
			if len(messages) != want {
				t.Fatalf("got %d recovery notifications, want %d", len(messages), want)
			}
			if want == 1 && (messages[0].PID != parent.PID() || messages[0].Ensure.Launcher != "launcher" || messages[0].Ensure.Process.Args[0] != "persisted-argument") {
				t.Fatalf("lost recovery identity: %+v", messages)
			}
		})
	}
}
