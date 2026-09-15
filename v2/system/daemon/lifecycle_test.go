package daemon

import (
	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"testing"
	"time"
)

func TestDaemonStopsLaunchWorkersOnRestart(t *testing.T) {
	node, err := ergo.StartNode("pool-lifetime@localhost", gen.NodeOptions{Network: gen.NetworkOptions{Mode: gen.NetworkModeDisabled}, Log: gen.LogOptions{Level: gen.LogLevelDisabled}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	for i := 0; i < 5; i++ {
		scheduler := &launchLifecycleDaemon{daemon: Factory(core.NewAddressBook(), nil)().(*daemon)}
		pid, err := node.Spawn(func() gen.ProcessBehavior { return scheduler }, gen.ProcessOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var stopped []<-chan struct{}
		for _, slot := range scheduler.launchWorkers {
			stopped = append(stopped, slot.stopped)
		}
		if err := node.Kill(pid); err != nil {
			t.Fatal(err)
		}
		for _, done := range stopped {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("daemon did not signal waiting workers while node remained alive")
			}
		}
	}
}

type launchLifecycleDaemon struct{ *daemon }

func (w *launchLifecycleDaemon) Init(...any) error {
	w.SetTrapExit(true)
	return w.ensureLaunchWorkers()
}
