package daemon

import (
	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"testing"
	"time"
)

func TestLaunchPoolStopsWorkersOnRestart(t *testing.T) {
	node, err := ergo.StartNode("pool-lifetime@localhost", gen.NodeOptions{Network: gen.NetworkOptions{Mode: gen.NetworkModeDisabled}, Log: gen.LogOptions{Level: gen.LogLevelDisabled}})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	for i := 0; i < 5; i++ {
		pool := &daemonLaunchPool{}
		pid, err := node.Spawn(func() gen.ProcessBehavior { return pool }, gen.ProcessOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stopped := pool.stopped
		if err := node.Kill(pid); err != nil {
			t.Fatal(err)
		}
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("pool did not signal waiting workers while node remained alive")
		}
	}
}
