package cron

import (
	"reflect"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

func TestCronRegistersNetworkTypes(t *testing.T) {
	for _, name := range []gen.Atom{"cron-a@localhost", "cron-b@localhost"} {
		node := unit.StartNode(t, name, gen.NodeOptions{})
		for range 2 {
			if _, err := node.Spawn(Factory(nil, SchedulerOptions{}), gen.ProcessOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		for _, value := range []any{InspectRequest{}, MessageTrigger{}, MessageTriggerBatch{}} {
			want := reflect.TypeOf(value)
			got, ok := node.Network().LookupType("#" + want.PkgPath() + "/" + want.Name())
			if !ok || got != want {
				t.Fatalf("node %s: type %v not registered", name, want)
			}
		}
	}
}

func TestCronInitReturnsRegistrationError(t *testing.T) {
	node := unit.StartNode(t, "cron@localhost", gen.NodeOptions{})
	node.Network().FailRegisterTypes(gen.ErrUnsupported)
	if _, err := node.Spawn(Factory(nil, SchedulerOptions{}), gen.ProcessOptions{}); err == nil {
		t.Fatal("expected registration failure to fail initialization")
	}
}
