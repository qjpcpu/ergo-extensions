package daemon

import (
	"reflect"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
)

func TestDaemonRegistersNetworkTypes(t *testing.T) {
	for _, name := range []gen.Atom{"daemon-a@localhost", "daemon-b@localhost"} {
		node := unit.StartNode(t, name, gen.NodeOptions{})
		for range 2 {
			if _, err := node.Spawn(Factory(core.NewAddressBook(), daemonTestDecorator), gen.ProcessOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		for _, value := range []any{
			core.MessageLaunchAllDaemon{}, core.DaemonProcess{}, core.MessageEnsureDaemon{},
			core.MessageLaunchOneDaemon{}, core.MessageDaemonLaunchOffer{}, core.MessageDaemonLaunchPull{},
			core.MessageDaemonLaunchWithdraw{}, core.MessageDaemonLaunchResult{},
		} {
			want := reflect.TypeOf(value)
			got, ok := node.Network().LookupType("#" + want.PkgPath() + "/" + want.Name())
			if !ok || got != want {
				t.Fatalf("node %s: type %v not registered", name, want)
			}
		}
	}
}

func TestDaemonInitReturnsRegistrationError(t *testing.T) {
	node := unit.StartNode(t, "daemon@localhost", gen.NodeOptions{})
	node.Network().FailRegisterTypes(gen.ErrUnsupported)
	if _, err := node.Spawn(Factory(core.NewAddressBook(), daemonTestDecorator), gen.ProcessOptions{}); err == nil {
		t.Fatal("expected registration failure to fail initialization")
	}
}
