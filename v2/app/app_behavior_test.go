package app

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

func TestSimpleAppLoadAndLifecycle(t *testing.T) {
	book := system.NewAddressBook()
	extra := gen.ApplicationMemberSpec{Name: gen.Atom("extra-member")}
	router := newTestActorRouter(t)
	routes := newActorRoutes(book, router)
	app := newApp(book, router, routes, SimpleNodeOptions{
		NodeForwardWorker: 3,
		MembershipOptions: system.MembershipOptions{
			RefreshInterval: time.Second,
		},
		MemberSpecs: func(got ActorRoutes) []gen.ApplicationMemberSpec {
			if got != routes {
				t.Fatal("member factory received a different actor routes facade")
			}
			return []gen.ApplicationMemberSpec{extra}
		},
	})

	spec, err := app.Load(nil)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if spec.Name != "simple_app" || spec.Mode != gen.ApplicationModePermanent {
		t.Fatalf("unexpected app spec: %#v", spec)
	}
	if len(spec.Group) != 3 {
		t.Fatalf("expected system member, route member, and extra member; got %d", len(spec.Group))
	}
	if spec.Group[1].Name != routeProcessName {
		t.Fatalf("expected route member at index 1, got %s", spec.Group[1].Name)
	}
	if spec.Group[2].Name != extra.Name {
		t.Fatalf("expected extra member at index 2, got %s", spec.Group[2].Name)
	}
	app.Start(gen.ApplicationModePermanent)
	app.Terminate(nil)
}
