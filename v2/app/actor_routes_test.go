package app

import (
	"context"
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

type actorRoutesTestActor struct{ act.Actor }
type actorRoutesTestSupervisor struct{ act.Supervisor }
type actorRoutesTestPool struct{ act.Pool }

func (*actorRoutesTestSupervisor) Init(...any) (act.SupervisorSpec, error) {
	return act.SupervisorSpec{}, nil
}

func (*actorRoutesTestPool) Init(...any) (act.PoolOptions, error) {
	return act.PoolOptions{}, nil
}

func TestActorRoutesLookupAndDecorators(t *testing.T) {
	pid := gen.PID{Node: "node-a@localhost", ID: 1, Creation: 1}
	router := newTestActorRouter(t)
	routes := newActorRoutes(actorLocatorStub{
		routes: map[gen.Atom]gen.PID{"worker": pid},
	}, router)

	got, found, err := routes.Locate(context.Background(), "worker")
	if err != nil || !found || got != pid {
		t.Fatalf("unexpected route lookup: pid=%v found=%v err=%v", got, found, err)
	}
	if routed := routes.WithActorRoute("actor", &actorRoutesTestActor{}); routed == nil {
		t.Fatal("expected routed actor")
	}
	if routed := routes.WithSupervisorRoute("supervisor", &actorRoutesTestSupervisor{}); routed == nil {
		t.Fatal("expected routed supervisor")
	}
	if routed := routes.WithPoolRoute("pool", &actorRoutesTestPool{}); routed == nil {
		t.Fatal("expected routed pool")
	}
}
