package main

import (
	"context"
	"fmt"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/app"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type echo struct{ act.Actor }

func (e *echo) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	return request, nil
}

func main() {
	store := system.NewMemoryActorRoutePersistence()
	defer store.Close()
	node, err := app.StartSimpleNode(app.SimpleNodeOptions{
		Registrar:             mem.Create(),
		NodeName:              "example@localhost",
		ActorRoutePersistence: store,
	})
	if err != nil {
		panic(err)
	}
	defer node.Stop()
	routes := node.ActorRoutes()

	key := gen.Atom("examples/echo")
	_, err = node.Spawn(func() gen.ProcessBehavior {
		return routes.WithActorRoute(key, &echo{})
	}, gen.ProcessOptions{})
	if err != nil {
		panic(err)
	}

	pid, found, err := routes.Locate(context.Background(), key)
	if err != nil {
		panic(err)
	}
	if !found {
		panic("route was not found")
	}

	reply, err := node.Call(pid, "hello")
	if err != nil {
		panic(err)
	}
	fmt.Println(reply)
}
