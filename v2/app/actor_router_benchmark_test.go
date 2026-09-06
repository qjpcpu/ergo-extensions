package app

import (
	"fmt"
	"sync/atomic"
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
)

type benchmarkRoutedActor struct{ act.Actor }

func BenchmarkRoutedActorSpawn(b *testing.B) {
	node, err := StartSimpleNode(SimpleNodeOptions{
		Registrar:             mem.Create(),
		ActorRoutePersistence: newTestRoutePersistence(b),
		NodeName:              "route-benchmark@localhost",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer node.Stop()
	routes := node.ActorRoutes()

	var sequence atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := gen.Atom(fmt.Sprintf("benchmark/actor/%d", sequence.Add(1)))
			if _, err := node.Spawn(func() gen.ProcessBehavior {
				return routes.WithActorRoute(key, &benchmarkRoutedActor{})
			}, gen.ProcessOptions{}); err != nil {
				b.Errorf("spawn routed actor: %v", err)
				return
			}
		}
	})
}
