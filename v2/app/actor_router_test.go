package app

import (
	"context"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
	"testing"
)

type testRoutePersistence struct {
	*system.MemoryActorRoutePersistence
}

func newTestRoutePersistence(t testing.TB) *testRoutePersistence {
	s := &testRoutePersistence{system.NewMemoryActorRoutePersistence()}
	t.Cleanup(s.Close)
	return s
}
func newTestActorRouter(t testing.TB) *system.ActorRouter {
	t.Helper()
	return newTestActorRouterWithPersistence(t, newTestRoutePersistence(t))
}

func newTestActorRouterWithPersistence(t testing.TB, persistence system.ActorRoutePersistence) *system.ActorRouter {
	t.Helper()
	router, err := system.NewActorRouter(persistence, system.ActorRouterOptions{})
	if err != nil {
		t.Fatalf("create test actor router: %v", err)
	}
	t.Cleanup(router.Close)
	return router
}

func routePID(store system.ActorRoutePersistence, key gen.Atom) (gen.PID, bool, error) {
	s, found, err := store.ReadRoute(context.Background(), key)
	return s.Owner.PID, found, err
}
