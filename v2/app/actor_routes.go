package app

import (
	"context"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type actorRoutes struct {
	locator actorLocator
	router  *system.ActorRouter
}

var _ ActorRoutes = (*actorRoutes)(nil)
var _ Topology = (*system.AddressBook)(nil)

func newActorRoutes(locator actorLocator, router *system.ActorRouter) *actorRoutes {
	return &actorRoutes{locator: locator, router: router}
}

func (r *actorRoutes) Locate(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	return r.locator.Locate(ctx, key)
}

func (r *actorRoutes) WithActorRoute(key gen.Atom, actor system.IActor) system.IActor {
	return r.router.WithActorRoute(key, actor)
}

func (r *actorRoutes) WithSupervisorRoute(key gen.Atom, supervisor system.ISupervisor) system.ISupervisor {
	return r.router.WithSupervisorRoute(key, supervisor)
}

func (r *actorRoutes) WithPoolRoute(key gen.Atom, pool system.IPool) system.IPool {
	return r.router.WithPoolRoute(key, pool)
}

func (r *actorRoutes) Stats() system.ActorRouterStats { return r.router.Stats() }
