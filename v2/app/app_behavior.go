package app

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type simpleApp struct {
	book   *system.AddressBook
	router *system.ActorRouter
	routes ActorRoutes
	opts   SimpleNodeOptions
}

func newApp(book *system.AddressBook, router *system.ActorRouter, routes ActorRoutes, opts SimpleNodeOptions) *simpleApp {
	return &simpleApp{
		book:   book,
		router: router,
		routes: routes,
		opts:   opts,
	}
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	var members []gen.ApplicationMemberSpec
	opts := system.ApplicationMemberSpecOptions{
		CronSource:           app.opts.CronSource,
		CronSchedulerOptions: app.opts.CronSchedulerOptions,
		ActorRouter:          app.router,
		MembershipOptions:    app.opts.MembershipOptions,
		DaemonOptions:        app.opts.DaemonOptions,
		AddressBook:          app.book,
	}
	members = append(members, system.ApplicationMemberSpec(opts), app.routeMemberSpec())
	if app.opts.MemberSpecs != nil {
		members = append(members, app.opts.MemberSpecs(app.routes)...)
	}
	return gen.ApplicationSpec{
		Name:        "simple_app",
		Description: "Simple application",
		Mode:        gen.ApplicationModePermanent,
		Group:       members,
		Depends:     gen.ApplicationDepends{Network: true},
	}, nil
}

func (app *simpleApp) Start(mode gen.ApplicationMode) {}
func (app *simpleApp) Terminate(reason error)         {}

func (app *simpleApp) routeMemberSpec() gen.ApplicationMemberSpec {
	return gen.ApplicationMemberSpec{
		Name: routeProcessName,
		Factory: CreatePool(func() gen.ProcessBehavior {
			return newRouteActor(app.book)
		}, app.opts.NodeForwardWorker),
	}
}
