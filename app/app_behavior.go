package app

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type simpleApp struct {
	book  *system.AddressBook
	hints *routeHintCache
	opts  SimpleNodeOptions
}

func newApp(book *system.AddressBook, opts SimpleNodeOptions) *simpleApp {
	return &simpleApp{
		book:  book,
		hints: newRouteHintCache(opts.SyncProcessInterval),
		opts:  opts,
	}
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	var members []gen.ApplicationMemberSpec
	opts := system.ApplicationMemberSpecOptions{
		CronSource:               app.opts.CronSource,
		CronSchedulerOptions:     app.opts.CronSchedulerOptions,
		SyncAddressBookInterval:  app.opts.SyncProcessInterval,
		PlacementMonitorInterval: app.opts.PlacementMonitorInterval,
		AddressBook:              app.book,
	}
	members = append(members, system.ApplicationMemberSpec(opts), app.routeMemberSpec())
	members = append(members, app.opts.MemberSpecs...)
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
			return newRouteActor(app.book, app.hints)
		}, app.opts.NodeForwardWorker),
	}
}
