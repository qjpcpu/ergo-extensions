package app

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type simpleApp struct {
	book *system.AddressBook
	opts SimpleNodeOptions
}

func newApp(book *system.AddressBook, opts SimpleNodeOptions) *simpleApp {
	return &simpleApp{
		book: book,
		opts: opts,
	}
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	var members []gen.ApplicationMemberSpec
	opts := system.ApplicationMemberSpecOptions{
		CronJobs:                app.opts.CronJobs,
		SyncAddressBookInterval: app.opts.SyncProcessInterval,
		AddressBook:             app.book,
	}
	members = append([]gen.ApplicationMemberSpec{
		system.ApplicationMemberSpec(opts)},
		app.opts.MemberSpecs...,
	)
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
