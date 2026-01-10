package app

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type simpleApp struct {
	book system.RWAddressBook
	opts SimpleNodeOptions
}

func newApp(book system.RWAddressBook, opts SimpleNodeOptions) *simpleApp {
	return &simpleApp{
		book: book,
		opts: opts,
	}
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	var members []gen.ApplicationMemberSpec
	opts := system.ApplicationMemberSepcOptions{
		CronJobs:                app.opts.CronJobs,
		SyncAddressBookInterval: app.opts.SyncProcessInterval,
		AddressBookBuffer:       app.opts.ProcessChangeBuffer,
		AddressBook:             app.book,
	}
	members = append([]gen.ApplicationMemberSpec{
		system.ApplicationMemberSepc(opts)},
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
