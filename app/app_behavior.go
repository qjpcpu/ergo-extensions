package app

import (
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type simpleApp struct {
	book                    system.RWAddressBook
	cron                    []CronJob
	MemberSpecs             []gen.ApplicationMemberSpec
	SyncAddressBookInterval time.Duration
	AddressBookBuffer       int
	err                     error
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	opts := system.ApplicationMemberSepcOptions{
		CronJobs:                app.cron,
		SyncAddressBookInterval: app.SyncAddressBookInterval,
		AddressBookBuffer:       app.AddressBookBuffer,
		AddressBook:             app.book,
	}
	members := append([]gen.ApplicationMemberSpec{
		system.ApplicationMemberSepc(opts)},
		app.MemberSpecs...,
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
