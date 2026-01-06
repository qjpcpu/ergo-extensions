package app

import (
	"fmt"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

type simpleApp struct {
	book                    system.IAddressBook
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
	}
	switch book := app.book.(type) {
	case *system.AddressBook:
		opts.AddressBook = book
	case *system.PersistAddressBook:
		opts.AddressBook = book
	default:
		return gen.ApplicationSpec{}, fmt.Errorf("address book<%T> not supported", app.book)
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
