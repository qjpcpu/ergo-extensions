package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

const Supervisor = gen.Atom("extensions_sup")

type ApplicationMemberSpecOptions struct {
	AddressBook             *AddressBook
	CronJobs                []CronJob
	SyncAddressBookInterval time.Duration
}

func ApplicationMemberSpec(opts ApplicationMemberSpecOptions) gen.ApplicationMemberSpec {
	return gen.ApplicationMemberSpec{
		Name:    Supervisor,
		Factory: FactorySystemSup(opts),
	}
}

func FactorySystemSup(opts ApplicationMemberSpecOptions) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		sup := &systemSup{
			cron:                opts.CronJobs,
			syncProcessInterval: opts.SyncAddressBookInterval,
		}
		if opts.AddressBook != nil {
			sup.book = opts.AddressBook
		} else {
			sup.book = NewAddressBook()
		}
		return sup
	}
}

type systemSup struct {
	act.Supervisor
	book                *AddressBook
	cron                []CronJob
	syncProcessInterval time.Duration
}

func (sup *systemSup) Init(args ...any) (act.SupervisorSpec, error) {
	var spec act.SupervisorSpec

	// set supervisor type
	spec.Type = act.SupervisorTypeOneForOne

	book := sup.book

	// add children
	spec.Children = []act.SupervisorChildSpec{
		{
			Name:    WhereIsProcess,
			Factory: factoryWhereIs(book, sup.syncProcessInterval),
		},
		{
			Name:    DaemonMonitorProcess,
			Factory: factoryDaemon(book),
		},
		{
			Name:    CronJobProcess,
			Factory: factoryCron(sup.cron),
		},
	}

	// set strategy
	spec.Restart.Strategy = act.SupervisorStrategyTransient
	spec.Restart.Intensity = 2 // How big bursts of restarts you want to tolerate.
	spec.Restart.Period = 5    // In seconds.

	return spec, nil
}

// Terminate invoked on a termination supervisor process
func (sup *systemSup) Terminate(reason error) {
	sup.Log().Info("supervisor terminated with reason: %s", reason)
}
