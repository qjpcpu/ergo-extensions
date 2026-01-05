package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

const Supervisor = gen.Atom("extensions_sup")

type ApplicationMemberSepcOptions struct {
	AddressBook             *AddressBook
	PersistAddressBook      *PersistAddressBook
	CronJobs                []CronJob
	SyncAddressBookInterval time.Duration
	AddressBookBuffer       int
}

func ApplicationMemberSepc(opts ApplicationMemberSepcOptions) gen.ApplicationMemberSpec {
	return gen.ApplicationMemberSpec{
		Name:    Supervisor,
		Factory: FactorySystemSup(opts),
	}
}

func FactorySystemSup(opts ApplicationMemberSepcOptions) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		sup := &systemSup{
			cron:                opts.CronJobs,
			syncProcessInterval: opts.SyncAddressBookInterval,
			changeBufferCap:     opts.AddressBookBuffer,
			persistBook:         opts.PersistAddressBook,
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
	persistBook         *PersistAddressBook
	book                *AddressBook
	cron                []CronJob
	syncProcessInterval time.Duration
	changeBufferCap     int
}

var system_process = map[gen.Atom]struct{}{
	WhereIsProcess:       struct{}{},
	DaemonMonitorProcess: struct{}{},
	CronJobProcess:       struct{}{},
	Supervisor:           struct{}{},
}

func isSystemProc(p gen.Atom) bool {
	_, ok := system_process[p]
	return ok
}

func (sup *systemSup) Init(args ...any) (act.SupervisorSpec, error) {
	var spec act.SupervisorSpec

	// set supervisor type
	spec.Type = act.SupervisorTypeOneForOne

	book := sup.book

	// add children
	makeWhereIs := func() gen.ProcessFactory {
		if sup.persistBook != nil {
			return factory_persist_whereis(sup.persistBook, sup.syncProcessInterval)
		}
		return factory_whereis(book, sup.syncProcessInterval, sup.changeBufferCap)
	}
	spec.Children = []act.SupervisorChildSpec{
		{
			Name:    WhereIsProcess,
			Factory: makeWhereIs(),
		},
		{
			Name:    DaemonMonitorProcess,
			Factory: factory_daemon(book),
		},
		{
			Name:    CronJobProcess,
			Factory: factory_cron(sup.cron),
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
