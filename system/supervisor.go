package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	cronpkg "github.com/qjpcpu/ergo-extensions/system/cron"
	"github.com/qjpcpu/ergo-extensions/system/daemon"
	"github.com/qjpcpu/ergo-extensions/system/whereis"
)

const Supervisor = gen.Atom("extensions_sup")

type ApplicationMemberSpecOptions struct {
	AddressBook              *AddressBook
	CronSource               cronpkg.Source
	CronSchedulerOptions     cronpkg.SchedulerOptions
	WhereIsOptions           whereis.Options
	DaemonOptions            daemon.Options
	PlacementMonitorInterval time.Duration
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
			cronSource:               opts.CronSource,
			cronOptions:              opts.CronSchedulerOptions,
			whereisOptions:           opts.WhereIsOptions,
			daemonOptions:            opts.DaemonOptions,
			placementMonitorInterval: opts.PlacementMonitorInterval,
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
	book                     *AddressBook
	cronSource               cronpkg.Source
	cronOptions              cronpkg.SchedulerOptions
	whereisOptions           whereis.Options
	daemonOptions            daemon.Options
	placementMonitorInterval time.Duration
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
			Factory: whereis.FactoryWithOptions(book, sup.whereisOptions),
		},
		{
			Name:    DaemonMonitorProcess,
			Factory: daemon.FactoryWithOptions(book, sup.daemonOptions),
		},
		{
			Name:    PlacementMonitorProcess,
			Factory: whereis.MonitorPlacementFactory(sup.placementMonitorInterval),
		},
		{
			Name:    CronJobProcess,
			Factory: cronpkg.Factory(sup.cronSource, sup.cronOptions),
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
