package system

import (
	"errors"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	cronpkg "github.com/qjpcpu/ergo-extensions/v2/system/cron"
	"github.com/qjpcpu/ergo-extensions/v2/system/daemon"
	"github.com/qjpcpu/ergo-extensions/v2/system/membership"
)

const Supervisor = gen.Atom("extensions_sup")
const MembershipProcess = membership.ProcessName

type ApplicationMemberSpecOptions struct {
	AddressBook          *AddressBook
	ActorRouter          *ActorRouter
	CronSource           cronpkg.Source
	CronSchedulerOptions cronpkg.SchedulerOptions
	MembershipOptions    membership.Options
	DaemonOptions        daemon.Options
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
			cronSource:        opts.CronSource,
			cronOptions:       opts.CronSchedulerOptions,
			router:            opts.ActorRouter,
			membershipOptions: opts.MembershipOptions,
			daemonOptions:     opts.DaemonOptions,
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
	book              *AddressBook
	router            *ActorRouter
	cronSource        cronpkg.Source
	cronOptions       cronpkg.SchedulerOptions
	membershipOptions membership.Options
	daemonOptions     daemon.Options
}

func (sup *systemSup) Init(args ...any) (act.SupervisorSpec, error) {
	var spec act.SupervisorSpec
	if sup.router == nil {
		return spec, errors.New("actor router is required")
	}
	if err := sup.router.Bind(sup.Node()); err != nil {
		return spec, err
	}
	if err := sup.book.BindLocator(sup.Node().Name(), sup.router.lookup); err != nil {
		return spec, err
	}

	// set supervisor type
	spec.Type = act.SupervisorTypeOneForOne

	book := sup.book

	// add children
	spec.Children = []act.SupervisorChildSpec{
		{
			Name:    MembershipProcess,
			Factory: membership.Factory(book, sup.membershipOptions),
		},
		{
			Name:    DaemonMonitorProcess,
			Factory: daemon.FactoryWithRouteCleanup(book, sup.router.routeFactory, sup.daemonOptions, sup.router.releaseExitedRoute),
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
