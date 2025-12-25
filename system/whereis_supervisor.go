package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

const WhereIsSupervisor = gen.Atom("whereissup")

type ApplicationMemberSepcOptions struct {
	AddressBook             *AddressBook
	CronJobs                []CronJob
	SyncAddressBookInterval time.Duration
	AddressBookBuffer       int
}

func ApplicationMemberSepc(opts ApplicationMemberSepcOptions) gen.ApplicationMemberSpec {
	return gen.ApplicationMemberSpec{
		Name:    WhereIsSupervisor,
		Factory: FactoryWhereisSup(opts),
	}
}

func FactoryWhereisSup(opts ApplicationMemberSepcOptions) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		sup := &WhereisSup{cron: opts.CronJobs, syncProcessInterval: opts.SyncAddressBookInterval, changeBufferCap: opts.AddressBookBuffer}
		if opts.AddressBook != nil {
			sup.book = opts.AddressBook
		} else {
			sup.book = NewAddressBook()
		}
		return sup
	}
}

type WhereisSup struct {
	act.Supervisor
	book                *AddressBook
	cron                []CronJob
	syncProcessInterval time.Duration
	changeBufferCap    int
}

func (sup *WhereisSup) Init(args ...any) (act.SupervisorSpec, error) {
	var spec act.SupervisorSpec

	// set supervisor type
	spec.Type = act.SupervisorTypeOneForOne

	book := sup.book

	// add children
	spec.Children = []act.SupervisorChildSpec{
		{
			Name:    WhereIsProcess,
			Factory: factory_whereis(book, sup.syncProcessInterval, sup.changeBufferCap),
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

//
// Methods below are optional, so you can remove those that aren't be used
//

// HandleChildStart invoked on a successful child process starting if option EnableHandleChild
// was enabled in act.SupervisorSpec
func (sup *WhereisSup) HandleChildStart(name gen.Atom, pid gen.PID) error {
	return nil
}

// HandleChildTerminate invoked on a child process termination if option EnableHandleChild
// was enabled in act.SupervisorSpec
func (sup *WhereisSup) HandleChildTerminate(name gen.Atom, pid gen.PID, reason error) error {
	return nil
}

// HandleMessage invoked if Supervisor received a message sent with gen.Process.Send(...).
// Non-nil value of the returning error will cause termination of this process.
// To stop this process normally, return gen.TerminateReasonNormal or
// gen.TerminateReasonShutdown. Any other - for abnormal termination.
func (sup *WhereisSup) HandleMessage(from gen.PID, message any) error {
	sup.Log().Info("supervisor got message from %s", from)
	return nil
}

// HandleCall invoked if Supervisor got a synchronous request made with gen.Process.Call(...).
// Return nil as a result to handle this request asynchronously and
// to provide the result later using the gen.Process.SendResponse(...) method.
func (sup *WhereisSup) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	sup.Log().Info("supervisor got request from %s with reference %s", from, ref)
	return gen.Atom("pong"), nil
}

// Terminate invoked on a termination supervisor process
func (sup *WhereisSup) Terminate(reason error) {
	sup.Log().Info("supervisor terminated with reason: %s", reason)
}

// HandleInspect invoked on the request made with gen.Process.Inspect(...)
func (sup *WhereisSup) HandleInspect(from gen.PID, item ...string) map[string]string {
	sup.Log().Info("supervisor got inspect request from %s", from)
	return nil
}
