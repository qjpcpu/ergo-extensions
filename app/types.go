package app

import (
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
	cronpkg "github.com/qjpcpu/ergo-extensions/system/cron"
	"github.com/qjpcpu/registrar/zk"
)

// Node is the minimal interface returned by StartSimpleNode.
//
// It wraps an Ergo gen.Node and provides helper methods to locate a named
// process via the shared address book, and to forward sends/calls to the node
// currently hosting that process.
type Node interface {
	gen.Node

	// LocateProcess returns the node that currently owns the named process.
	LocateProcess(process gen.Atom) gen.Atom

	// ForwardCall calls the named process on its current owner node.
	ForwardCall(to string, msg any, opts ...ForwardOpts) (any, error)

	// ForwardSend sends a message to the named process on its current owner node.
	ForwardSend(to string, msg any, opts ...ForwardOpts) error

	// ForwardSpawn starts a process through the route worker and returns after spawn completes.
	ForwardSpawn(name string, fac gen.ProcessFactory, args ...any) error

	// ForwardSpawnAndWait starts a process through the route worker and waits until it exits.
	ForwardSpawnAndWait(name string, fac gen.ProcessFactory, args ...any) error

	// WaitPID waits until the given process exits.
	WaitPID(pid gen.PID) error

	// AddressBook returns the node's shared process address book.
	AddressBook() system.IAddressBook
}

type CronJob = cronpkg.JobSpec
type CronSource = cronpkg.Source
type CronSchedulerOptions = cronpkg.SchedulerOptions

type SimpleNodeOptions struct {
	zk.Options        // ZooKeeper registrar options.
	NodeName   string // Node name.
	// Optional
	Port                     uint16                      // Listen port, default to 11144
	AcceptorNetFamily        string                      // Acceptor network family, default to tcp. Supported values: tcp, tcp4, tcp6.
	AdvertiseHost            string                      // Publicly accessible hostname or IP address of the node.
	AdvertisePort            uint16                      // Publicly accessible port of the node.
	Cookie                   string                      // Cluster cookie (must match across nodes).
	MoreApps                 []gen.ApplicationBehavior   // Extra applications to start on the node.
	MemberSpecs              []gen.ApplicationMemberSpec // Additional application members to start.
	NodeForwardWorker        int64                       // Worker count for forwarding calls/sends. Defaults to 128 to keep hot forwarding paths from stalling on slow lookups or remote calls.
	LogLevel                 gen.LogLevel                // Node log level.
	DefaultLogOptions        gen.DefaultLoggerOptions    // Default logger configuration.
	CronSource               CronSource                  // Managed cron source composed of a job provider and a state KV store.
	CronSchedulerOptions     CronSchedulerOptions        // Cron scheduler options.
	WhereIsOptions           system.WhereIsOptions       // Whereis discovery, query, and topology sync options.
	DaemonOptions            system.DaemonOptions        // Daemon recovery and retry timing options.
	PlacementMonitorInterval time.Duration               // Placement monitor interval for duplicate named process notifications.
	Registrar                gen.Registrar               // Custom registrar implementation (used if Endpoints is empty).
}

type ForwardOpts func(*forwardopts)
type forwardopts struct {
	Timeout int
	Node    gen.Atom
}

func ForwardTimeout(t int) ForwardOpts {
	return func(o *forwardopts) { o.Timeout = t }
}

func ForwardNode(t gen.Atom) ForwardOpts {
	return func(o *forwardopts) { o.Node = t }
}
