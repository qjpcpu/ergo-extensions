package app

import (
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
	"github.com/qjpcpu/ergo-extensions/system"
)

// Node is the minimal interface returned by StartSimpleNode.
//
// It wraps an Ergo gen.Node and provides helper methods to locate a named
// process via the shared address book, and to forward sends/calls to the node
// currently hosting that process.
type Node interface {
	gen.Node
	LocateProcess(process gen.Atom) gen.Atom
	ForwardCall(to string, msg any) (any, error)
	CallLocal(to string, msg any) (any, error)
	ForwardSend(to string, msg any) error
	ForwardSpawn(fac gen.ProcessFactory, args ...any) error
	ForwardSpawnAndWait(fac gen.ProcessFactory, args ...any) error
	WaitPID(pid gen.PID) error
	AddressBook() system.IAddressBook
}

type CronJob = system.CronJob

type SimpleNodeOptions struct {
	zk.Options        // ZooKeeper registrar options.
	NodeName   string // Node name.
	// Optional
	Port                  uint16                      // Listen port, default to 11144
	Cookie                string                      // Cluster cookie (must match across nodes).
	MoreApps              []gen.ApplicationBehavior   // Extra applications to start on the node.
	MemberSpecs           []gen.ApplicationMemberSpec // Additional application members to start.
	NodeForwardWorker     int64                       // Worker count for forwarding calls/sends.
	LogLevel              gen.LogLevel                // Node log level.
	DefaultLogOptions     gen.DefaultLoggerOptions    // Default logger configuration.
	CronJobs              []CronJob                   // Cron jobs for `system.CronJobProcess`.
	DefaultRequestTimeout int                         // Default request timeout (seconds).
	SyncProcessInterval   time.Duration               // Whereis sync interval for pulling remote changes.
	Registrar             gen.Registrar               // Custom registrar implementation (used if Endpoints is empty).
}
