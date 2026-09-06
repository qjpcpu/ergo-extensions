package app

import (
	"context"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
	cronpkg "github.com/qjpcpu/ergo-extensions/v2/system/cron"
)

// ActorRoutes exposes actor lookup and route decoration without router
// lifecycle operations. Node binding and shutdown remain owned by StartSimpleNode.
type ActorRoutes interface {
	Stats() system.ActorRouterStats
	Locate(ctx context.Context, key gen.Atom) (gen.PID, bool, error)
	WithActorRoute(key gen.Atom, actor system.IActor) system.IActor
	WithSupervisorRoute(key gen.Atom, supervisor system.ISupervisor) system.ISupervisor
	WithPoolRoute(key gen.Atom, pool system.IPool) system.IPool
}

// Topology exposes the immutable cluster node view and placement operations.
type Topology interface {
	PickNode(key gen.Atom) gen.Atom
	PickCoordinatorNode(key gen.Atom) gen.Atom
	GetAvailableNodes() *system.NodeList
	NodesVersion() int64
}

// Node is the minimal interface returned by StartSimpleNode.
//
// It wraps an Ergo gen.Node and provides helpers for routed communication,
// actor routes, and cluster topology.

type Node interface {
	gen.Node

	// ForwardCall calls the named process on its current owner node.
	ForwardCall(to string, msg any, opts ...ForwardOpts) (any, error)

	// ForwardCallPID calls the process identified by PID.
	ForwardCallPID(to gen.PID, msg any, opts ...ForwardOpts) (any, error)

	// ForwardSend sends a message to the named process on its current owner node.
	ForwardSend(to string, msg any, opts ...ForwardOpts) error

	// ForwardSendPID sends a message to the process identified by PID.
	ForwardSendPID(to gen.PID, msg any, opts ...ForwardOpts) error

	// ForwardSpawn starts a process through the route worker and returns its PID.
	ForwardSpawn(name string, fac gen.ProcessFactory, args ...any) (gen.PID, error)

	// WaitPID waits until the given process exits.
	WaitPID(pid gen.PID) error

	// ActorRoutes returns the node's actor lookup and route decoration view.
	ActorRoutes() ActorRoutes

	// Topology returns the node's cluster topology and placement view.
	Topology() Topology
}

type CronJob = cronpkg.JobSpec
type CronSource = cronpkg.Source
type CronSchedulerOptions = cronpkg.SchedulerOptions

type SimpleNodeOptions struct {
	NodeName              string                       // Node name.
	ActorRoutePersistence system.ActorRoutePersistence // Required durable actor route storage.
	ActorRouterOptions    system.ActorRouterOptions    // Actor route lease timing and worker limits.
	Registrar             gen.Registrar                // Required registrar supplied by the caller.
	// Optional
	Port                 uint16                                        // Listen port, default to 11144
	AcceptorNetFamily    string                                        // Acceptor network family, default to tcp. Supported values: tcp, tcp4, tcp6.
	AdvertiseHost        string                                        // Publicly accessible hostname or IP address of the node.
	AdvertisePort        uint16                                        // Publicly accessible port of the node.
	Cookie               string                                        // Cluster cookie (must match across nodes).
	MoreApps             func(ActorRoutes) []gen.ApplicationBehavior   // Extra applications built with the node-owned actor routes.
	MemberSpecs          func(ActorRoutes) []gen.ApplicationMemberSpec // Additional application members built with the node-owned actor routes.
	NodeForwardWorker    int64                                         // Worker count for forwarding calls/sends. Defaults to 128 to keep hot forwarding paths from stalling on slow lookups or remote calls.
	LogLevel             gen.LogLevel                                  // Node log level.
	DefaultLogOptions    gen.DefaultLoggerOptions                      // Default logger configuration.
	CronSource           CronSource                                    // Managed cron source composed of a job provider and a state KV store.
	CronSchedulerOptions CronSchedulerOptions                          // Cron scheduler options.
	MembershipOptions    system.MembershipOptions                      // Membership refresh, debounce, and retry timing.
	DaemonOptions        system.DaemonOptions                          // Daemon recovery and retry timing options.
}

type ForwardOpts func(*forwardopts)
type forwardopts struct {
	Context   context.Context
	Timeout   int
	Node      gen.Atom
	Important bool
}

func ForwardTimeout(t int) ForwardOpts {
	return func(o *forwardopts) { o.Timeout = t }
}

func ForwardNode(t gen.Atom) ForwardOpts {
	return func(o *forwardopts) { o.Node = t }
}

func ForwardImportant() ForwardOpts {
	return func(o *forwardopts) { o.Important = true }
}

// ForwardContext supplies cancellation or an earlier end-to-end deadline.
func ForwardContext(ctx context.Context) ForwardOpts {
	return func(o *forwardopts) { o.Context = ctx }
}
