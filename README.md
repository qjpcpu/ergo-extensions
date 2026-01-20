# Ergo Extensions (system)

This repository provides a small set of building blocks to add distributed process discovery and daemon orchestration to an Ergo-based cluster. It ships a supervisor that wires together:

- `extensions_whereis` — a process that periodically inspects local processes, maintains an address book, and syncs snapshots/changes with the cluster.
- `extensions_daemon` — a process that (on the elected leader) recovers and launches daemon processes across nodes using consistent hashing.
- `extensions_cron` — a cron-like scheduler that triggers messages on a node or across the cluster.
- `AddressBook` — a thread-safe, eventually-consistent cache of nodes and their registered processes, with node picking via consistent hashing.

Core components live under the `system` package and are designed to integrate with `ergo.services/ergo` and a registrar (Zookeeper via `ergo.services/registrar/zk`, or the built-in in-memory registrar used by `app.StartSimpleNode` when no registrar is provided). The `app` package provides a small helper to start a node with these components wired in.

## Features

- Distributed process discovery and naming via a shared address book
- Periodic local inspection and cluster-wide sync of process snapshots
- Leader-driven daemon recovery and remote spawn requests
- Node- or cluster-scoped cron jobs (`extensions_cron`) with stable placement
- Consistent hashing (`xxhash` + `buraksezer/consistent`) for stable node selection
- Simple APIs to register daemon launchers and spawn named processes

## Requirements

- `ergo.services/ergo v1.999.310`
- A network registrar; the default code expects Zookeeper via `ergo.services/registrar/zk`

The module’s `go.mod` includes a `replace` directive to use `github.com/qjpcpu/registrar/zk` for the registrar.

## Install

```bash
go get github.com/qjpcpu/ergo-extensions@latest
```

Import the system package:

```go
import "github.com/qjpcpu/ergo-extensions/system"
```

## Architecture

- `Supervisor` (`extensions_sup`) starts three children:
  - `WhereIsProcess` (`extensions_whereis`):
    - Inspects local processes periodically (default: every 3 seconds)
    - Maintains PID→Name and Name→PID maps
    - Stores a snapshot (`ProcessInfoList`) in-memory and updates the `AddressBook`
    - Syncs process snapshots or deltas with other nodes (request/reply with forwarding)
    - Answers calls: `MessageLocate{Name}` → node, `MessageGetAddressBook{}` → `MessageAddressBook{Book IAddressBook}`
  - `DaemonMonitorProcess` (`extensions_daemon`):
    - Subscribes to registrar events for leader election and membership changes
    - On leader, scans registered `Launcher` recovery iterators and launches daemons to selected nodes
    - Sends remote spawn requests when target node is not local
  - `CronJobProcess` (`extensions_cron`):
    - Triggers cron jobs either on a single node or cluster-wide
  - `AddressBook`:
    - Tracks available nodes and per-node registered processes
    - Picks a node for a process name using a consistent hashing ring (PartitionCount: 10240, ReplicationFactor: 40)
- `app.StartSimpleNode`:
  - Starts an Ergo node and loads the `system.Supervisor` with a shared `IAddressBook`
  - Provides helper methods for locating named processes and forwarding sends/calls

## Quick Start

1) Add the supervisor to your application members:

```go
spec := gen.ApplicationSpec{
    Members: []gen.ApplicationMemberSpec{
        system.ApplicationMemberSpec(system.ApplicationMemberSpecOptions{}),
    },
}
// Wire this application spec into your Ergo node environment/startup as usual.
```

Or start a node with everything wired in (uses Zookeeper registrar when `Endpoints` is set, otherwise falls back to an in-memory single-node registrar):

```go
n, err := app.StartSimpleNode(app.SimpleNodeOptions{
    NodeName: "node-1",
    Options: zk.Options{
        Endpoints: []string{"127.0.0.1:2181"},
    },
    CronJobs: []app.CronJob{
        {
            Name:          gen.Atom("job.ping"),
            Spec:          "* * * * *",
            Location:      system.CronJobLocationUTC,
            TriggerProcess: gen.Atom("ping"),
            Scope:         system.CronJobScopeCluster,
        },
    },
    NodeForwardWorker: 8,
    SyncProcessInterval: time.Second * 3,
})
_ = n
_ = err
```

2) Register a launcher for your daemon processes (during init or startup):

```go
var WorkerLauncher = system.Launcher{
    Factory: func() gen.ProcessBehavior { return &Worker{} },
    Option:  gen.ProcessOptions{EnableRemote: true},
    RecoveryScanner: func() system.DaemonIterator {
        // Provide desired daemons to recover when the cluster leader starts/restarts.
        jobs := []system.DaemonProcess{
            {ProcessName: gen.Atom("worker.A")},
            {ProcessName: gen.Atom("worker.B")},
        }
        i := 0
        return func() ([]system.DaemonProcess, bool, error) {
            if i == 0 {
                i++
                return jobs, false, nil
            }
            return nil, false, nil
        }
    },
}

func init() {
    _ = system.RegisterLauncher(gen.Atom("worker"), WorkerLauncher)
}
```

3) Spawn a named daemon using a `Spawner`:

```go
sp := system.NewSpawner(self, gen.Atom("worker"))
pid, err := sp.SpawnRegister(gen.Atom("worker.A"), /* args... */)
```

4) Locate a process by its registered name via `whereis`:

```go
nodeAny, err := self.Call(gen.ProcessID{Name: system.WhereIsProcess}, system.MessageLocate{Name: gen.Atom("worker.A")})
if err != nil { /* handle */ }
node := nodeAny.(gen.Atom) // empty atom if unknown
```

5) Access the shared `AddressBook` if you need more control:

```go
respAny, err := self.Call(gen.ProcessID{Name: system.WhereIsProcess}, system.MessageGetAddressBook{})
if err != nil { /* handle */ }
book := respAny.(system.MessageAddressBook).Book
picked := book.PickNode(gen.Atom("worker.A"))
if b, ok := book.(*system.AddressBook); ok {
    list, _ := b.GetProcessList(picked)
    _ = list
}
```

## Public API (selected)

- Constants:
  - `system.Supervisor`, `system.WhereIsProcess`, `system.DaemonMonitorProcess`, `system.CronJobProcess`
- Supervisor helpers:
  - `system.ApplicationMemberSpec(opts system.ApplicationMemberSpecOptions) gen.ApplicationMemberSpec`
  - `system.FactorySystemSup(opts system.ApplicationMemberSpecOptions) gen.ProcessFactory`
- Daemon orchestration:
  - `system.RegisterLauncher(name gen.Atom, launcher system.Launcher) error`
  - `system.NewSpawner(parent gen.Process, launcher gen.Atom) system.Spawner`
  - `Spawner.SpawnRegister(processName gen.Atom, args ...any) (gen.PID, error)`
  - `Launcher{ Factory, Option, RecoveryScanner }`
  - `DaemonProcess{ ProcessName gen.Atom, Args []any }`
- Discovery & address book:
  - Call `extensions_whereis` with `MessageLocate{Name gen.Atom}` → `gen.Atom` (node)
  - Call `extensions_whereis` with `MessageGetAddressBook{}` → `MessageAddressBook{Book IAddressBook}`
  - `IAddressBook` provides: `PickNode`, `GetAvailableNodes`

## Registrar & Events

The code expects a working registrar from the Ergo network. With Zookeeper (`ergo.services/registrar/zk`), the following events are handled:

- Leadership changes: `EventNodeSwitchedToLeader`, `EventNodeSwitchedToFollower`
- Membership changes: `EventNodeJoined`, `EventNodeLeft`

`extensions_cron` will rebalance on joins/left; `extensions_daemon` will re-plan launches on left/failover and trigger recovery when this node becomes leader. `extensions_whereis` syncs periodically and does not depend on registrar events.

## Design Notes

- Consistency: the `AddressBook` is eventually consistent; broadcasts retry on failures.
- Locate semantics: if multiple nodes report the same process name, `LocateLocal` picks the oldest instance; ties are deterministic.
- Hashing: consistent hashing ring uses `xxhash` and `buraksezer/consistent` to spread process names across nodes.
- Scheduling: `whereis` inspects periodically (default: 3s); `extensions_daemon` schedules recovery with small delays to absorb churn.
- Safety: remote spawns are issued via `SendImportant` to target nodes.

## Limitations

- `MessageLocate` returns a node, not a PID; ask the address book or the node itself for details.
- Recovery scanners are user-supplied; ensure they are idempotent and resilient.
- Broadcasts are best-effort; transient network issues may delay convergence.

## Development

- Code lives in `system/` and `app/`
- No external binaries; integrate directly with your Ergo application
- Linting/formatting follow your project’s standards

## License

MIT License. See `LICENSE` for details.

