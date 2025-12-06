# Ergo Extensions (system)

This repository provides a small set of building blocks to add distributed process discovery and daemon orchestration to an Ergo-based cluster. It ships a supervisor that wires together:

- `whereis` — a process that continuously inspects local processes, maintains an address book, and broadcasts snapshots/changes to the cluster.
- `daemon_monitor` — a process that (on the elected leader) recovers and launches daemon processes across nodes using consistent hashing.
- `AddressBook` — a thread-safe, eventually-consistent cache of nodes and their registered processes, with node picking via consistent hashing.

All components live under the `system` package and are designed to integrate with `ergo.services/ergo` and a registrar (Zookeeper by default).

## Features

- Distributed process discovery and naming via a shared address book
- Periodic local inspection and cluster-wide broadcast of process snapshots
- Leader-driven daemon recovery and remote spawn requests
- Consistent hashing (`xxhash` + `buraksezer/consistent`) for stable node selection
- Simple APIs to register daemon launchers and spawn named processes

## Requirements

- Go `1.24`
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

- `WhereIsSupervisor` (`whereissup`) starts two children:
  - `WhereIsProcess` (`whereis`):
    - Inspects local processes every second
    - Maintains PID→Name and Name→PID maps
    - Stores a snapshot (`ProcessInfoList`) in-memory and updates the `AddressBook`
    - Broadcasts process snapshots or deltas to other nodes
    - Answers calls: `MessageLocate{Name}` → node, `MessageGetAddressBook{}` → `AddressBook`
  - `DaemonMonitorProcess` (`daemon_monitor`):
    - Subscribes to registrar events for leader election and membership changes
    - On leader, scans registered `Launcher` recovery iterators and launches daemons to selected nodes
    - Sends remote spawn requests when target node is not local
- `AddressBook`:
  - Tracks available nodes and per-node registered processes
  - Picks a node for a process name using a consistent hashing ring (PartitionCount: 10240, ReplicationFactor: 40)

## Quick Start

1) Add the supervisor to your application members:

```go
app := gen.ApplicationSpec{
    Members: []gen.ApplicationMemberSpec{
        system.ApplicationMemberSepc(),
    },
}
// Wire this application spec into your Ergo node environment/startup as usual.
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
list := book.GetProcessList(picked)
```

## Public API (selected)

- Constants:
  - `system.WhereIsSupervisor`, `system.WhereIsProcess`, `system.DaemonMonitorProcess`
- Supervisor helpers:
  - `system.ApplicationMemberSepc() gen.ApplicationMemberSpec`
  - `system.FactoryWhereisSup() gen.ProcessFactory`
- Daemon orchestration:
  - `system.RegisterLauncher(name gen.Atom, launcher system.Launcher) error`
  - `system.NewSpawner(parent gen.Process, launcherName gen.Atom) system.Spawner`
  - `Spawner.SpawnRegister(processName gen.Atom, args ...any) (gen.PID, error)`
  - `Launcher{ Factory, Option, RecoveryScanner }`
  - `DaemonProcess{ ProcessName gen.Atom, Args []any }`
- Discovery & address book:
  - Call `whereis` with `MessageLocate{Name gen.Atom}` → `gen.Atom` (node)
  - Call `whereis` with `MessageGetAddressBook{}` → `MessageAddressBook{Book IAddressBook}`
  - `IAddressBook` provides: `Locate`, `GetProcessList`, `PickNode`, `GetAvailableNodes`

## Registrar & Events

The code expects a working registrar from the Ergo network. With Zookeeper (`ergo.services/registrar/zk`), the following events are handled:

- Leadership changes: `EventNodeSwitchedToLeader`, `EventNodeSwitchedToFollower`
- Membership changes: `EventNodeJoined`, `EventNodeLeft`

`whereis` will broadcast on joins; `daemon_monitor` will re-plan launches on left/failover and trigger recovery when this node becomes leader.

## Design Notes

- Consistency: the `AddressBook` is eventually consistent; broadcasts retry on failures.
- Hashing: consistent hashing ring uses `xxhash` and `buraksezer/consistent` to spread process names across nodes.
- Scheduling: `whereis` inspects every second; `daemon_monitor` schedules recovery with small delays to absorb churn.
- Safety: remote spawns are issued via `SendImportant` to target nodes.

## Limitations

- `MessageLocate` returns a node, not a PID; ask the address book or the node itself for details.
- Recovery scanners are user-supplied; ensure they are idempotent and resilient.
- Broadcasts are best-effort; transient network issues may delay convergence.

## Development

- Code lives in `system/`
- No external binaries; integrate directly with your Ergo application
- Linting/formatting follow your project’s standards

## License

MIT License. See `LICENSE` for details.

