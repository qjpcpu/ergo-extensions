# Ergo Extensions (system)

This repository provides a small set of building blocks to add distributed process discovery and daemon orchestration to an Ergo-based cluster. The `system` package contains the main runtime processes, and the `app` package provides helpers to start a node with them wired in.

For a shorter architecture reference, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Requirements

- `ergo.services/ergo v1.999.320`
- A network registrar implementation (e.g. ZooKeeper via `github.com/qjpcpu/registrar/zk`).

`app.StartSimpleNode` uses ZooKeeper when `SimpleNodeOptions.Endpoints` is set, a custom registrar when `SimpleNodeOptions.Registrar` is provided, and the built-in in-memory registrar otherwise.

## Install

```bash
go get github.com/qjpcpu/ergo-extensions@latest
```

Import the system package:

```go
import "github.com/qjpcpu/ergo-extensions/system"
```

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

Or start a node with everything wired in (uses ZooKeeper registrar when `Endpoints` is set; otherwise uses `Registrar` when provided; otherwise falls back to an in-memory single-node registrar):

```go
provider := cron.NewStaticSource(128,
    cron.JobSpec{
        ID:             "job.ping",
        ShardKey:       "job.ping",
        Schedule:       "* * * * *",
        Location:       cron.LocationUTC,
        TriggerProcess: gen.Atom("ping"),
    },
)
store := cron.NewMemoryKVStore()
source := cron.NewManagedSource(provider, store)

n, err := app.StartSimpleNode(app.SimpleNodeOptions{
    NodeName: "node-1",
    Options: zk.Options{
        Endpoints: []string{"127.0.0.1:2181"},
    },
    CronSource: source,
    CronSchedulerOptions: cron.SchedulerOptions{
        ShardCount: 128,
    },
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

4) Locate a process by its registered name:

Using `app.Node` helper:
```go
node := n.LocateProcess(gen.Atom("worker.A"))
```

Or via `AddressBook` for distributed lookup:
```go
respAny, err := self.Call(gen.ProcessID{Name: system.WhereIsProcess}, system.MessageGetAddressBook{})
if err != nil { /* handle */ }
book := respAny.(system.MessageAddressBook).Book
node, err := book.QueryBy(self, system.QueryOption{Timeout: 5}).Locate(gen.Atom("worker.A"))
```

5) Access the shared `AddressBook` if you need more control (e.g., local pick):

```go
respAny, err := self.Call(gen.ProcessID{Name: system.WhereIsProcess}, system.MessageGetAddressBook{})
if err != nil { /* handle */ }
book := respAny.(system.MessageAddressBook).Book
picked := book.PickNode(gen.Atom("worker.A")) // pick based on consistent hashing
```

## Selected Entry Points

- Supervisor: `system.ApplicationMemberSpec`, `system.FactorySystemSup`
- WhereIs: `system.WhereIsProcess`, `MessageLocate`, `MessageGetAddressBook`
- Address book: `IAddressBook`, `IAddressBookQuery`, `app.Node.LocateProcess`
- Daemon orchestration: `system.RegisterLauncher`, `system.NewSpawner`, `system.SingletonDaemon`
- Cron scheduling: `cron.JobSpec`, `cron.JobProvider`, `cron.KVStore`, `cron.NewManagedSource`

## Limitations

- `MessageLocate` returns a node, not a PID; ask the address book or the node itself for details.
- Recovery scanners are user-supplied; ensure they are idempotent and resilient.
- Broadcasts are best-effort; transient network issues may delay convergence.

## License

MIT License. See `LICENSE` for details.
