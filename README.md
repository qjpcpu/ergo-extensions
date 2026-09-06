# Ergo Extensions v2

Ergo Extensions v2 provides leased actor routing, registrar-backed membership, daemon recovery, and distributed cron scheduling for `ergo.services/ergo v1.999.320`. It requires Go 1.24 or later. The root module remains v1; see [README-v1.md](README-v1.md).

```sh
go get github.com/qjpcpu/ergo-extensions/v2@latest
```

## Start a node

Supply an Ergo registrar and a shared actor-route persistence implementation:

```go
import (
    "context"
    "ergo.services/ergo/gen"
    "github.com/qjpcpu/ergo-extensions/v2/app"
    "github.com/qjpcpu/ergo-extensions/v2/system"
    redisregistrar "github.com/qjpcpu/registrar/redis"
)

registrar, err := redisregistrar.Create(redisregistrar.Options{
    Endpoints: []string{"redis:6379"},
    Cluster: "orders",
})
if err != nil { return err }

node, err := app.StartSimpleNode(app.SimpleNodeOptions{
    NodeName: "orders-1@orders-1",
    AdvertiseHost: "orders-1",
    Registrar: registrar,
    ActorRoutePersistence: routeStore,
})
if err != nil { return err }
defer node.Stop()

routes := node.ActorRoutes()
_, err = node.Spawn(func() gen.ProcessBehavior {
    return routes.WithActorRoute("orders/42", &orderWorker{})
}, gen.ProcessOptions{})
if err != nil { return err }

pid, found, err := routes.Locate(context.Background(), "orders/42")
if err != nil { return err }
if !found { return gen.ErrProcessUnknown }
return node.Send(pid, ProcessOrder{OrderID: "A-100"})
```

The example uses application-defined `routeStore`, `orderWorker`, and `ProcessOrder`. A complete executable example is in [v2/examples/basic](v2/examples/basic/main.go).

`StartSimpleNode` creates, binds, and closes one router per node. `ActorRoutes()` provides lookup, behavior decorators, and lease statistics; `Topology()` provides the immutable node set, version, and consistent-hash placement.

Use `WithActorRoute`, `WithSupervisorRoute`, or `WithPoolRoute` inside a factory to give each spawn its own wrapper and behavior.

## Persistence contract

Implement all five operations in the shared backend:

```go
type ActorRoutePersistence interface {
    Acquire(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error)
    Replace(ctx context.Context, key gen.Atom, old, pid gen.PID, ttl time.Duration) (bool, error)
    Renew(ctx context.Context, key gen.Atom, pid gen.PID, ttl time.Duration) (bool, error)
    Release(ctx context.Context, key gen.Atom, pid gen.PID) error
    Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error)
}
```

Operations must be concurrent-safe and honor context cancellation and deadlines. Store the full PID, including creation, in a shared namespace.

- `Acquire` succeeds for an absent/expired lease or the same owner. Another live lease returns `false, nil`.
- `Replace` atomically compares the existing full PID with `old` and replaces it with `pid` and a fresh TTL. An absent key or a different owner returns `false, nil`. Implement this as one conditional storage operation, not separate release and acquire calls.
- `Renew` extends only a live lease belonging to the exact PID.
- `Release` deletes only the exact PID's lease and is idempotent.
- `Lookup` treats absent and expired leases as not found.

When Acquire conflicts, the router can replace an exited local PID after its business cleanup completes. For a remote owner, the router queries the registrar's current `Nodes()` result. Absence means offline and permits replacement before TTL expiry. Registrar query errors are returned to the caller; the AddressBook cache does not authorize takeover.

Existing persistence implementations must add `Replace` before adopting this version.

## Lifecycle and lease timing

The wrapper acquires and starts renewing its route before business initialization. A conflict therefore fails before business Init executes. During initialization, messages can reach the Ergo mailbox; business processing starts after Init succeeds. Init failures run business termination cleanup and release the lease. A spawn timeout does not cancel a running user Init callback.

On termination, renewal continues while business cleanup runs. The wrapper then queues the conditional release. Failed or dropped releases expire through TTL, and offline nodes can be taken over earlier through the registrar rule above.

Each node uses one sharded timing wheel and a fixed worker pool. Renewals are individual storage operations. Default timing remains TTL 30 seconds, renewal interval 10 seconds, and operation timeout 3 seconds. Larger deployments can use longer TTLs together with longer renewal intervals, sized from measured latency and actor counts. Steady renewal demand is approximately `routed actors / renewal interval` per node. Increasing TTL alone does not reduce that demand.

`ActorRoutes().Stats()` exposes tracked leases, queue depths, maximum observed renewal scheduling delay, renewal/release failures, lease losses, and dropped releases. Release work gets priority in bounded bursts so renewals can progress.

When Renew reports that the PID no longer owns its lease, the router kills that exact actor incarnation and runs its existing termination lifecycle. Daemons then recover through their exit hook. Temporary storage errors retry without killing the actor. Termination cannot undo external work already performed.

Shutdown allows business cleanup callbacks one operation-timeout budget, waits for current worker operations, and drains queued releases with another operation-timeout budget. Persistence methods must honor their contexts. Any remaining cleanup falls back to expiration. Custom bootstrap must stop its node before calling `router.Close()`.

## Sending and calling

```go
err := node.ForwardSend("orders/42", ProcessOrder{OrderID: "A-101"}, app.ForwardImportant())
reply, err := node.ForwardCall("orders/42", GetOrder{OrderID: "A-101"}, app.ForwardTimeout(5))
```

Forwarding resolves full PIDs through persistence. `ForwardNode(name)` addresses a registered process on a specific node. PID variants skip lookup.

`ForwardTimeout` bounds queue wait, lookup, and delivery/call from the caller's perspective; the default is Ergo's 5-second request timeout. `ForwardContext(ctx)` supplies cancellation or an earlier deadline. Expired queued requests are discarded. Once a message has been sent or Init has begun, caller timeout does not undo the operation. Ergo's internal call timeouts have integer-second granularity, so a worker can finish later than its caller's deadline.

`ForwardSpawn` uses the same default total timeout; decorate its factory if the process needs a route. `WaitPID` waits for process exit or node shutdown. Forwarding defaults to 128 workers, shared by sends and calls.

Native `actor.Send("name", message)` still addresses a local process name. Use a resolved PID or `app.NewCaller(process, node.ActorRoutes())` for global routing from a business actor. `Caller.Send` uses the process delivery settings; ordinary remote sends do not confirm mailbox receipt. Use `Caller.SendImportant` for explicit mailbox delivery confirmation. Important delivery confirms mailbox routing, not completion of business processing.

## Daemons

```go
err := system.RegisterLauncher("worker", system.Launcher{
    Factory: func() gen.ProcessBehavior { return &worker{} },
    RecoveryScanner: system.SingletonDaemon("worker/A", []any{"configuration"}),
})
```

Register launchers before starting the node. The leader rotates through one page per launcher and sends launches directly to the consistent-hash target. `DaemonOptions.ScanBatchSize` defaults to 32; `MaxInFlight` defaults to 64 per initiating daemon and covers local and remote recovery, including retained per-key retries. Failed scanner-backed tasks release capacity and retry through a coalesced scan after `RetryMaxDelay` (default 60 seconds); pending exact-PID cleanup retains its retry state. A fixed pool of eight I/O workers handles lookup, exact-owner cleanup, and remote delivery outside the daemon callback. At most one scanner fetch runs at a time; scanner implementations must bound their I/O.

Each target admits up to eight outstanding launches and coalesces duplicate keys. If Init times out but keeps running, its worker remains occupied until Init returns or the launch pool stops. Business Init should bound its external calls. Retries use exponential backoff and jitter and retain their admission slot. Successful starts release capacity immediately; `RunningGrace` is retained for source compatibility and no longer delays completion. Init returning `gen.TerminateReasonNormal` ends recovery for that key until a later scanner includes it again.

After membership publishes the updated topology, it notifies daemon recovery. A launched daemon's termination wrapper notifies recovery after cleanup; recovery conditionally releases that exact exited PID and retries cleanup failures before ensuring a replacement. Overflow notifications coalesce into a full recovery request. Full recovery defaults to 15 minutes as a repair pass. Node shutdown relies on membership recovery and configured scanners.

For application-driven spawning, `system.NewSpawner(process, router, "worker").SpawnRegister(...)` installs the same route and exit-recovery lifecycle when using custom bootstrap with an explicit router.

## Deployment and upgrades

Existing actors stay on their current nodes after expansion. Rolling upgrades recover actors as old instances exit; business state should be restored by the new instance's Init. Keep launcher names, recovery arguments, and message types compatible across coexisting versions. Placement uses node membership rather than launcher-version capabilities.

Pod readiness does not remove a node from the registrar. The application must stop its Ergo node during shutdown. Give the container enough termination time for business cleanup and route release.

## Cron and migration

Supply `CronSource` and `CronSchedulerOptions` through `SimpleNodeOptions`. Use a shared production `cron.KVStore` for scheduler state. See [v2/docs/ARCHITECTURE.md](v2/docs/ARCHITECTURE.md).

v1 imports remain available at the root module. In v2, durable route persistence and decorators replace whereis registration; `ActorRoutes().Locate` resolves actors and `Topology()` exposes placement. New callers must supply their registrar explicitly.

## License

MIT. See [LICENSE](LICENSE).

Recovery scans pace batches with `DaemonOptions.ScanBatchInterval` (default 50 ms). With the default batch size of 32, each leader admits at most about 640 scanned items per second, plus the initial batch; completion messages do not bypass this interval. This trades recovery speed for lower persistence load. Lease renewals remain proportional to active actors divided by the renewal interval; size that interval and Redis capacity together.
