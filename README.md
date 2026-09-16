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

Implement the shared session and route operations in `system.ActorRoutePersistence`:

```go
type ActorRoutePersistence interface {
    OpenSession(context.Context, gen.Atom, time.Duration) (SessionLease, error)
    RenewSession(context.Context, SessionID, time.Duration) (SessionLease, error)
    CloseSession(context.Context, SessionID) error
    ReadRoute(context.Context, gen.Atom) (RouteSnapshot, bool, error)
    AcquireRoute(context.Context, SessionID, gen.Atom, gen.PID, *RouteOwner, time.Duration) (AcquireRouteResult, error)
    ReleaseRoute(context.Context, SessionID, gen.Atom, gen.PID) error
}
```

Each node instance opens a fresh `SessionID`. A route stores its key and `RouteOwner{SessionID, PID}`, including the full PID creation value. Sessions and routes expire independently. Operations must be concurrent-safe and honor their contexts. The backend must reclaim expired records even when they are never read again.

- `OpenSession` allocates a new identity. `RenewSession` extends only a live existing session. `CloseSession` is idempotent and terminal, including against concurrent renewal and acquisition.
- `ReadRoute` returns a consistent route/session snapshot, with the route's remaining `ValidFor` and `SessionValid`.
- `AcquireRoute` atomically verifies the requesting session and the expected owner. A nil expected owner requires an absent or expired route. A supplied owner requires an exact match. Results distinguish acquisition, occupation, and a failed comparison. Repeating acquisition for the same session and full PID succeeds and resets the route TTL.
- `ReleaseRoute` removes only the exact session and full PID; absence or replacement is success.
- `ValidFor` describes remaining validity when the operation executes. Local deadlines use the request start time plus `ValidFor`, less a safety margin.
- An acquisition error has an uncertain outcome unless it wraps `ErrRouteNotApplied`, which certifies that the operation did not write. Backends must preserve that distinction and avoid implicitly replaying uncertain acquisitions. Successful operations must remain durable within the backend's documented failure model.

Lookup and takeover use the same validity rule: an unexpired route, a live owner session, and an online owner node. Remote membership is checked directly through `registrar.Nodes()`; self uses `node.IsAlive()`. Registrar errors propagate and prevent takeover. Node membership is deliberately part of this shared rule: temporary overlap during discovery lag is accepted to allow prompt recovery when a node disappears.

An invalid route can be replaced by comparing its exact observed owner. This replacement affects that route; the owner closes its own session. Graceful exit releases its route. An absent node can be replaced before its session expires. Name-only membership cannot distinguish a same-name restart; that case still depends on the prior session closing or expiring.

The new contract replaces the previous per-actor lease interface. Custom persistence implementations must implement all six operations. `system.NewMemoryActorRoutePersistence()` supplies an in-process backend for examples and tests; call its `Close` when finished.

## Lifecycle and timing

Acquisition completes before business Init. The route remains associated with the instance through Init and business Terminate; after cleanup, an exact-owner release is queued. Failed releases remain queued for retry. Admission returns `ErrActorRouterBusy` when pending cleanup reaches `ReleaseQueueSize`, or the route operation queue is full.

One heartbeat renews the node session, independently of the bounded route operation workers. A shared timing wheel schedules route renewals and local expiration; renewals run on the existing bounded route operation workers. Defaults are:

| Option | Default |
| --- | --- |
| `SessionTTL` | 30 seconds |
| `SessionRenewInterval` | 10 seconds, with bounded jitter |
| `OperationTimeout` | 3 seconds |
| `LeaseSafetyMargin` | 3 seconds |
| `RouteTTL` | 2 hours |
| `RouteRenewInterval` | 100 minutes, with ±10% per-actor jitter |
| `RouteChangeWorkers` | 16 |
| `RouteChangeQueueSize` | 65,536 |
| `ReleaseQueueSize` | 65,536 pending releases before admission backpressure |

Renewal traffic is approximately `nodes / SessionRenewInterval + actors / RouteRenewInterval`. Each route renewal calls `AcquireRoute` with its own exact `(SessionID, PID)` as the expected owner. Success extends both the stored TTL and local deadline. An owner mismatch stops only the displaced actor. Errors retain the last confirmed deadline and retry; a late response cannot revive an expired actor. Renewal never adopts another owner's identity to reclaim its route. The first and subsequent renewals use independent per-actor jitter seeded by the key and full PID. Storage failures and a full worker queue retry with exponential backoff from 1 second to a 60-second base, capped by the configured interval, plus ±10% jitter. Waiting for workers or retry never extends the last confirmed deadline. The worker count limits concurrent operations; it is not a storage writes-per-second limit. Route expiry stops that actor and lets daemon recovery start a fresh instance where configured.

Configure both values through the existing `StartSimpleNode` options:

```go
node, err := app.StartSimpleNode(app.SimpleNodeOptions{
    Registrar: registrar,
    ActorRoutePersistence: persistence,
    ActorRouterOptions: system.ActorRouterOptions{
        RouteTTL:           2 * time.Hour,
        RouteRenewInterval: 100 * time.Minute,
    },
})
```

`RouteTTL` must exceed `RouteRenewInterval + RouteRenewInterval/10 + OperationTimeout + LeaseSafetyMargin`. Zero uses the default for each option; when shortening the TTL, configure the renewal interval as well.

A confirmed lost session or the local session deadline moves the router to Lost. It stops admissions and renewal, kills its exact managed PIDs, and independently closes its own session with an operation timeout. Failed initial acquisitions affect only that actor and never enter business Init. Once an in-flight acquisition returns, successful or uncertain writes are released using the exact owner; failed releases stay queued for retry. Temporary renewal errors retain the last confirmed deadline. A late response cannot reactivate a Lost router. Routed admission resumes with a fresh node/router instance. Business dispatch checks both local deadlines before processing another mailbox item; a route deadline stops only that actor.

Concurrent old and new actors after a membership-based takeover are an accepted availability tradeoff. With responsive storage/workers, the old actor detects replacement at its next renewal (normally within 90–110 minutes of the last successful acquisition/renewal by default); if renewal fails or stalls, its existing local deadline stops further dispatch (up to the remaining 2-hour route TTL, less the safety margin). Already-running Init, callbacks, and application goroutines must finish cooperatively. Routing does not provide exactly-once business execution; mailbox work, direct-PID messages, and self-timers can still execute on the old actor during the overlap.

Shutdown enters Draining before stopping the node, then `router.Close()` stops local route management and closes the shared session. Session closure invalidates all associated routes; their records expire through their own TTLs. Session closure is bounded by `OperationTimeout`; a failure is logged and the session expires naturally. Custom bootstrap should call `router.Drain()`, stop the node, then call `router.Close()`. Business cleanup belongs to the node/application shutdown flow: `node.Wait()` can finish before Terminate callbacks return, and closing the session permits takeover while those callbacks are still running.

`ActorRoutes().Stats()` reports tracked lifecycle records, route queue depth, pending releases, session renewal failures, session losses, and release failures.

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

Register launchers before starting the node. The leader rotates through one page per launcher. `DaemonOptions.ScanBatchSize` defaults to 32; `MaxInFlight` defaults to 64 per initiating daemon and covers lookup, waiting for a target, local and remote launches, and retained per-key retries. A fixed pool of sixteen I/O workers handles scanner page reads, lookup, exact-owner cleanup, and remote delivery. The daemon dispatches one page task at a time and resumes scanning when the worker returns its result. Scanner implementations must bound their I/O.

The initiator retains launch arguments and announces each task to its consistent-hash target. Targets acknowledge task identities and coalesce duplicate keys. Each of eight launch workers requests a task when idle; its target daemon reserves the worker before asking the initiator for the launch arguments. On completion, the worker immediately becomes available to request another task. The initiator releases the completed task's capacity and resumes its scanner. A full initiator keeps remaining items in the current scanner page until capacity is released. `ScanBatchSize` bounds work per callback; batches continue through messages as capacity becomes available.

Waiting for a worker has no launch deadline. Unconfirmed announcements are resent after `LaunchTimeout`; the launch deadline starts when the initiator answers a target's pull request. A target also expires reservations whose launch arguments never arrive. If Init times out but keeps running, its worker remains occupied until Init returns or the worker stops. Business Init should bound its external calls. Failed scanner-backed tasks release capacity and retry through a coalesced scan after `RetryMaxDelay` (default 60 seconds). Exact-PID cleanup and tasks without scanners retain per-key retries with exponential backoff and jitter. Successful starts release capacity immediately; `RunningGrace` no longer delays completion. Init returning `gen.TerminateReasonNormal` ends recovery for that key until a later scanner includes it again.

After membership publishes the updated topology, it notifies daemon recovery. A launched daemon's termination wrapper notifies recovery after cleanup; recovery conditionally releases that exact exited PID and retries cleanup failures before ensuring a replacement. Overflow notifications coalesce into a full recovery request. Full recovery defaults to 15 minutes as a repair pass. Node shutdown relies on membership recovery and configured scanners.

For application-driven spawning, `system.NewSpawner(process, router, "worker").SpawnRegister(...)` installs the same route and exit-recovery lifecycle when using custom bootstrap with an explicit router.

## Deployment and upgrades

Deploy the daemon pull-protocol change to all cluster nodes together.

Existing actors stay on their current nodes after expansion. Rolling upgrades recover actors as old instances exit; business state should be restored by the new instance's Init. Keep launcher names, recovery arguments, and message types compatible across coexisting versions. Placement uses node membership rather than launcher-version capabilities.

Pod readiness does not remove a node from the registrar. The application must stop its Ergo node during shutdown. Give the container enough termination time for business cleanup and route release.

## Cron and migration

Supply `CronSource` and `CronSchedulerOptions` through `SimpleNodeOptions`. Use a shared production `cron.KVStore` for scheduler state. See [v2/docs/ARCHITECTURE.md](v2/docs/ARCHITECTURE.md).

v1 imports remain available at the root module. In v2, durable route persistence and decorators replace whereis registration; `ActorRoutes().Locate` resolves actors and `Topology()` exposes placement. New callers must supply their registrar explicitly.

## License

MIT. See [LICENSE](LICENSE).
