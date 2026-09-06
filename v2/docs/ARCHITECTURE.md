# Ergo Extensions v2 Architecture

## Responsibilities

v2 separates actor routing from cluster topology:

- `ActorRoutePersistence` is the external source of truth for leased `key -> PID` mappings.
- `ActorRouter` decorates Actor, Supervisor, and Pool behavior instances and manages route lease lifecycle.
- `AddressBook` internally combines actor lookup filtering with the local node-topology snapshot. `StartSimpleNode` exposes those capabilities separately through `ActorRoutes` and `Topology`.
- `membership` updates that topology from registrar events and periodic refreshes.
- `daemon` coordinates recovery and wraps launched actors with routes.
- `cron` assigns scheduler shards from the node topology and keeps its v1 scheduling model.

There is no whereis actor, process scan, process directory, route broadcast, placement monitor, or in-memory `key -> PID` table.

## Actor lifecycle

For a factory that returns `router.WithActorRoute(key, actor)` (or the Supervisor/Pool equivalent):

1. The wrapper binds to the node and atomically acquires the route before executing business Init.
2. On conflict, it may replace an exited local owner after business cleanup completes, or a remote owner absent from the registrar's current node set. `Replace` compares the full old PID atomically.
3. The lease is registered with the node's renewal manager. Business Init receives a process view exposing the original behavior so Ergo discovers its complete callback interfaces.
4. The manager renews leases through a sharded timing wheel, bounded queues, and fixed workers. Transient failures retry; confirmed lease loss kills that exact actor incarnation and invokes its termination lifecycle.
5. Business Terminate runs while renewal remains active. Its completion removes local instance tracking and queues a conditional release, including when business cleanup panics.
6. Daemon termination wrappers notify recovery after route cleanup has been queued. Recovery retries if the departed PID's remote lease is still present.
7. Shutdown gives outstanding business cleanup a bounded wait and drains pending releases. Node absence permits early takeover; TTL remains the cleanup fallback.

Router options require the lease TTL to exceed the renewal interval plus maximum jitter and timing-wheel rounding. Allow additional headroom for persistence latency and queueing.

Conditional persistence operations prevent an older actor incarnation from renewing or deleting a newer owner's route.

There is no companion actor, per-route goroutine, per-route mailbox, per-route timer, or periodic full-table scan. Lease metadata and timing-wheel buckets are sharded to limit lock contention. Pending flags suppress duplicate renewal jobs. Release work has priority in bounded bursts, allowing renewal work to progress. When either bounded queue is saturated, work remains bounded; a release that cannot be queued is safely resolved by lease expiry. Persistence panics are recovered per operation, the affected renewal is rescheduled, and the worker continues. `StartSimpleNode` creates the router from its persistence and options, binds it through the system supervisor, exposes it through the returned node, and closes it after stopping the node. Custom bootstrap code must create, bind, and close its `ActorRouter` itself.

The public decorators are behavior-family specific and keep their input and output aligned: `IActor -> IActor`, `ISupervisor -> ISupervisor`, and `IPool -> IPool`. Each interface includes the corresponding complete Ergo behavior interface and the promoted `gen.Process` API. The preserving process view also keeps the user's original concrete behavior visible during Ergo initialization.

## Lookup path

```text
caller
  -> ActorRoutes.Locate(ctx, key)
  -> AddressBook lookup filter
  -> ActorRoutePersistence.Lookup(ctx, key)
  -> local immutable membership snapshot check
  -> full gen.PID or not found
```

The hot path contains no actor mailbox, registrar query, cluster fan-out, or route cache. The local node is accepted directly. A remote PID is returned only when its node appears in the current AddressBook snapshot. An offline route is ignored but is not deleted, because persistence TTL and exact-owner operations remain authoritative.

## Membership

The membership actor obtains the registrar from the Ergo node after process initialization. Registrar events trigger a debounced refresh. A periodic refresh repairs missed events and reconnects after failures. Retry delay grows exponentially between configured bounds, and repeated warnings are rate-limited.

AddressBook canonicalizes the node list, updates one consistent-hash ring, publishes an immutable snapshot, and increments `NodesVersion` only when the node set changes. `PickNode` and `PickCoordinatorNode` intentionally use the same ring; v2 has no special directory-node subset.

## Daemon recovery

Every node runs a daemon. The leader fetches one scanner page at a time outside its actor callback and rotates launchers after each page. The initiating daemon tracks a recovery from lookup through the target's launch result using the existing launch/result messages. Consistent hashing selects the target.

`ScanBatchSize` defaults to 32; `MaxInFlight` defaults to 64 per initiator, including remote work and retries. Eight I/O workers perform lookup, exact-PID cleanup, and remote delivery. Targets admit eight outstanding launches and coalesce keys. Successful completion immediately releases admission capacity. Init returning Normal ends that recovery; Failures of scanner-backed tasks release their slot and retry through a coalesced recovery scan after `RetryMaxDelay` (default 60 seconds). Exact exited-PID cleanup and tasks without a scanner retain per-key retries. Init that outlives spawn timeout occupies its launch worker until it returns or the launch pool stops. Scanner callbacks must bound their own I/O.

Membership notifies recovery after publishing its topology snapshot, including registrar incarnation events that preserve the node-name set. Launched daemons and the custom-bootstrap spawner notify recovery after business termination. Recovery retries conditional cleanup of that exact exited PID before ensuring another instance. Shutdown recovery uses scanner data after membership changes; the 15-minute full scan repairs missed notifications.

When a registered local instance already exists, the launcher attempts to restore its exact route without repeating Init. Renewal jobs carry their scheduled lease identity so old completions cannot remove a restored schedule.

## Scaling properties

- Lookup work is O(1) locally plus one persistence lookup.
- Topology membership checks use an immutable set and do not call the registrar.
- Consistent-hash placement changes only a subset of keys when membership changes.
- Cron shard preparation uses a fixed-size worker pool rather than one goroutine per shard.
- Lease renewal load is approximately `routed actors / renewal interval`; persistence must be provisioned for this rate and retry headroom.
- Renewal jitter spreads steady-state operations. A sharded timing wheel avoids O(N) periodic scans. Bounded priority queues and a fixed worker pool prevent goroutine or message growth during backend outages, while router-wide log limiting prevents one warning per actor.
- Stress tests register and expire one million routes at once, exercise slow and panicking persistence, and verify bounded queues, fixed concurrency, worker survival, and exit-storm cleanup.
- Route keys and PIDs are never broadcast between cluster nodes.

## Failure semantics

| Failure | Result |
| --- | --- |
| Acquire conflict | Actor initialization fails with `ErrActorRouteTaken`. |
| Acquire storage error | Actor initialization fails with the wrapped storage error. |
| Temporary renew error | Renewal retries; the last successful lease remains authoritative. |
| Lost ownership | The exact old PID is killed; its termination lifecycle runs and daemons recover. |
| Release error | Daemon exit recovery retries exact-PID cleanup; otherwise TTL is the fallback. |
| Node missing from membership | Lookup reports not found without deleting persistence state. |
| Node crash | Once the registrar omits the owner node, acquisition can replace the old route before TTL expiry. |

## Module isolation

The v2 module path is `github.com/qjpcpu/ergo-extensions/v2`. It does not import packages from the root v1 module. v1 source code and behavior remain unchanged.

Recovery scans pace batches with `DaemonOptions.ScanBatchInterval` (default 50 ms). With the default batch size of 32, each leader admits at most about 640 scanned items per second, plus the initial batch; completion messages do not bypass this interval. This trades recovery speed for lower persistence load. Lease renewals remain proportional to active actors divided by the renewal interval; size that interval and Redis capacity together.
