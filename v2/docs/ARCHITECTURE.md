# Ergo Extensions v2 Architecture

## Responsibilities

- `ActorRoutePersistence` stores node sessions and independent `key -> (session, PID)` routes.
- `ActorRouter` binds one session to a node instance and decorates Actor, Supervisor, and Pool behaviors.
- `AddressBook` delegates actor resolution to the router and maintains the topology snapshot for placement.
- `membership` publishes topology from registrar events and refreshes.
- `daemon` coordinates recovery; `cron` distributes scheduler shards using that topology.

## Session and actor lifecycle

Binding opens a fresh session and starts an independent heartbeat and deadline watchdog. All routed actors on that node share its session. The router progresses through Active, Draining, Lost, and Closed. A Lost session remains terminal, even if an earlier renewal succeeds late.

Acquisition runs through a bounded worker pool before business Init. `AcquireRoute` atomically checks the requesting session and the expected `(SessionID, full PID)` owner. Concurrent contenders re-read after comparison failure. A storage error certified with `ErrRouteNotApplied` leaves the session usable; an uncertain write stops the router and initiates owner-side session closure.

The process view preserves the original behavior for Ergo's callback discovery and checks session/route deadlines through `Process.State`. This prevents another mailbox item from entering business dispatch after a local deadline. Each route has one timing-wheel entry for its deadline. Due work is handled in bounded batches, continuing immediately while more expired work remains. Already-running callbacks and goroutines must finish cooperatively.

Lifecycle records remain associated through acquisition, Init, and business Terminate. After cleanup, exact-owner release enters a retained retry queue. Release work receives bounded priority bursts. When pending cleanup reaches the configured threshold, new actor admission applies backpressure. Session renewal runs separately from route workers, so slow lookup, acquisition, or release cannot occupy the heartbeat worker.

Normal shutdown enters Draining, stops the node, then closes the router. Router closure stops local route management and closes the shared session with an operation timeout, invalidating all associated routes. Remaining route records expire through their own TTLs; closure failures are logged and fall back to session expiration. Business cleanup belongs to the node/application shutdown flow. Ergo's node wait can finish before business termination, so callbacks still running when the session closes may overlap a replacement actor. Lost-session closure also proceeds independently of callbacks.

## Lookup and takeover

A valid route requires all three conditions:

1. Its independent route TTL remains live.
2. The referenced session remains live in the consistent storage snapshot.
3. Its owner is online: direct `registrar.Nodes()` membership for remote nodes, or `node.IsAlive()` for self.

Lookup reads storage synchronously in the caller with an operation timeout, then checks validity. Acquisition and release use the route worker pool. Lookup and acquisition use this same predicate. Invalid routes can be replaced with an exact-owner comparison. Only a session's owning router closes it. Each check consumes the registrar's own current result; implementations may maintain their own internal cache. Name-only membership cannot identify a same-name restart until the old session is closed or expires.

A registrar-based takeover may overlap a still-running old owner until its local deadline, potentially for the route TTL (24 hours by default). Route/session checks govern routing and future dispatch; they cannot reverse or serialize external business side effects.

## Membership

The membership actor obtains the registrar from the Ergo node after process initialization. Registrar events trigger a debounced refresh. A periodic refresh repairs missed events and reconnects after failures. Retry delay grows exponentially between configured bounds, and repeated warnings are rate-limited.

AddressBook canonicalizes the node list, updates one consistent-hash ring, publishes an immutable snapshot, and increments `NodesVersion` only when the node set changes. `PickNode` and `PickCoordinatorNode` intentionally use the same ring; v2 has no special directory-node subset.

## Daemon recovery

Every node runs a daemon. The leader fetches one scanner page at a time outside its actor callback and rotates launchers after each page. The initiating daemon tracks a recovery from lookup through the target's launch result using the existing launch/result messages. Consistent hashing selects the target.

`ScanBatchSize` defaults to 32; `MaxInFlight` defaults to 64 per initiator, including remote work and retries. Eight I/O workers perform lookup, exact-PID cleanup, and remote delivery. Targets admit eight outstanding launches and coalesce keys. Successful completion immediately releases admission capacity. Init returning Normal ends that recovery; Failures of scanner-backed tasks release their slot and retry through a coalesced recovery scan after `RetryMaxDelay` (default 60 seconds). Exact exited-PID cleanup and tasks without a scanner retain per-key retries. Init that outlives spawn timeout occupies its launch worker until it returns or the launch pool stops. Scanner callbacks must bound their own I/O.

Membership notifies recovery after publishing its topology snapshot, including registrar incarnation events that preserve the node-name set. Launched daemons and the custom-bootstrap spawner notify recovery after business termination. Recovery retries conditional cleanup of that exact exited PID before ensuring another instance. Shutdown recovery uses scanner data after membership changes; the 15-minute full scan repairs missed notifications.

Expired routed daemons terminate and recover as fresh actor instances through the normal launch path.

## Scaling and persistence

Session renewal traffic is proportional to node count. Acquisition, lookup, release, and garbage collection remain proportional to route activity. Route TTL is independent of session renewal. The default worker count is 16, with a 65,536-entry operation queue and a 65,536 pending-release admission threshold.

The backend must atomically check sessions and compare owners, return remaining validity, honor operation contexts, reclaim unread expired records, and keep session closure terminal against concurrent requests. Local deadlines are anchored to the monotonic request start plus returned validity minus the configured safety margin. Successful persistence operations must remain durable within the backend's stated failure model.

`MemoryActorRoutePersistence` implements this contract for one process using a mutex and indexed expiration heap. Updating TTL replaces the existing heap entry, so repeated session renewal and same-owner registration retain bounded expiration metadata. A bounded background sweep reclaims unread records.

Recovery scans pace batches with `DaemonOptions.ScanBatchInterval` (default 50 ms). At the default batch size of 32, each leader admits about 640 scanned items per second plus the initial batch. This pacing limits recovery storage pressure independently of session heartbeat traffic.
