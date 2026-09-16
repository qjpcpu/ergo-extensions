# Ergo Extensions v2 Architecture

## Responsibilities

- `ActorRoutePersistence` stores node sessions and independent `key -> (session, PID)` routes.
- `ActorRouter` binds one session to a node instance and decorates Actor, Supervisor, and Pool behaviors.
- `AddressBook` delegates actor resolution to the router and maintains the topology snapshot for placement.
- `membership` publishes topology from registrar events and refreshes.
- `daemon` coordinates recovery; `cron` distributes scheduler shards using that topology.

## Session and actor lifecycle

Binding opens a fresh session and starts an independent heartbeat and deadline watchdog. All routed actors on that node share its session. The router progresses through Active, Draining, Lost, and Closed. A Lost session remains terminal, even if an earlier renewal succeeds late.

Acquisition runs through a bounded worker pool before business Init. `AcquireRoute` atomically checks the requesting session and the expected `(SessionID, full PID)` owner. Concurrent contenders re-read after comparison failure. Failed or timed-out initial acquisition prevents business Init without stopping other actors. Queued work checks its context before accessing storage. In-flight work retains cleanup responsibility until it returns; successful or uncertain writes are then released using the exact owner, with failed releases queued for retry. Local replacement waits for the old actor’s callbacks and writes to finish before releasing its route. A confirmed `ErrSessionLost` still invalidates the shared session.

The process view preserves the original behavior for Ergo's callback discovery and checks session/route deadlines through `Process.State`. This prevents another mailbox item from entering business dispatch after a local deadline. Each route has one timing-wheel entry for its next renewal or deadline. Renewals use the existing bounded route worker pool; an in-flight renewal keeps the local deadline scheduled. Due work is handled in bounded batches, continuing immediately while more expired work remains. Already-running callbacks and goroutines must finish cooperatively.

Lifecycle records remain associated through acquisition, Init, and business Terminate. After cleanup, exact-owner release enters a retained retry queue. Release work receives bounded priority bursts. When pending cleanup reaches the configured threshold, new actor admission applies backpressure. Session renewal runs separately from route workers, so slow lookup, acquisition, or release cannot occupy the heartbeat worker.

Node shutdown closes the router before stopping the node, for both normal and forced shutdown. Router closure stops admission and local route management, then closes the shared session with an operation timeout, invalidating all associated routes independently of business cleanup. Remaining route records expire through their own TTLs; closure failures are logged and shutdown continues with session expiration as the fallback. Business cleanup belongs to the node/application shutdown flow. Already-running callbacks may overlap a replacement actor. Existing actors follow the node's normal shutdown lifecycle, including shutdown messages and business termination callbacks. Lost-session closure also proceeds independently of callbacks.

## Lookup and takeover

A valid route requires all three conditions:

1. Its independent route TTL remains live.
2. The referenced session remains live in the consistent storage snapshot.
3. The owner node is present in a direct `registrar.Nodes()` result, or `node.IsAlive()` for self.

Lookup reads storage synchronously in the caller with an operation timeout, then checks validity. Acquisition and release use the route worker pool. Lookup and acquisition use this same predicate. Invalid routes can be replaced with an exact-owner comparison. Only a session's owning router closes it. Registrar failures propagate and prevent takeover. An absent node makes its route reclaimable even while its session remains live. This deliberately accepts overlapping actors during discovery lag in exchange for prompt recovery. Name-only membership cannot distinguish a same-name restart until the old session closes or expires.

Route TTL defaults to 2 hours and route renewal to 100 minutes with ±10% per-actor jitter (90–110 minutes). Both are configurable through `StartSimpleNode(SimpleNodeOptions{ActorRouterOptions: ...})` using `RouteTTL` and `RouteRenewInterval`. The TTL must cover the renewal interval plus its 10% jitter, operation timeout, and safety margin.

Renewal calls `AcquireRoute` with the actor's own exact session and PID as the expected owner. It extends the deadline on success, stops the displaced actor on an owner mismatch, and retries errors without extending the last confirmed deadline. It never re-reads another owner to take the route back. Late results cannot restart dispatch after expiry. Pending cleanup waits for in-flight renewal before releasing the exact owner. First and subsequent renewal times are spread using jitter seeded by the key and full PID. Storage failures and queue saturation back off from a 1-second base to at most 60 seconds (or the configured renewal interval when shorter), with ±10% jitter; deadlines remain scheduled throughout. The worker pool bounds concurrency, not storage writes per second.

Concurrent execution after takeover is accepted. An old actor normally detects replacement at its next renewal; failures or blocked workers leave its original route deadline as the bound on further dispatch. Already-running callbacks must finish cooperatively. Direct-PID messages, mailbox work, and self-timers can execute on the old actor during this interval; routing does not serialize external side effects.

## Membership

The membership actor obtains the registrar from the Ergo node after process initialization. Registrar events trigger a debounced refresh. A periodic refresh repairs missed events and reconnects after failures. Retry delay grows exponentially between configured bounds, and repeated warnings are rate-limited.

AddressBook canonicalizes the node list, updates one consistent-hash ring, publishes an immutable snapshot, and increments `NodesVersion` only when the node set changes. `PickNode` and `PickCoordinatorNode` intentionally use the same ring; v2 has no special directory-node subset.

## Daemon recovery

Every node runs a daemon. The leader sends one scanner page task at a time to the I/O pool and rotates launchers after each page. The I/O worker creates or resumes the iterator and returns `messageScanPage`; the daemon owns page admission, scheduling, and retry state. Consistent hashing selects each task's target. `ScanBatchSize` defaults to 32 and bounds one scanner callback; `MaxInFlight` defaults to 64 per initiator and includes lookup, waiting tasks, local and remote launches, and retained retries. A full initiator keeps remaining items in its current scanner page. Completion releases capacity and schedules the next scanner step immediately. Sixteen I/O workers perform scanner page reads, lookup, exact-PID cleanup, and remote delivery.

After lookup, the initiator retains the launch arguments and sends a task offer containing the name, owner, and epoch. The target acknowledges the offer and queues its identity. It coalesces same-name offers from multiple initiators and records each request for result delivery. An idle launch worker reserves a queued task, and the target requests its arguments from the initiator. Only that reservation receives the corresponding launch message. When the worker finishes, the target replies to all waiting requests and immediately pulls another task for that worker. Eight workers execute Init, with one reservation or running task per worker. Protocol delivery proceeds independently of the initiator's task admission limit.

Unacknowledged offers are resent after `LaunchTimeout`. Acknowledged tasks wait for worker capacity without a launch timer. The initiator starts its launch deadline when responding to a pull; the target also times out reservations whose arguments never arrive. Withdrawal removes a queued request or an unstarted reservation and acknowledges a terminal result. Targets retain terminal results and resend them after `LaunchTimeout` until acknowledged or the initiating daemon exits; acknowledged capacity waits therefore also recover from a lost completion. A running Init keeps its worker until it returns or the worker stops, including when spawn times out. Peer monitoring is established by an I/O worker, which owns the remote monitor and forwards down notifications. The daemon coalesces pending monitor requests per node and resumes offers after completion; withdrawals cancel pending offers, and task placement is checked before resuming. It monitors the owning worker locally so worker or pool exit releases abandoned reservations and retries affected tasks. Topology changes withdraw old offers and recheck placement.

Init returning Normal ends that recovery. Scanner-backed failures release their slot and retry through a coalesced recovery scan after `RetryMaxDelay` (default 60 seconds). Exact exited-PID cleanup and tasks without scanners retain per-key retries. Scanner callbacks and business Init must bound their own I/O.

Membership notifies recovery after publishing its topology snapshot, including registrar incarnation events that preserve the node-name set. Launched daemons and the custom-bootstrap spawner notify recovery after business termination. Recovery retries conditional cleanup of that exact exited PID before ensuring another instance. Shutdown recovery uses scanner data after membership changes; the 15-minute full scan repairs missed notifications.

Expired routed daemons terminate and recover as fresh actor instances through the normal launch path.

## Scaling and persistence

Session renewal traffic is proportional to node count; route renewal traffic is proportional to actor count divided by `RouteRenewInterval`. Acquisition, lookup, release, and garbage collection remain proportional to route activity. Route TTL is independent of session renewal. The default worker count is 16, with a 65,536-entry operation queue and a 65,536 pending-release admission threshold.

The backend must atomically check sessions and compare owners, return remaining validity, honor operation contexts, reclaim unread expired records, and keep session closure terminal against concurrent requests. Local deadlines are anchored to the monotonic request start plus returned validity minus the configured safety margin. Successful persistence operations must remain durable within the backend's stated failure model.

`MemoryActorRoutePersistence` implements this contract for one process using a mutex and indexed expiration heap. Updating TTL replaces the existing heap entry, so repeated session renewal and same-owner registration retain bounded expiration metadata. A bounded background sweep reclaims unread records.

Recovery throughput follows task capacity and worker completion. Scanner batches yield through self-messages, and capacity release resumes pending work. Persistence concurrency remains bounded by the sixteen I/O workers per daemon.
