# Ergo Extensions Architecture Overview

This document describes the runtime design of the `system` package. The package adds distributed process discovery, daemon recovery, and sharded cron scheduling to an [Ergo](https://ergo.services)-based cluster.

## Project Goals

The repository focuses on three infrastructure problems that tend to appear together in Ergo deployments:

- naming and locating long-lived processes across a changing cluster,
- recovering singleton or managed daemon processes after failures or node churn,
- dispatching scheduled jobs without centralizing all cron state on one node.

The design intentionally keeps each concern in a separate actor while sharing a small set of core data structures and message contracts.

## Runtime Topology

The `system.Supervisor` process starts four long-running child actors:

- `system.WhereIsProcess`
- `system.PlacementMonitorProcess`
- `system.DaemonMonitorProcess`
- `system.CronJobProcess`

All four actors run independently. `WhereIsProcess` and `DaemonMonitorProcess` use the registrar for cluster membership and leader information. `WhereIsProcess` and `DaemonMonitorProcess` also share an in-memory `AddressBook`, which is created once by the supervisor and can be injected from the application layer.

At node startup, `app.StartSimpleNode` wires the supervisor into the Ergo application tree and exposes the same `AddressBook` instance to helper APIs such as distributed locate calls.

## System Package Layout

- `/system/facade.go`: public aliases and constructors for the internal core types.
- `/system/supervisor.go`: starts and supervises the runtime actors.
- `/system/whereis`: distributed process directory synchronization and locate forwarding.
- `/system/daemon`: leader-driven daemon recovery and remote spawning.
- `/system/cron`: sharded cron scheduler, source abstraction, and durable state backend.
- `/system/internal/core`: shared address book, launcher registry, immutable helpers, and message definitions.

## Shared Core Design

### AddressBook

`system.AddressBook` is the shared in-memory control-plane cache. It has two separate consistent-hash rings:

- the data ring, used by `PickNode(processName)` to select a target execution node,
- the directory ring, used by `PickDirectoryNode(processName)` to select the node responsible for storing the authoritative directory shard for that process name.

This split is important:

- daemon placement and process ownership do not need to match directory ownership,
- process discovery can scale without requiring every node to store every global mapping,
- topology changes can rebalance directory responsibility independently from daemon placement.

Internally the address book stores:

- `nodes`: currently available cluster members,
- `nodeProcesses`: per-node named process snapshots,
- `processToNodes`: reverse index from process name to candidate nodes.

Read semantics are intentionally eventual:

- `LocateLocal` reads only the current memory view,
- if the same process name temporarily exists on multiple nodes, the oldest `BirthAt` wins,
- offline nodes are removed from the reverse index when membership changes.

The address book also caches the available-node list in an atomic value to reduce lock contention on hot read paths.

### Public Messages and Launchers

`system/internal/core/types.go` defines the cross-process protocol used by the runtime actors:

- locate flow: `MessageLocate`, `MessageForwardLocate`, `MessageLocateResult`,
- directory replication: `MessageProcessChanged`,
- daemon orchestration: `MessageEnsureDaemon`, `MessageLaunchOneDaemon`, `MessageDaemonLaunchResult`,
- address book access: `MessageGetAddressBook`, `MessageAddressBook`.

Launcher registration lives in `system/internal/core/launcher.go`. A launcher describes:

- how to spawn one daemon process,
- the Ergo spawn options required for that process,
- an optional recovery scanner that enumerates the desired daemon set after leader election or node loss.

This keeps the daemon subsystem generic: it only orchestrates placement and retries, while the application defines the actual daemon types.

## Supervisor Design

`system.Supervisor` is a one-for-one Ergo supervisor. Its only job is process lifecycle management:

- it creates or accepts a shared `AddressBook`,
- it starts `WhereIsProcess`, `PlacementMonitorProcess`, `DaemonMonitorProcess`, and `CronJobProcess`,
- it applies transient restart semantics so temporary failures do not take down the full node runtime.

The supervisor deliberately contains no control logic. Cluster behavior belongs in child actors so each concern can be reasoned about and tested independently.

## WhereIs Design

### Responsibility

`system.WhereIsProcess` maintains an eventually consistent distributed directory of named Ergo processes. Each node publishes explicit local registration updates when helpers are used, periodically inspects its local process table as a fallback, publishes ownership updates to the appropriate directory nodes, and answers locate requests.

`system.PlacementMonitorProcess` is a local companion actor for duplicate placement notifications. A local process sends `MonitorPlacement{Name}` to it; the monitor periodically locates that name via `WhereIsProcess` and sends `DuplicatePlacement{Name, Node}` back when the selected placement is on another node. It only notifies and does not kill, migrate, or repair processes.

### Local State

The actor tracks:

- the shared `AddressBook`,
- registrar membership state,
- the node's own monotonically increasing `ProcessVersion`,
- the latest accepted version from every remote node,
- local PID/name/birth-time maps used to compute diffs between inspection cycles,
- a cached snapshot of all local named processes.

### Process Inspection

The fallback loop is periodic:

1. fetch the current local Ergo process list,
2. compare it with the last observed PID set,
3. derive `up`, `down`, and full snapshots of named processes,
4. update the local address book entry,
5. publish either incremental shard updates or a periodic full anti-entropy sync.

Incremental sync is used when only a few process entries changed. Full sync is forced periodically and after topology churn so stale directory state is eventually overwritten even if individual update messages were lost.

The default `WhereIsOptions.SyncInterval` is 2 seconds. Explicit local registration messages are the fast path for new named processes; the periodic loop is the anti-entropy fallback for processes spawned outside the helpers or for missed messages.

### Directory Sharding

Each process name is mapped to one directory node using the address book's directory ring. The local node never broadcasts process changes to the entire cluster. Instead it:

- groups changed processes by directory owner,
- sends one `MessageProcessChanged` per owner,
- stores its own local processes in the shared address book immediately.

During topology changes, `syncDirectoryShards` sends a full authoritative shard snapshot to every current directory node, including empty shards. That last detail matters because a previous directory owner must be able to clear stale data after rebalancing.

### Locate Path

The locate protocol is owner-based:

1. caller sends `MessageLocate{Name}`,
2. receiving `WhereIsProcess` computes the directory owner,
3. if the current node is the owner, it resolves from `AddressBook.LocateLocal`,
4. otherwise it forwards a `MessageForwardLocate` to the owner,
5. the owner replies with the resolved node name or an empty result.

Forwarding is hop-limited. The purpose is not arbitrary routing, but a single redirected lookup after directory ownership changes.

### Topology Handling

`WhereIsProcess` subscribes to registrar node join and leave events. Membership churn is debounced with randomized delay:

- the delay scales with cluster size,
- only one topology-change sync is allowed to win via `topologyChangeID`,
- the anti-entropy full sync is reused to repair shard ownership after rebalance.

This is the main scalability guardrail for the module.

### Design Constraints

Changes to `WhereIsProcess` must preserve these properties:

- no full-mesh node-to-node directory synchronization,
- no broadcast storms during steady-state sync,
- no concentrated storm when many nodes join or leave together,
- process-location improvements must stay compatible with those limits.

`registrar.Nodes()` is considered efficient and safe to use for membership refresh.

## Daemon Monitor Design

### Responsibility

`system.DaemonMonitorProcess` ensures registered daemon processes exist somewhere in the cluster. It does not own daemon business logic; it owns recovery, placement, retries, and deduplication.

### Leader-Driven Recovery

Only the registrar-elected leader performs recovery scans. The daemon actor listens for:

- `EventNodeSwitchedToLeader`,
- `EventNodeSwitchedToFollower`,
- `EventNodeLeft`.

When the local node becomes leader, or when a node leaves, the actor schedules a delayed recovery pass. The delay includes jitter so duplicate triggers do not cause synchronized recovery bursts.

The default daemon recovery profile is tuned for fast convergence: initial, leader, and node-left recovery are scheduled after 500ms plus bounded jitter; launch confirmation timeout defaults to 3 seconds and running grace defaults to 2 seconds. These values can be changed with `DaemonOptions`.

### Recovery Model

Recovery is driven by application-registered launchers. For each launcher:

1. call its `RecoveryScanner`,
2. iterate the desired daemon list,
3. for each daemon name, compute its directory owner,
4. send `MessageEnsureDaemon` to that owner.

The directory owner is the serialization point for a daemon name. This prevents multiple leaders or multiple observers from racing to launch the same process independently.

### Launch State Machine

The owner node keeps a `launching` map keyed by daemon process name. Each entry contains:

- launcher identity,
- daemon arguments,
- selected target node,
- logical epoch,
- retry attempt count,
- current phase.

The phases are:

- `launching`: launch was dispatched but the worker has not reported success yet,
- `running_grace`: the worker reported `started` or `already_taken`, and the owner is waiting for `WhereIsProcess` to observe the named process in the address book.

Epochs make delayed worker replies and timeout messages harmless. Any response with an old epoch is ignored.

### Placement and Retry

The owner chooses the target execution node with `AddressBook.PickNode(processName)`. That gives a stable consistent-hash placement while still allowing rebalance when nodes change.

Launching is done by spawning a short-lived local worker on the target node. The worker:

- looks up the launcher definition,
- calls `SpawnRegister`,
- reports `started`, `already_taken`, or `failed` back to the owner.

Failures and timeouts schedule `MessageEnsureDaemon` with exponential backoff plus jitter. The retry state remains name-scoped, not node-scoped, so a later attempt can pick a different node after topology changes.

### Safety Characteristics

The daemon subsystem relies on several defensive rules:

- only the directory owner can coordinate launch attempts for a daemon name,
- `already_taken` is treated as success because the target state is "daemon exists",
- stale launching entries are periodically cleaned during full recovery,
- discovery confirmation comes from `WhereIsProcess`, not from trusting the launch worker alone.

## Cron Scheduler Design

### Responsibility

`system.CronJobProcess` is a distributed, shard-based scheduler. It separates job definitions from scheduling state:

- job definitions come from a `JobProvider`,
- leases, checkpoints, and dispatch records live in a shared `KVStore`.

This separation lets the provider be optimized for configuration management while the KV store enforces execution coordination.

### Ownership and Ring

The cron process builds its own consistent-hash ring over cluster nodes. For each logical shard in `0..ShardCount-1`, the ring selects exactly one owner node.

The actor keeps `owned[shard] => shardRuntime` for the shards currently assigned to the local node. On membership changes it rebalances:

1. refresh the owner ring from registrar membership,
2. compute which shards should now belong to the local node,
3. stop watchers for shards that moved away,
4. load newly owned shards and activate them.

Shard loading can run concurrently, bounded by `ScanConcurrency`.

### Shard Acquisition

Before a node can schedule a shard, it must acquire a lease in the shared KV store. The lease record contains:

- owner node,
- lease epoch,
- expiration time.

The epoch increments whenever an expired lease is taken over by a new owner. That epoch is later used to reject stale checkpoint and dispatch acknowledgements from a previous owner.

### Shard Runtime

A loaded shard runtime contains:

- compiled jobs for that shard,
- the last provider cursor,
- the current lease,
- the scheduler checkpoint,
- in-memory slot indexes for due-job calculation,
- an optional watch process for incremental updates.

Activation happens only after the shard snapshot is loaded, checkpoint is restored, and replay from the last durable slot to the current slot completes.

### Snapshot, Watch, and Replay

Shard initialization has three stages:

1. scan the provider snapshot page by page for the shard,
2. compile each `JobSpec` into an executable schedule representation,
3. load the shard checkpoint and replay unprocessed slots into the pending queue.

If the provider supports watch, a linked watch actor is started after the snapshot is committed. The watch actor streams `JobDeltaBatch` updates back to the parent cron actor. Generation IDs ensure that old watchers cannot mutate a newer shard runtime after rebalance.

### Tick Loop

The cron actor aligns its ticks to `TickResolution`. On each tick it:

1. renews leases that are close to expiry,
2. computes every logical slot from `lastTick+resolution` through the current slot,
3. collects due jobs from each active shard runtime,
4. converts them into pending dispatches,
5. flushes up to `MaxDispatchPerTick` jobs through the trigger implementation.

This design allows bounded catch-up after short pauses or node restarts instead of assuming exactly-on-time ticks.

### Dispatch Protocol

Dispatch coordination uses the KV state backend:

1. `ClaimDispatches` attempts to create one record per `(shard, job, scheduled_at)`,
2. if the record is new, the job becomes pending for local trigger,
3. if the record is already `acked`, the job is considered already delivered,
4. after trigger success, `AckDispatches` marks the record as acknowledged,
5. checkpoint advancement is attempted only after the shard slot is fully acknowledged.

The durable dispatch record is the deduplication boundary. It prevents repeated scheduling after crashes, ownership changes, or replay.

### Checkpointing

Each shard stores a durable checkpoint representing the highest fully completed slot. A slot is completed only when:

- all jobs due in that slot have either been observed as already acknowledged, or
- newly triggered jobs were acknowledged successfully.

This means replay after restart starts from the last safely completed slot, not from the current time.

### State Backend

`stateBackend` provides CAS-based coordination on top of a generic `KVStore`. It manages three record families:

- shard lease records,
- shard checkpoint records,
- dispatch state records.

The implementation intentionally uses small retry loops around `PutIfAbsent` and `CompareAndSwap` instead of introducing global locking. Concurrency control is delegated to the storage engine's versioned writes.

### Trigger Model

The default trigger is local Ergo message delivery:

- single dispatch sends `MessageTrigger`,
- batch dispatch sends `MessageTriggerBatch` when enabled.

The trigger only reports whether dispatch succeeded from the scheduler's perspective. Business-level idempotency is still the responsibility of the target process if a job's side effects are not naturally idempotent.

## Configuration Surfaces

### Node and Supervisor

- `app.SimpleNodeOptions` controls registrar wiring and runtime startup.
- `ApplicationMemberSpecOptions` controls the injected address book, cron source, cron scheduler options, the `WhereIs` inspection interval, and the placement monitor interval.

### Cron

Key `SchedulerOptions` values:

- `ShardCount`: total logical shard count,
- `TickResolution`: scheduler granularity,
- `ScanPageSize`: provider page size during snapshot load,
- `ScanConcurrency`: parallelism for shard loading,
- `RebalanceDelay`: debounce for topology-triggered rebalance,
- `LeaseTTL`: shard lease duration,
- `MaxDispatchPerTick`: backpressure cap per tick.

## Extension Guidance

When extending the system package:

- keep actor responsibilities narrow and protocol-oriented,
- prefer ownership-based routing over broadcast,
- treat registrar membership as eventually consistent input,
- keep durable coordination in KV when correctness must survive restart,
- do not rely on local address book reads for globally strong answers,
- preserve epoch or generation checks anywhere delayed messages can race with rebalance.

## Key Runtime Flows

### WhereIs Locate Flow

The normal locate path for `system.QueryBy(...).Locate(name)` is:

1. the caller sends `MessageLocate{Name}` to the local `WhereIsProcess`,
2. the local node computes the directory owner via `PickDirectoryNode(name)`,
3. if the local node is the owner, it resolves directly from `AddressBook.LocateLocal`,
4. otherwise it forwards `MessageForwardLocate` to the owner node,
5. the owner resolves against its local directory shard,
6. the owner replies with the selected node name,
7. the caller decides how to contact the target process on that node.

Failure and consistency notes:

- if no directory owner exists, the response is empty instead of blocking,
- forwarding is hop-limited so stale ownership cannot create loops,
- the returned value is a node name, not a PID,
- if the cluster is converging, a caller may need to retry after a short delay.

### WhereIs Sync Flow

The steady-state sync path for one node is:

1. periodic inspection reads the current local Ergo process list,
2. local caches derive `up`, `down`, and full snapshots for named processes only,
3. the local address book entry is updated immediately,
4. changed names are grouped by directory owner,
5. one `MessageProcessChanged` is sent to each relevant owner,
6. the remote owner applies the update only if `Version` is newer than the last accepted version for that source node.

The anti-entropy path differs in two places:

1. the source node increments its version and marks the update as `FullSync`,
2. receivers replace the full process set for that source node instead of applying a delta.

### Daemon Recovery Flow

The full daemon recovery path after a leader election is:

1. registrar emits `EventNodeSwitchedToLeader`,
2. the local `DaemonMonitorProcess` marks itself leader and schedules delayed recovery,
3. the recovery pass iterates all registered launchers,
4. each launcher's `RecoveryScanner` enumerates desired daemon processes,
5. each daemon name is routed to its directory owner with `MessageEnsureDaemon`,
6. the owner checks whether the daemon is already visible in the address book,
7. if missing, the owner selects a target node with `PickNode(processName)`,
8. the owner records a `launching` entry with a new epoch,
9. the owner sends `MessageLaunchOneDaemon` to the target node,
10. the target node spawns a short-lived launch worker,
11. the worker attempts `SpawnRegister`,
12. the worker replies `started`, `already_taken`, or `failed`,
13. the owner moves into grace, retries, or clears state accordingly,
14. the launch worker publishes a local whereis registration update, with periodic inspection as fallback,
15. `WhereIsProcess` observes the named process and the owner clears the launch state.

The key serialization rule is that only the directory owner is allowed to drive steps 6 through 15 for a given daemon name.

### Daemon Retry Flow

A failed or slow launch follows this retry path:

1. owner schedules `messageDaemonLaunchTimeout` when a launch starts,
2. if the worker reports failure, the owner deletes the in-flight state and re-enqueues `MessageEnsureDaemon`,
3. if the worker reports success, the owner switches to running grace and waits for directory convergence,
4. if the timeout fires before convergence, the owner checks whether the daemon is now visible,
5. if still missing, the owner retries with incremented attempt count and exponential backoff with jitter.

This design treats launch acknowledgement and discoverability as separate events. That separation is what lets the system survive worker success followed by delayed directory convergence.

### Cron Rebalance Flow

The shard rebalance path is:

1. registrar emits node join or leave,
2. `CronJobProcess` schedules a debounced rebalance,
3. the cron actor refreshes its owner ring from `registrar.Nodes()`,
4. it computes the shard set that should now belong to the local node,
5. it stops watchers and drops runtimes for shards that moved away,
6. it loads newly owned shards, optionally in parallel,
7. each new shard acquires a KV lease before becoming active,
8. each active shard starts an optional watch actor after snapshot load.

A shard is not considered live just because the ring says so. It becomes executable only after lease acquisition, snapshot load, checkpoint restore, replay, and runtime activation all complete.

### Cron Dispatch Flow

The steady-state dispatch path for one slot is:

1. the tick loop advances from `lastTick` to the current aligned slot,
2. each active shard runtime returns the jobs due in that slot,
3. the cron actor claims dispatch records in KV for `(shard, job, scheduled_at)`,
4. already-acked records are skipped as already delivered work,
5. newly claimed records are appended to the in-memory pending queue,
6. `flushPending` sends up to `MaxDispatchPerTick` jobs through the trigger,
7. successful deliveries are acknowledged in KV,
8. shard runtime marks the slot acknowledged and reschedules recurring jobs,
9. checkpoint advancement persists the highest fully completed slot.

Correctness depends on the order above:

- claim before trigger prevents duplicate concurrent delivery,
- ack after trigger prevents false completion,
- checkpoint after ack prevents replay gaps.

### Cron Restart Replay Flow

When a node newly acquires a shard after restart or rebalance:

1. it loads the durable checkpoint from KV,
2. it computes the current slot from `TickResolution`,
3. it replays every slot from `checkpoint+1` to `currentSlot`,
4. each replayed slot goes through the same claim logic as a real-time tick,
5. only slots that become fully acknowledged can advance the checkpoint.

This means replay is convergent:

- already dispatched and acknowledged jobs stay skipped,
- pending-but-unacked jobs can be retried,
- future jobs stay scheduled according to the compiled cron plan.
