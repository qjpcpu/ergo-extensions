# Daemon Async Launch Change Notes

## Background

The original `system/daemon_process.go` recovery path was:

1. leader scans desired daemon list
2. `Locate` the process name through `WhereIs`
3. if not found, send a launch request or spawn locally

That model had two problems for the target scenario:

- It relied on a synchronous `Locate` call in the launch path.
- It treated `WhereIs` as part of the uniqueness guard, even though `WhereIs` is an eventually consistent directory built from periodic scans.

For a large-scale actor keepalive workload, this means:

- a single slow or hanging target actor can hold up launch coordination
- repeated recovery attempts can pile onto the same hot key
- uniqueness is only "best effort + eventual convergence", not a launch-path guarantee

## Goal

The goal of this change is not to provide strict distributed uniqueness.

The goal is:

- make daemon recovery non-blocking
- avoid `call/response` in the actor startup path
- serialize duplicate launch intent for the same actor name on one owner node
- keep the system responsive even if one actor startup hangs or is slow
- reduce duplicate launch amplification under concurrent ensure requests

## What Changed

### 1. Added asynchronous ensure protocol

New message types were introduced in `system/types.go`:

- `MessageEnsureDaemon`
- `MessageLaunchOneDaemon`
- `MessageDaemonLaunchResult`
- internal `messageDaemonLaunchTimeout`

Purpose:

- `MessageEnsureDaemon` expresses intent: "this daemon should exist"
- `MessageLaunchOneDaemon` is the fire-and-forget dispatch from owner to target node
- `MessageDaemonLaunchResult` reports launch outcome asynchronously
- timeout messages drive retry without blocking

### 2. Changed daemon recovery from direct launch to owner coordination

`recoverDaemon` no longer directly does `Locate -> launch`.

It now routes each process name through `ensureDaemon(...)`, which sends the request to the directory owner chosen by:

- `AddressBook.PickDirectoryNode(processName)`

This makes one node responsible for coordinating repeated ensure requests for the same name.

### 3. Added per-name in-memory launch state on the owner

`daemon` now keeps:

- `launching map[gen.Atom]daemonLaunchState`
- `nextEpoch`

Per-name state includes:

- launcher
- process payload
- target node
- epoch
- retry attempt
- phase

Current phases:

- `daemonLaunchPhaseLaunching`
- `daemonLaunchPhaseRunningGrace`

Purpose:

- coalesce duplicate ensure requests
- ignore stale async results
- retry through timeout instead of waiting synchronously

### 4. Removed synchronous `Locate` from the launch path

Previously `launchDaemonOnNode` performed:

- `w.book.QueryBy(w, QueryOption{Timeout: 15}).Locate(proc.ProcessName)`

This was the blocking part of the path.

Now the owner checks only its local directory view using:

- `book.LocateLocal(processName)`

This keeps the coordination path non-blocking and avoids hanging on remote lookup.

To support that, `LocateLocal` was added to the `IAddressBook` interface.

### 5. Moved actual spawn into an isolated worker actor

The owner does not run `SpawnRegister` directly in its main process loop.

Instead, the target node spawns a short-lived `daemonLaunchWorker`, which:

- receives the launch request
- performs `SpawnRegister`
- sends `MessageDaemonLaunchResult` back to the owner
- terminates

Purpose:

- prevent the daemon coordinator actor from doing potentially slow startup work
- isolate per-actor launch execution from the owner control path

### 6. Added timeout and retry scheduling

Owner-side coordination now uses timers instead of waiting:

- launch timeout after dispatch
- grace timeout after a successful start notification
- exponential-ish retry with jitter

Purpose:

- avoid deadlocking the control path
- allow recovery from lost async result or slow visibility convergence

## Behavioral Intent

After this change, the daemon path should behave like this:

1. scanner says a daemon should exist
2. request is routed to the name's directory owner
3. owner checks whether the name is already visible locally
4. if not already running and not already launching, owner picks a target node
5. owner asynchronously dispatches a launch request
6. target launches in a separate worker actor
7. worker asynchronously reports result
8. owner transitions state or retries later

This gives:

- no synchronous confirmation wait
- no per-key blocking on startup
- coalescing of duplicate ensure requests on the owner

## Important Non-Goals / Current Limits

This change does **not** implement a true distributed lease or CAS-backed claim.

That means:

- it does not provide strict global uniqueness across ownership failover
- if directory ownership changes during churn, old and new owners may still race in edge cases
- uniqueness is improved by single-owner coordination, but still bounded by the current eventually consistent cluster model

In other words:

- this is a throughput and coordination improvement
- not a final strong-uniqueness design

## Why This Is Still Better Than Before

Compared to the old model, this version:

- removes blocking `Locate` from the launch path
- prevents the main daemon actor from synchronously waiting for launch completion
- localizes duplicate suppression to one owner node
- makes retries explicit and timer-driven
- limits the blast radius of one slow actor startup

For the stated requirement of "do not use call/response because one actor may hang", this directly addresses the main issue.

## Tests Added

Added `TestDaemonEnsureConcurrentSingleLaunch` in `system/daemon_process_test.go`.

It verifies:

- many concurrent `EnsureDaemon` requests for the same process name
- only one actual actor init happens

Existing daemon and system tests also still pass.

## Suggested Review Focus For Claude Code

Please review the following areas carefully:

### Correctness

- whether owner-side `LocateLocal` is sufficient for this coordination model
- whether stale async results are fully ignored by `epoch`
- whether retries can cause duplicate launches under owner rebalance
- whether `RunningGrace` semantics are correct or too optimistic

### Failure Modes

- target node receives launch but dies before reporting result
- async result message is lost
- owner changes while a launch is in flight
- a process starts successfully but `WhereIs` visibility lags

### Performance

- whether spawning one temporary worker actor per launch is acceptable at scale
- whether retry timers or logging can become noisy for hot keys
- whether `launching` map cleanup is sufficient under churn

### Architecture

- whether this should be split into a dedicated coordinator process instead of extending `extensions_daemon`
- whether the current owner selection via `PickDirectoryNode` is the right long-term place to coordinate daemon ensure
- whether a proper external lease/CAS layer should be introduced next

## Likely Next Step

If the repository wants true "only one effective launcher in the cluster" semantics even during owner failover, the next step is:

- introduce a real distributed claim/lease abstraction
- make the owner obtain that claim before dispatch
- keep the async result model from this patch

That would preserve the non-blocking startup path while upgrading uniqueness from "owner-coordinated best effort" to "claim-backed coordination".
