# Ergo Extensions Architecture Overview

This document summarizes the project structure and the main runtime components.

## Project Overview

Ergo Extensions provides building blocks for distributed process discovery and daemon orchestration within an [Ergo](https://ergo.services)-based cluster. It supports process naming, leader election, and automatic daemon recovery across nodes.

## Main Components

- `system.WhereIsProcess`: periodically inspects local processes and maintains an eventually consistent `AddressBook`.
- `system.DaemonMonitorProcess`: coordinates daemon recovery and remote spawning from the elected leader.
- `system.CronJobProcess`: a distributed cron-like scheduler.
- `system.AddressBook`: a thread-safe cache using consistent hashing (`xxhash` + `buraksezer/consistent`) for process-to-node mapping.

Note: `AddressBook.LocateLocal` only queries local memory data and should not be relied upon for guaranteed global process discovery.

## Runtime Shape

- `system.Supervisor` starts `WhereIsProcess`, `DaemonMonitorProcess`, and `CronJobProcess`.
- `app.StartSimpleNode` wires the supervisor into an Ergo node and shares a single `AddressBook` instance with the application layer.
- The discovery path is owner-based: a process name is mapped to a directory node with consistent hashing, then resolved locally or forwarded once.
- Cron state is split into provider data and shared KV state; the shared KV tracks leases, checkpoints, and dispatch records.

## Configuration

- Node setup is controlled by `app.SimpleNodeOptions` and `gen.NodeOptions`.
- `app.StartSimpleNode` accepts a ZooKeeper registrar via `SimpleNodeOptions.Endpoints`, a custom registrar via `SimpleNodeOptions.Registrar`, or the built-in in-memory registrar otherwise.
- `GetAdvertiseAddressByENV` can be used to control the advertised address from environment variables.

## Directory Structure

- `/app`: helpers for starting and interacting with nodes.
- `/system`: discovery, daemon orchestration, cron scheduling, and the shared address book.
- `/registrar`: registrar implementations, including in-memory cluster support.

## WhereIs Process Constraints

To keep `WhereIsProcess` stable at scale:

- Do not introduce full-mesh connectivity.
- Avoid broadcast storms during synchronization.
- Avoid concentrated broadcast storms during node join/leave events.
- Improve process location efficiency only within those bounds.

`registrar.Nodes()` is considered efficient and may be used.
