# Ergo Extensions Architecture Overview

This document provides a high-level overview of the Ergo Extensions project to help developers and AI agents understand the codebase structure and conventions.

## 1. Project Overview
Ergo Extensions provides building blocks for distributed process discovery and daemon orchestration within an [Ergo](https://ergo.services)-based cluster. It facilitates process naming, leader election, and automated daemon recovery across multiple nodes.

### Key Components:
- **`system.WhereIsProcess`**: Periodically inspects local processes and maintains an eventually consistent `AddressBook`.
- **`system.DaemonMonitorProcess`**: Coordinates daemon recovery and remote spawning from the elected leader.
- **`system.CronJobProcess`**: A distributed cron-like scheduler.
- **`system.AddressBook`**: A thread-safe cache using consistent hashing (`xxhash` + `buraksezer/consistent`) for process-to-node mapping. **Note**: `LocateLocal` method only queries local memory data and should not be relied upon for guaranteed global process discovery.

## 2. Build & Commands
The project is built using standard Go toolchains (Go 1.24+).

- **Update Dependencies**: `go mod tidy`
- **Build**: `go build ./...`
- **Test**: `go test ./...`
- **Coverage**: `go test -coverprofile=coverage.out ./...`

## 3. Code Style
- **Formatting**: Standard `gofmt` and `goimports`.
- **Naming**: Follows standard Go conventions (CamelCase).
- **Patterns**: Heavily uses the Ergo/Erlang actor model (`gen.ProcessBehavior`, `gen.ApplicationBehavior`).
- **Communication**: Inter-process communication via `self.Send`, `self.Call`, and `self.Cast`.
- **Interaction**: Prefer using Chinese for communication and interaction with the user.
- **Comments**:
  - Always write comments in English.
  - Use `//` for comments on the same line as code.
  - Use `/* ... */` for comments on their own line or multi-line comments.

## 4. Testing
- **Framework**: Standard Go `testing` package.
- **Structure**: Tests are co-located with source code in `_test.go` files.
- **Execution**: Run all tests with `go test ./...`.
- **Key Tests**: `system/whereis_convergence_test.go` verifies eventual consistency of the address book across multiple nodes.

## 5. Security
- **Node Authentication**: Uses Ergo's cookie-based authentication.
- **Encryption**: By default, `StartSimpleNode` sets `InsecureSkipVerify: true`. Production environments should configure TLS via Ergo node options.
- **Data Protection**: Avoid logging sensitive process arguments in `DaemonProcess.Args`.

## 6. Configuration
- **Node Setup**: Configured via `app.SimpleNodeOptions` and `gen.NodeOptions`.
- **Registrar**: Supports Zookeeper (`ergo.services/registrar/zk`) or a built-in in-memory registrar for local development.
- **Environment Variables**: `GetAdvertiseAddressByENV` can be used to set the node's advertise address via environment variables.

## 7. Directory Structure
- `/app`: High-level helpers for starting and managing nodes.
- `/system`: Core logic for supervisors, discovery, and daemons.
- `/registrar`: Registrar implementations (e.g., in-memory cluster support).

## 8. Development Constraints for `WhereIsProcess`
To maintain stability and scalability in large-scale clusters, any changes to the `whereis_process` must adhere to these restrictions:
- **Connectivity Control**: Full-mesh connectivity (every node communicating with every other node) is strictly prohibited. The connection scope must be restricted to prevent excessive resource consumption in large clusters.
- **Broadcast Storm Prevention**: Highly avoid broadcast storms during information synchronization. Large-scale information transmission or synchronization must be carefully designed.
- **Churn Management**: Avoid concentrated broadcast storms during node online/offline (join/leave) events.
- **Performance**: Seek to improve process location efficiency only while satisfying the above constraints. Calling `registrar.Nodes` is considered efficient and does not have performance issues.
