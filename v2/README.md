# Ergo Extensions v2

Version 2 provides durable actor routing, registrar-backed membership, daemon recovery, and distributed cron scheduling for Ergo applications.

## Install

```bash
go get github.com/qjpcpu/ergo-extensions/v2@latest
```

## Core flow

Applications supply a `gen.Registrar` and implement `system.ActorRoutePersistence`. `StartSimpleNode` creates and owns one `ActorRouter` per node; its `ActorRoutes` facade provides lookup and decorates only actors that need global location:

```go
node, err := app.StartSimpleNode(app.SimpleNodeOptions{
    NodeName:              "node-1@example.net",
    ActorRoutePersistence: routeStore,
    ActorRouterOptions:    system.ActorRouterOptions{},
    Registrar:             registrar,
})
if err != nil {
    return err
}
defer node.Stop()

routes := node.ActorRoutes()

key := gen.Atom("tenant/42/worker")
pid, err := node.Spawn(func() gen.ProcessBehavior {
	return routes.WithActorRoute(key, &worker{})
}, gen.ProcessOptions{})
if err != nil {
    return err
}

located, found, err := routes.Locate(context.Background(), key)
```

`ActorRoutePersistence` has five operations: `Acquire`, `Replace`, `Renew`, `Release`, and `Lookup`. Ownership changes must compare the exact key and PID atomically. The module does not import or assume Redis, MySQL, or another storage implementation.

Each routed behavior owns a leased route. A router-wide sharded timing wheel, separate bounded renewal and high-priority release queues, and a fixed worker pool manage all local leases; there is no companion actor or per-route timer. Background renewal and release panics are isolated per operation. Lookup reads persistence directly, then uses AddressBook's local membership snapshot to reject an offline PID. It does not scan actors, broadcast, query the registrar on the hot path, or enqueue lookup work in a system actor.

See the repository [README](../README.md) for the full persistence contract, forwarding helpers, daemon and cron examples, operational guidance, and the v1 migration table. A runnable local example is in [examples/basic/main.go](examples/basic/main.go), and the internal design is described in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Public entry points

- Actor routes: `node.ActorRoutes().Locate`, `WithActorRoute`, `WithSupervisorRoute`, `WithPoolRoute`, `ActorRoutePersistence`.
- Topology: `node.Topology().GetAvailableNodes`, `NodesVersion`, `PickNode`, `PickCoordinatorNode`.
- Node bootstrap and forwarding: `app.StartSimpleNode`, `ForwardSend`, `ForwardCall`, `ForwardSpawn`.
- Business actor messaging: `app.NewCaller`, `Caller.Send`, `Caller.SendImportant`, `Caller.Call`. `Send` uses the process delivery settings; `SendImportant` explicitly confirms mailbox delivery, not business completion.
- Daemons: `system.RegisterLauncher`, `system.NewSpawner` for custom bootstrap, `system.SingletonDaemon`.
- Cron: `cron.JobSpec`, `cron.NewManagedSource`, `cron.SchedulerOptions`.

## License

MIT License. See [LICENSE](LICENSE).

Route acquisition precedes business Init; release follows business Terminate. An owner absent from the registrar can be replaced atomically before its route TTL expires. Add `Replace` to existing persistence implementations when upgrading. Forwarding has an end-to-end deadline, and `ActorRoutes().Stats()` exposes lease scheduling and failure statistics. See the repository README for the full contract and shutdown budgets.
