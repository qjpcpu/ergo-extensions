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

`ActorRoutePersistence` provides `OpenSession`, `RenewSession`, `CloseSession`, `ReadRoute`, `AcquireRoute`, and `ReleaseRoute`. Each node owns one session; each route stores the session and full PID with an independent TTL, defaulting to 24 hours. Acquisition and release compare the exact owner atomically. The storage interface is backend independent.

One heartbeat renews each node session. Bounded workers handle route operations and retry pending releases, while a shared timing wheel schedules route expiration. Lookup and takeover both check route/session validity and directly query `registrar.Nodes()` for remote owners. Local owners use node liveness. The topology snapshot continues to serve placement.

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

Route acquisition precedes business Init; release follows business Terminate. Session loss stops all managed actors on that node; route expiration stops its actor. An owner absent from the registrar can be replaced before its session expires, allowing overlap with the old actor until its local deadline. See the repository README for the persistence contract, shutdown lifecycle, and execution limits.
