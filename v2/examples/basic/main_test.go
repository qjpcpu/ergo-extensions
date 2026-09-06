package main

import (
	"context"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestMemoryRoutesLifecycleAndOwnership(t *testing.T) {
	store := &memoryRoutes{routes: make(map[gen.Atom]routeRecord)}
	ctx := context.Background()
	first := gen.PID{Node: "node-a", ID: 1, Creation: 1}
	second := gen.PID{Node: "node-b", ID: 2, Creation: 1}

	if acquired, err := store.Acquire(ctx, "worker", first, time.Minute); err != nil || !acquired {
		t.Fatalf("acquire: acquired=%v err=%v", acquired, err)
	}
	if acquired, err := store.Acquire(ctx, "worker", second, time.Minute); err != nil || acquired {
		t.Fatalf("conflicting acquire: acquired=%v err=%v", acquired, err)
	}
	if owned, err := store.Renew(ctx, "worker", second, time.Minute); err != nil || owned {
		t.Fatalf("wrong-owner renew: owned=%v err=%v", owned, err)
	}
	if owned, err := store.Renew(ctx, "worker", first, time.Minute); err != nil || !owned {
		t.Fatalf("renew: owned=%v err=%v", owned, err)
	}
	if pid, found, err := store.Lookup(ctx, "worker"); err != nil || !found || pid != first {
		t.Fatalf("lookup: pid=%s found=%v err=%v", pid, found, err)
	}
	if err := store.Release(ctx, "worker", second); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.Lookup(ctx, "worker"); !found {
		t.Fatal("wrong-owner release deleted the route")
	}
	if err := store.Release(ctx, "worker", first); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.Lookup(ctx, "worker"); found {
		t.Fatal("route still exists after release")
	}
}

func TestMemoryRoutesExpirationAndCanceledContext(t *testing.T) {
	store := &memoryRoutes{routes: make(map[gen.Atom]routeRecord)}
	pid := gen.PID{Node: "node-a", ID: 1, Creation: 1}
	if acquired, err := store.Acquire(context.Background(), "worker", pid, -time.Second); err != nil || !acquired {
		t.Fatal("failed to create expired route")
	}
	if _, found, err := store.Lookup(context.Background(), "worker"); err != nil || found {
		t.Fatalf("expired route was returned: found=%v err=%v", found, err)
	}
	if owned, err := store.Renew(context.Background(), "worker", pid, time.Minute); err != nil || owned {
		t.Fatalf("expired route was renewed: owned=%v err=%v", owned, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Acquire(ctx, "a", pid, time.Minute); err == nil {
		t.Fatal("Acquire ignored canceled context")
	}
	if _, err := store.Renew(ctx, "a", pid, time.Minute); err == nil {
		t.Fatal("Renew ignored canceled context")
	}
	if err := store.Release(ctx, "a", pid); err == nil {
		t.Fatal("Release ignored canceled context")
	}
	if _, _, err := store.Lookup(ctx, "a"); err == nil {
		t.Fatal("Lookup ignored canceled context")
	}
}

func TestEchoReturnsRequest(t *testing.T) {
	actor := &echo{}
	if got, err := actor.HandleCall(gen.PID{}, gen.Ref{}, "hello"); err != nil || got != "hello" {
		t.Fatalf("unexpected echo result: got=%v err=%v", got, err)
	}
}

func TestExampleRunsEndToEnd(t *testing.T) {
	main()
}
