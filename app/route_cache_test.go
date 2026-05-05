package app

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestRouteHintCacheHitExpireAndInvalidate(t *testing.T) {
	cache := newRouteHintCache(0)
	now := time.Unix(100, 0)

	cache.set(gen.Atom("worker.A"), gen.Atom("node1@localhost"), now)

	node, ok := cache.get(gen.Atom("worker.A"), now.Add(time.Second))
	if !ok {
		t.Fatal("expected cache hit")
	}
	if node != gen.Atom("node1@localhost") {
		t.Fatalf("expected node1@localhost, got %s", node)
	}

	if _, ok := cache.get(gen.Atom("worker.A"), now.Add(3*time.Second)); ok {
		t.Fatal("expected cache entry to expire")
	}

	cache.set(gen.Atom("worker.A"), gen.Atom("node2@localhost"), now)
	cache.invalidate(gen.Atom("worker.A"))
	if _, ok := cache.get(gen.Atom("worker.A"), now.Add(time.Second)); ok {
		t.Fatal("expected invalidated cache entry to disappear")
	}
}

func TestRouteHintCacheTouchCapsAtMaxTTL(t *testing.T) {
	cache := &routeHintCache{
		ttl:    2 * time.Second,
		maxTTL: 5 * time.Second,
	}
	now := time.Unix(100, 0)

	cache.set(gen.Atom("worker.A"), gen.Atom("node1@localhost"), now)
	cache.touch(gen.Atom("worker.A"), gen.Atom("node1@localhost"), now.Add(1*time.Second))
	hint, ok := cache.load(gen.Atom("worker.A"))
	if !ok {
		t.Fatal("expected cache entry")
	}
	if got := hint.expiresAt.Sub(now); got != 3*time.Second {
		t.Fatalf("expected expiry at +3s, got %s", got)
	}

	cache.touch(gen.Atom("worker.A"), gen.Atom("node1@localhost"), now.Add(4*time.Second))
	hint, ok = cache.load(gen.Atom("worker.A"))
	if !ok {
		t.Fatal("expected cache entry")
	}
	if got := hint.expiresAt.Sub(now); got != 5*time.Second {
		t.Fatalf("expected expiry capped at +5s, got %s", got)
	}
}
