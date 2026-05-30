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

func TestRouteHintCacheEdges(t *testing.T) {
	var nilCache *routeHintCache
	if node, ok := nilCache.get("worker.A", time.Now()); ok || node != "" {
		t.Fatalf("nil cache should miss, got node=%s ok=%v", node, ok)
	}
	nilCache.set("worker.A", "node1", time.Now())
	nilCache.touch("worker.A", "node1", time.Now())
	nilCache.invalidate("worker.A")
	if _, ok := nilCache.load("worker.A"); ok {
		t.Fatal("nil cache load should miss")
	}

	cache := newRouteHintCache(2 * time.Second)
	if cache.ttl != time.Second {
		t.Fatalf("expected sync interval to reduce TTL to half, got %s", cache.ttl)
	}
	now := time.Unix(100, 0)

	cache.set("", "node1", now)
	cache.set("worker.A", "", now)
	if _, ok := cache.load(""); ok {
		t.Fatal("empty name should not be stored")
	}
	if _, ok := cache.load("worker.A"); ok {
		t.Fatal("empty node should not be stored")
	}

	cache.touch("worker.A", "node1", now)
	hint, ok := cache.load("worker.A")
	if !ok || hint.node != "node1" {
		t.Fatalf("touch should create missing hint, got %#v ok=%v", hint, ok)
	}

	cache.touch("worker.A", "node2", now.Add(time.Second))
	hint, ok = cache.load("worker.A")
	if !ok || hint.node != "node2" {
		t.Fatalf("touch with a different node should replace hint, got %#v ok=%v", hint, ok)
	}

	cache.entries.Store(gen.Atom("bad"), "not-a-hint")
	if _, ok := cache.load("bad"); ok {
		t.Fatal("invalid stored value should not load")
	}
	cache.invalidate("")
}
