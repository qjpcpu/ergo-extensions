package app

import (
	"sync"
	"time"

	"ergo.services/ergo/gen"
)

type routeHint struct {
	node      gen.Atom
	createdAt time.Time
	expiresAt time.Time
}

type routeHintCache struct {
	ttl     time.Duration
	maxTTL  time.Duration
	entries sync.Map
}

func newRouteHintCache(syncInterval time.Duration) *routeHintCache {
	ttl := 2 * time.Second
	if syncInterval > 0 {
		half := syncInterval / 2
		if half > 0 && half < ttl {
			ttl = half
		}
	}
	if ttl <= 0 {
		ttl = time.Second
	}
	maxTTL := 10 * time.Second
	if maxTTL < ttl {
		maxTTL = ttl
	}
	return &routeHintCache{
		ttl:    ttl,
		maxTTL: maxTTL,
	}
}

func (c *routeHintCache) get(name gen.Atom, now time.Time) (gen.Atom, bool) {
	if c == nil {
		return "", false
	}
	hint, ok := c.load(name)
	if !ok {
		return "", false
	}
	if !hint.expiresAt.IsZero() && !now.Before(hint.expiresAt) {
		c.entries.Delete(name)
		return "", false
	}
	return hint.node, true
}

func (c *routeHintCache) set(name, node gen.Atom, now time.Time) {
	if c == nil || name == "" || node == "" {
		return
	}
	c.entries.Store(name, routeHint{
		node:      node,
		createdAt: now,
		expiresAt: now.Add(c.ttl),
	})
}

func (c *routeHintCache) touch(name, node gen.Atom, now time.Time) {
	if c == nil || name == "" || node == "" {
		return
	}
	hint, ok := c.load(name)
	if !ok || hint.node != node {
		c.entries.Store(name, routeHint{
			node:      node,
			createdAt: now,
			expiresAt: now.Add(c.ttl),
		})
		return
	}
	expireAt := now.Add(c.ttl)
	maxExpireAt := hint.createdAt.Add(c.maxTTL)
	if expireAt.After(maxExpireAt) {
		expireAt = maxExpireAt
	}
	hint.expiresAt = expireAt
	if hint.createdAt.IsZero() {
		hint.createdAt = now
	}
	c.entries.Store(name, hint)
}

func (c *routeHintCache) invalidate(name gen.Atom) {
	if c == nil || name == "" {
		return
	}
	c.entries.Delete(name)
}

func (c *routeHintCache) load(name gen.Atom) (routeHint, bool) {
	if c == nil {
		return routeHint{}, false
	}
	value, ok := c.entries.Load(name)
	if !ok {
		return routeHint{}, false
	}
	hint, ok := value.(routeHint)
	if !ok {
		return routeHint{}, false
	}
	return hint, true
}
