package cron

import (
	"testing"

	"ergo.services/ergo/gen"
)

func TestShardForZeroShardCount(t *testing.T) {
	if got := ShardFor("job-1", 0); got != 0 {
		t.Fatalf("expected shard 0 for zero shard count, got %d", got)
	}
}

func TestShardOwnerReturnsTaggedNodeOrMemberString(t *testing.T) {
	ring := makeRing(ringMember{id: "member-a", node: gen.Atom("node-a")})
	if got := shardOwner(ring, 1); got != "node-a" {
		t.Fatalf("expected tagged node, got %s", got)
	}

	ring = makeRing(stringMember("node-b"))
	if got := shardOwner(ring, 1); got != "node-b" {
		t.Fatalf("expected member string fallback, got %s", got)
	}
}

type stringMember string

func (m stringMember) String() string {
	return string(m)
}
