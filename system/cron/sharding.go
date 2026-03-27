package cron

import (
	"strconv"

	"ergo.services/ergo/gen"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type ringMember string

func (m ringMember) String() string {
	return string(m)
}

func makeRing(members ...consistent.Member) *consistent.Consistent {
	return consistent.New(members, consistent.Config{
		PartitionCount:    10240,
		ReplicationFactor: 40,
		Load:              1.2,
		Hasher:            hasher{},
	})
}

func ShardFor(key string, shardCount uint32) uint32 {
	if shardCount == 0 {
		return 0
	}
	return uint32(xxhash.Sum64String(key) % uint64(shardCount))
}

func shardToken(shard uint32) []byte {
	return []byte("cron-shard:" + strconv.FormatUint(uint64(shard), 10))
}

func shardOwner(ring *consistent.Consistent, shard uint32) gen.Atom {
	member := ring.LocateKey(shardToken(shard))
	if member == nil {
		return ""
	}
	return gen.Atom(member.String())
}
