package cron

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

type faultStateStore struct {
	*MemoryKVStore
	get    func(context.Context, string) (KVEntry, error)
	create func(context.Context, string, []byte) (bool, error)
	swap   func(context.Context, string, uint64, []byte) (bool, error)
}

func (s *faultStateStore) Get(c context.Context, k string) (KVEntry, error) {
	if s.get != nil {
		return s.get(c, k)
	}
	return s.MemoryKVStore.Get(c, k)
}
func (s *faultStateStore) PutIfAbsent(c context.Context, k string, v []byte) (bool, error) {
	if s.create != nil {
		return s.create(c, k, v)
	}
	return s.MemoryKVStore.PutIfAbsent(c, k, v)
}
func (s *faultStateStore) CompareAndSwap(c context.Context, k string, version uint64, v []byte) (bool, error) {
	if s.swap != nil {
		return s.swap(c, k, version, v)
	}
	return s.MemoryKVStore.CompareAndSwap(c, k, version, v)
}

func TestLeaseStorageFailuresPreserveOwnership(t *testing.T) {
	for _, stage := range []string{"read", "create", "renew", "takeover"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			s := &faultStateStore{MemoryKVStore: NewMemoryKVStore()}
			b := newStateBackend(s)
			owner := gen.Atom("owner")
			failure := errors.New("storage unavailable")
			if stage == "renew" || stage == "takeover" {
				ttl := time.Hour
				if stage == "takeover" {
					ttl = -time.Second
				}
				if _, err := b.AcquireShardLease(ctx, 2, owner, ttl); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := s.MemoryKVStore.Get(ctx, leaseKey(2))
			switch stage {
			case "read":
				s.get = func(context.Context, string) (KVEntry, error) { return KVEntry{}, failure }
			case "create":
				s.create = func(context.Context, string, []byte) (bool, error) { return false, failure }
			default:
				s.swap = func(context.Context, string, uint64, []byte) (bool, error) { return false, failure }
			}
			if stage == "takeover" {
				owner = "replacement"
			}
			lease, err := b.AcquireShardLease(ctx, 2, owner, time.Hour)
			if !errors.Is(err, failure) || lease.Acquired {
				t.Fatal("storage failure granted a lease", lease, err)
			}
			after, _ := s.MemoryKVStore.Get(ctx, leaseKey(2))
			if string(before.Value) != string(after.Value) || before.Found != after.Found {
				t.Fatal("failed lease operation changed ownership")
			}
		})
	}
}

func TestCheckpointStorageFailuresLeaveLastConfirmedSlot(t *testing.T) {
	for _, stage := range []string{"read", "create", "update", "corrupt"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			s := &faultStateStore{MemoryKVStore: NewMemoryKVStore()}
			b := newStateBackend(s)
			lease, err := b.AcquireShardLease(ctx, 1, "owner", time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if stage == "update" || stage == "corrupt" {
				if err := b.AdvanceShardCheckpoint(ctx, 1, "owner", lease.Epoch, 5); err != nil {
					t.Fatal(err)
				}
			}
			failure := errors.New("checkpoint unavailable")
			switch stage {
			case "read":
				s.get = func(c context.Context, k string) (KVEntry, error) {
					if k == checkpointKey(1) {
						return KVEntry{}, failure
					}
					return s.MemoryKVStore.Get(c, k)
				}
			case "create":
				s.create = func(context.Context, string, []byte) (bool, error) { return false, failure }
			case "update":
				s.swap = func(context.Context, string, uint64, []byte) (bool, error) { return false, failure }
			case "corrupt":
				if err := s.Put(ctx, checkpointKey(1), []byte("{")); err != nil {
					t.Fatal(err)
				}
			}
			err = b.AdvanceShardCheckpoint(ctx, 1, "owner", lease.Epoch, 6)
			if err == nil || (stage != "corrupt" && !errors.Is(err, failure)) {
				t.Fatal("checkpoint failure was hidden", err)
			}
			s.get = nil
			s.create = nil
			s.swap = nil
			if stage != "corrupt" {
				checkpoint, err := b.GetShardCheckpoint(ctx, 1)
				if err != nil || checkpoint.Slot > 5 {
					t.Fatal("failed checkpoint advanced", checkpoint, err)
				}
			}
		})
	}
}

func TestStateCASContentionHasBoundedRetries(t *testing.T) {
	for _, operation := range []string{"lease", "checkpoint"} {
		for _, conflicts := range []int{7, 8} {
			t.Run(fmt.Sprintf("%s/%d", operation, conflicts), func(t *testing.T) {
				ctx := context.Background()
				s := &faultStateStore{MemoryKVStore: NewMemoryKVStore()}
				b := newStateBackend(s)
				lease, err := b.AcquireShardLease(ctx, 1, "owner", time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				if err := b.AdvanceShardCheckpoint(ctx, 1, "owner", lease.Epoch, 5); err != nil {
					t.Fatal(err)
				}
				calls := 0
				s.swap = func(c context.Context, k string, version uint64, v []byte) (bool, error) {
					calls++
					if calls <= conflicts {
						old, err := s.MemoryKVStore.Get(c, k)
						if err != nil {
							return false, err
						}
						return false, s.MemoryKVStore.Put(c, k, old.Value)
					}
					return s.MemoryKVStore.CompareAndSwap(c, k, version, v)
				}
				if operation == "lease" {
					_, err = b.AcquireShardLease(ctx, 1, "owner", time.Hour)
				} else {
					err = b.AdvanceShardCheckpoint(ctx, 1, "owner", lease.Epoch, 6)
				}
				if calls != 8 {
					t.Fatalf("CAS calls=%d, want 8", calls)
				}
				if conflicts == 7 && err != nil {
					t.Fatal("last permitted attempt failed", err)
				}
				if conflicts == 8 && (err == nil || !strings.Contains(err.Error(), "retry budget")) {
					t.Fatal("contention did not exhaust budget", err)
				}
				checkpoint, e := b.GetShardCheckpoint(ctx, 1)
				if e != nil {
					t.Fatal(e)
				}
				want := int64(5)
				if operation == "checkpoint" && conflicts == 7 {
					want = 6
				}
				if checkpoint.Slot != want {
					t.Fatal("unexpected persisted checkpoint", checkpoint)
				}
			})
		}
	}
}

func TestConcurrentShardContendersHaveOneOwner(t *testing.T) {
	const contenders = 64
	store := NewMemoryKVStore()
	backend := newStateBackend(store)
	start := make(chan struct{})
	results := make(chan ShardLease, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			l, e := backend.AcquireShardLease(context.Background(), 7, gen.Atom(fmt.Sprintf("node-%d", id)), time.Hour)
			results <- l
			errs <- e
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	acquired := 0
	var owner gen.Atom
	for l := range results {
		if l.Acquired {
			acquired++
			owner = l.Owner
		}
	}
	if acquired != 1 {
		t.Fatalf("acquired owners=%d, want 1", acquired)
	}
	record, err := backend.ensureLease(context.Background(), 7, owner, 1)
	if err != nil || record.Owner != owner {
		t.Fatal("winning lease was not persisted", record, err)
	}
}
