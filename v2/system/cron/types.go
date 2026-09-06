package cron

import (
	"context"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/net/edf"
)

const (
	ProcessName = gen.Atom("extensions_cron")
	LocationUTC = "UTC"
)

type JobSpec struct {
	ID             string
	ShardKey       string
	Schedule       string
	Location       string
	TriggerProcess gen.Atom
}

type ScanShardsRequest struct {
	Shards []uint32
	Cursor string
	Limit  int
}

type ScanShardsResult struct {
	Jobs       []JobSpec
	NextCursor string
	Done       bool
}

type WatchRequest struct {
	Shards []uint32
	Since  string
}

type JobDeltaType uint8

const (
	JobDeltaUpsert JobDeltaType = iota + 1
	JobDeltaDelete
)

type JobDelta struct {
	Type  JobDeltaType
	Job   JobSpec
	JobID string
}

type JobDeltaBatch struct {
	Shard  uint32
	Deltas []JobDelta
	Cursor string
}

type JobProvider interface {
	// ScanShards returns the current job snapshot for the requested shards.
	// Implementations should support cursor-based pagination and return a stable
	// view for the current scan session as much as possible.
	ScanShards(ctx context.Context, req ScanShardsRequest) (ScanShardsResult, error)
	// Watch streams incremental job changes for the requested shards starting
	// from the provided cursor. Returning a nil channel means watch is disabled.
	Watch(ctx context.Context, req WatchRequest) (<-chan JobDeltaBatch, error)
}

type KVEntry struct {
	Value   []byte
	Version uint64
	Found   bool
}

type KVStore interface {
	// Get loads a single key together with its version for later CAS updates.
	Get(ctx context.Context, key string) (KVEntry, error)
	// Put writes a value unconditionally and bumps the stored version.
	Put(ctx context.Context, key string, value []byte) error
	// PutIfAbsent writes a value only when the key does not exist.
	PutIfAbsent(ctx context.Context, key string, value []byte) (bool, error)
	// CompareAndSwap replaces the value only when the expected version matches.
	CompareAndSwap(ctx context.Context, key string, version uint64, value []byte) (bool, error)
	// Delete removes a key. Missing keys are treated as success.
	Delete(ctx context.Context, key string) error
}

type Source interface {
	Provider() JobProvider
	StateStore() KVStore
}

type ManagedSource struct {
	Jobs    JobProvider
	StateKV KVStore
}

func NewManagedSource(provider JobProvider, stateStore KVStore) *ManagedSource {
	return &ManagedSource{
		Jobs:    provider,
		StateKV: stateStore,
	}
}

func (s *ManagedSource) Provider() JobProvider {
	if s == nil {
		return nil
	}
	return s.Jobs
}

func (s *ManagedSource) StateStore() KVStore {
	if s == nil {
		return nil
	}
	return s.StateKV
}

type OptionalWatchSource interface {
	SupportsWatch() bool
}

type SchedulerOptions struct {
	ShardCount         uint32
	TickResolution     time.Duration
	ScanPageSize       int
	ScanConcurrency    int
	RebalanceDelay     time.Duration
	MisfireGracePeriod time.Duration
	MaxDispatchPerTick int
	EnableBatchTrigger bool
	InitDelay          time.Duration
	LeaseTTL           time.Duration
	OwnerRingSalt      string
}

func (o SchedulerOptions) WithDefaults() SchedulerOptions {
	if o.ShardCount == 0 {
		o.ShardCount = 4096
	}
	if o.TickResolution <= 0 {
		o.TickResolution = time.Minute
	}
	if o.ScanPageSize <= 0 {
		o.ScanPageSize = 1000
	}
	if o.ScanConcurrency <= 0 {
		o.ScanConcurrency = 8
	}
	if o.RebalanceDelay <= 0 {
		o.RebalanceDelay = 3 * time.Second
	}
	if o.MisfireGracePeriod <= 0 {
		o.MisfireGracePeriod = 2 * time.Minute
	}
	if o.MaxDispatchPerTick <= 0 {
		o.MaxDispatchPerTick = 1000
	}
	if o.InitDelay <= 0 {
		o.InitDelay = 100 * time.Millisecond
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 10 * time.Second
	}
	if o.OwnerRingSalt == "" {
		o.OwnerRingSalt = "extensions_cron_owner"
	}
	return o
}

type InspectRequest struct{}

type ShardLease struct {
	Shard     uint32
	Owner     gen.Atom
	Epoch     int64
	ExpiresAt time.Time
	Acquired  bool
}

type ShardCheckpoint struct {
	Shard uint32
	Slot  int64
	Valid bool
}

type DispatchClaim struct {
	JobID       string
	ScheduledAt time.Time
}

type DispatchState uint8

const (
	DispatchStatePending DispatchState = iota + 1
	DispatchStateAcked
)

type DispatchRecord struct {
	Key         string
	JobID       string
	ScheduledAt time.Time
	State       DispatchState
	Owner       gen.Atom
	Epoch       int64
}

type MessageTrigger struct {
	JobID       string
	ScheduledAt time.Time
	DispatchKey string
}

type MessageTriggerBatch struct {
	Jobs []MessageTrigger
}

func init() {
	for _, item := range []any{
		InspectRequest{},
		MessageTrigger{},
		MessageTriggerBatch{},
	} {
		err := edf.RegisterTypeOf(item)
		if err == nil || err == gen.ErrTaken {
			continue
		}
		panic(err)
	}
}
