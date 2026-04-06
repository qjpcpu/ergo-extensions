package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ergo.services/ergo/gen"
)

type stateBackend struct {
	store KVStore
}

type leaseRecord struct {
	Owner     gen.Atom  `json:"owner"`
	Epoch     int64     `json:"epoch"`
	ExpiresAt time.Time `json:"expires_at"`
}

type checkpointRecord struct {
	Slot int64 `json:"slot"`
}

type dispatchRecord struct {
	Key         string    `json:"key"`
	JobID       string    `json:"job_id"`
	ScheduledAt time.Time `json:"scheduled_at"`
	State       uint8     `json:"state"`
	Owner       gen.Atom  `json:"owner"`
	Epoch       int64     `json:"epoch"`
}

func newStateBackend(store KVStore) *stateBackend {
	if store == nil {
		return nil
	}
	return &stateBackend{store: store}
}

func (b *stateBackend) AcquireShardLease(ctx context.Context, shard uint32, owner gen.Atom, ttl time.Duration) (ShardLease, error) {
	key := leaseKey(shard)
	now := time.Now().UTC()
	for i := 0; i < 8; i++ {
		entry, err := b.store.Get(ctx, key)
		if err != nil {
			return ShardLease{}, err
		}
		if !entry.Found {
			record := leaseRecord{
				Owner:     owner,
				Epoch:     1,
				ExpiresAt: now.Add(ttl),
			}
			data, err := marshalState(record)
			if err != nil {
				return ShardLease{}, err
			}
			created, err := b.store.PutIfAbsent(ctx, key, data)
			if err != nil {
				return ShardLease{}, err
			}
			if created {
				return ShardLease{Shard: shard, Owner: owner, Epoch: 1, ExpiresAt: record.ExpiresAt, Acquired: true}, nil
			}
			continue
		}

		record, err := unmarshalLease(entry.Value)
		if err != nil {
			return ShardLease{}, err
		}
		switch {
		case record.Owner == owner:
			record.ExpiresAt = now.Add(ttl)
			data, err := marshalState(record)
			if err != nil {
				return ShardLease{}, err
			}
			swapped, err := b.store.CompareAndSwap(ctx, key, entry.Version, data)
			if err != nil {
				return ShardLease{}, err
			}
			if swapped {
				return ShardLease{Shard: shard, Owner: owner, Epoch: record.Epoch, ExpiresAt: record.ExpiresAt, Acquired: true}, nil
			}
		case !record.ExpiresAt.After(now):
			record.Owner = owner
			record.Epoch++
			record.ExpiresAt = now.Add(ttl)
			data, err := marshalState(record)
			if err != nil {
				return ShardLease{}, err
			}
			swapped, err := b.store.CompareAndSwap(ctx, key, entry.Version, data)
			if err != nil {
				return ShardLease{}, err
			}
			if swapped {
				return ShardLease{Shard: shard, Owner: owner, Epoch: record.Epoch, ExpiresAt: record.ExpiresAt, Acquired: true}, nil
			}
		default:
			return ShardLease{Shard: shard, Owner: record.Owner, Epoch: record.Epoch, ExpiresAt: record.ExpiresAt, Acquired: false}, nil
		}
	}
	return ShardLease{}, fmt.Errorf("acquire lease for shard %d exceeded retry budget", shard)
}

func (b *stateBackend) GetShardCheckpoint(ctx context.Context, shard uint32) (ShardCheckpoint, error) {
	entry, err := b.store.Get(ctx, checkpointKey(shard))
	if err != nil {
		return ShardCheckpoint{}, err
	}
	if !entry.Found {
		return ShardCheckpoint{Shard: shard}, nil
	}
	record, err := unmarshalCheckpoint(entry.Value)
	if err != nil {
		return ShardCheckpoint{}, err
	}
	return ShardCheckpoint{Shard: shard, Slot: record.Slot, Valid: true}, nil
}

func (b *stateBackend) AdvanceShardCheckpoint(ctx context.Context, shard uint32, owner gen.Atom, epoch int64, slot int64) error {
	if _, err := b.ensureLease(ctx, shard, owner, epoch); err != nil {
		return err
	}
	key := checkpointKey(shard)
	for i := 0; i < 8; i++ {
		entry, err := b.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if !entry.Found {
			data, err := marshalState(checkpointRecord{Slot: slot})
			if err != nil {
				return err
			}
			created, err := b.store.PutIfAbsent(ctx, key, data)
			if err != nil {
				return err
			}
			if created {
				return nil
			}
			continue
		}
		record, err := unmarshalCheckpoint(entry.Value)
		if err != nil {
			return err
		}
		if slot <= record.Slot {
			return nil
		}
		record.Slot = slot
		data, err := marshalState(record)
		if err != nil {
			return err
		}
		swapped, err := b.store.CompareAndSwap(ctx, key, entry.Version, data)
		if err != nil {
			return err
		}
		if swapped {
			return nil
		}
	}
	return fmt.Errorf("advance checkpoint for shard %d exceeded retry budget", shard)
}

func (b *stateBackend) ClaimDispatches(ctx context.Context, shard uint32, owner gen.Atom, epoch int64, dispatches []DispatchClaim) ([]DispatchRecord, error) {
	if _, err := b.ensureLease(ctx, shard, owner, epoch); err != nil {
		return nil, err
	}
	records := make([]DispatchRecord, 0, len(dispatches))
	for _, claim := range dispatches {
		key := dispatchKey(shard, claim.JobID, claim.ScheduledAt)
		data, err := marshalState(dispatchRecord{
			Key:         key,
			JobID:       claim.JobID,
			ScheduledAt: claim.ScheduledAt.UTC(),
			State:       uint8(DispatchStatePending),
			Owner:       owner,
			Epoch:       epoch,
		})
		if err != nil {
			return nil, err
		}
		created, err := b.store.PutIfAbsent(ctx, dispatchStateKey(shard, key), data)
		if err != nil {
			return nil, err
		}
		if created {
			records = append(records, DispatchRecord{
				Key:         key,
				JobID:       claim.JobID,
				ScheduledAt: claim.ScheduledAt.UTC(),
				State:       DispatchStatePending,
				Owner:       owner,
				Epoch:       epoch,
			})
			continue
		}
		entry, err := b.store.Get(ctx, dispatchStateKey(shard, key))
		if err != nil {
			return nil, err
		}
		if !entry.Found {
			records = append(records, DispatchRecord{
				Key:         key,
				JobID:       claim.JobID,
				ScheduledAt: claim.ScheduledAt.UTC(),
				State:       DispatchStatePending,
				Owner:       owner,
				Epoch:       epoch,
			})
			continue
		}
		record, err := unmarshalDispatch(entry.Value)
		if err != nil {
			return nil, err
		}
		records = append(records, DispatchRecord{
			Key:         record.Key,
			JobID:       record.JobID,
			ScheduledAt: record.ScheduledAt,
			State:       DispatchState(record.State),
			Owner:       record.Owner,
			Epoch:       record.Epoch,
		})
	}
	return records, nil
}

func (b *stateBackend) AckDispatches(ctx context.Context, shard uint32, owner gen.Atom, epoch int64, keys []string) error {
	if _, err := b.ensureLease(ctx, shard, owner, epoch); err != nil {
		return err
	}
	for _, key := range keys {
		stateKey := dispatchStateKey(shard, key)
		for i := 0; i < 8; i++ {
			entry, err := b.store.Get(ctx, stateKey)
			if err != nil {
				return err
			}
			if !entry.Found {
				break
			}
			record, err := unmarshalDispatch(entry.Value)
			if err != nil {
				return err
			}
			record.State = uint8(DispatchStateAcked)
			record.Owner = owner
			record.Epoch = epoch
			data, err := marshalState(record)
			if err != nil {
				return err
			}
			swapped, err := b.store.CompareAndSwap(ctx, stateKey, entry.Version, data)
			if err != nil {
				return err
			}
			if swapped {
				break
			}
		}
	}
	return nil
}

func (b *stateBackend) ensureLease(ctx context.Context, shard uint32, owner gen.Atom, epoch int64) (leaseRecord, error) {
	entry, err := b.store.Get(ctx, leaseKey(shard))
	if err != nil {
		return leaseRecord{}, err
	}
	if !entry.Found {
		return leaseRecord{}, fmt.Errorf("missing shard lease %d", shard)
	}
	record, err := unmarshalLease(entry.Value)
	if err != nil {
		return leaseRecord{}, err
	}
	now := time.Now().UTC()
	if record.Owner != owner || record.Epoch != epoch || !record.ExpiresAt.After(now) {
		return leaseRecord{}, fmt.Errorf("stale shard lease %d", shard)
	}
	return record, nil
}

func leaseKey(shard uint32) string {
	return fmt.Sprintf("cron/lease/%d", shard)
}

func checkpointKey(shard uint32) string {
	return fmt.Sprintf("cron/checkpoint/%d", shard)
}

func dispatchStateKey(shard uint32, key string) string {
	return fmt.Sprintf("cron/dispatch/%d/%s", shard, key)
}

func dispatchKey(shard uint32, jobID string, scheduledAt time.Time) string {
	return fmt.Sprintf("%d:%s:%d", shard, jobID, scheduledAt.UTC().UnixNano())
}

func marshalState(v any) ([]byte, error) {
	return json.Marshal(v)
}

func unmarshalLease(data []byte) (leaseRecord, error) {
	var record leaseRecord
	err := json.Unmarshal(data, &record)
	return record, err
}

func unmarshalCheckpoint(data []byte) (checkpointRecord, error) {
	var record checkpointRecord
	err := json.Unmarshal(data, &record)
	return record, err
}

func unmarshalDispatch(data []byte) (dispatchRecord, error) {
	var record dispatchRecord
	err := json.Unmarshal(data, &record)
	return record, err
}
