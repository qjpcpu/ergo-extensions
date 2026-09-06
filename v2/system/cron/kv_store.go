package cron

import (
	"context"
	"sync"
)

type kvItem struct {
	value   []byte
	version uint64
}

type MemoryKVStore struct {
	mu    sync.RWMutex
	items map[string]kvItem
}

func NewMemoryKVStore() *MemoryKVStore {
	return &MemoryKVStore{
		items: make(map[string]kvItem),
	}
}

func (s *MemoryKVStore) Get(ctx context.Context, key string) (KVEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	select {
	case <-ctx.Done():
		return KVEntry{}, ctx.Err()
	default:
	}

	item, ok := s.items[key]
	if !ok {
		return KVEntry{}, nil
	}
	value := append([]byte(nil), item.value...)
	return KVEntry{Value: value, Version: item.version, Found: true}, nil
}

func (s *MemoryKVStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	item := s.items[key]
	item.version++
	item.value = append([]byte(nil), value...)
	s.items[key] = item
	return nil
}

func (s *MemoryKVStore) PutIfAbsent(ctx context.Context, key string, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if _, ok := s.items[key]; ok {
		return false, nil
	}
	s.items[key] = kvItem{
		value:   append([]byte(nil), value...),
		version: 1,
	}
	return true, nil
}

func (s *MemoryKVStore) CompareAndSwap(ctx context.Context, key string, version uint64, value []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	item, ok := s.items[key]
	if !ok || item.version != version {
		return false, nil
	}
	item.version++
	item.value = append([]byte(nil), value...)
	s.items[key] = item
	return true, nil
}

func (s *MemoryKVStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	delete(s.items, key)
	return nil
}
