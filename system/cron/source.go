package cron

import (
	"context"
	"sort"
	"strconv"
	"sync"
)

type StaticSource struct {
	mu          sync.RWMutex
	jobsByShard map[uint32][]JobSpec
}

func NewStaticSource(shardCount uint32, jobs ...JobSpec) *StaticSource {
	s := &StaticSource{
		jobsByShard: make(map[uint32][]JobSpec),
	}
	for _, job := range jobs {
		s.Upsert(shardCount, job)
	}
	return s
}

func (s *StaticSource) Upsert(shardCount uint32, job JobSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job.ID == "" {
		job.ID = job.ShardKey
	}
	if job.ShardKey == "" {
		job.ShardKey = job.ID
	}
	shard := ShardFor(job.ShardKey, shardCount)

	list := s.jobsByShard[shard]
	for i := range list {
		if list[i].ID == job.ID {
			list[i] = job
			s.jobsByShard[shard] = list
			return
		}
	}
	s.jobsByShard[shard] = append(list, job)
}

func (s *StaticSource) ScanShards(ctx context.Context, req ScanShardsRequest) (ScanShardsResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	select {
	case <-ctx.Done():
		return ScanShardsResult{}, ctx.Err()
	default:
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 1000
	}
	offset, _ := strconv.Atoi(req.Cursor)
	var jobs []JobSpec
	for _, shard := range req.Shards {
		jobs = append(jobs, s.jobsByShard[shard]...)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })

	if offset >= len(jobs) {
		return ScanShardsResult{Done: true}, nil
	}
	end := offset + limit
	if end > len(jobs) {
		end = len(jobs)
	}
	result := ScanShardsResult{
		Jobs: jobs[offset:end],
		Done: end == len(jobs),
	}
	if !result.Done {
		result.NextCursor = strconv.Itoa(end)
	}
	return result, nil
}

func (s *StaticSource) Watch(ctx context.Context, req WatchRequest) (<-chan JobDeltaBatch, error) {
	_ = ctx
	_ = req
	return nil, nil
}

func (s *StaticSource) SupportsWatch() bool {
	return false
}
