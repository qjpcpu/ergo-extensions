package cron

import (
	"time"

	"ergo.services/ergo/gen"
)

type shardState uint8

const (
	shardStateLoading shardState = iota + 1
	shardStateActive
)

type shardRuntime struct {
	id             uint32
	generation     int64
	state          shardState
	lease          ShardLease
	checkpoint     int64
	jobs           map[string]*CompiledJob
	slots          *slotIndex
	cursor         string
	watchPID       gen.PID
	loadedAt       time.Time
	pendingBySlot  map[int64]int
	completedSlots map[int64]struct{}
}

func newShardRuntime(id uint32, generation int64) *shardRuntime {
	return &shardRuntime{
		id:             id,
		generation:     generation,
		state:          shardStateLoading,
		jobs:           make(map[string]*CompiledJob),
		slots:          newSlotIndex(),
		loadedAt:       time.Now().UTC(),
		pendingBySlot:  make(map[int64]int),
		completedSlots: make(map[int64]struct{}),
	}
}

func (r *shardRuntime) Activate() {
	r.state = shardStateActive
}

func (r *shardRuntime) Upsert(job *CompiledJob, base time.Time, resolution time.Duration) error {
	r.jobs[job.Spec.ID] = job
	next, ok := job.Schedule.Next(base.Add(-resolution))
	if !ok {
		r.slots.RemoveJob(job.Spec.ID)
		return nil
	}
	r.slots.Put(job.Spec.ID, slotKey(next, resolution))
	return nil
}

func (r *shardRuntime) Delete(jobID string) {
	delete(r.jobs, jobID)
	r.slots.RemoveJob(jobID)
}

func (r *shardRuntime) TakeDue(slot int64) []*CompiledJob {
	ids := r.slots.Take(slot)
	if len(ids) == 0 {
		return nil
	}
	jobs := make([]*CompiledJob, 0, len(ids))
	for _, jobID := range ids {
		if job, ok := r.jobs[jobID]; ok {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func (r *shardRuntime) Reschedule(job *CompiledJob, after time.Time, resolution time.Duration) {
	next, ok := job.Schedule.Next(after)
	if !ok {
		return
	}
	r.slots.Put(job.Spec.ID, slotKey(next, resolution))
}

func (r *shardRuntime) DueJobsAt(slotTime time.Time, resolution time.Duration) []*CompiledJob {
	slot := slotKey(slotTime, resolution)
	jobs := make([]*CompiledJob, 0)
	for _, job := range r.jobs {
		if !job.Schedule.IsDueAt(slotTime) {
			continue
		}
		if nextSlot, ok := r.slots.nextSlotByJob[job.Spec.ID]; ok && nextSlot == slot {
			r.slots.RemoveJob(job.Spec.ID)
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func (r *shardRuntime) MarkSlotClaimed(slot int64, pending int) {
	if pending > 0 {
		r.pendingBySlot[slot] = pending
		delete(r.completedSlots, slot)
		return
	}
	delete(r.pendingBySlot, slot)
	r.completedSlots[slot] = struct{}{}
}

func (r *shardRuntime) MarkSlotAcked(slot int64) {
	remaining := r.pendingBySlot[slot] - 1
	if remaining > 0 {
		r.pendingBySlot[slot] = remaining
		return
	}
	delete(r.pendingBySlot, slot)
	r.completedSlots[slot] = struct{}{}
}
