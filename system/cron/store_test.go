package cron

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func TestShardRuntime_Upsert(t *testing.T) {
runtime := newShardRuntime(1, 1)
	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	job, err := compileJob(JobSpec{
		ID:             "test-job",
		Schedule:       "*/5 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	err = runtime.Upsert(job, base, resolution)
	if err != nil {
		t.Fatalf("upsert job: %v", err)
	}

	if len(runtime.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(runtime.jobs))
	}

	// Upsert should update existing job
	job.Spec.Schedule = "*/10 * * * *"
	newJob, err := compileJob(job.Spec)
	if err != nil {
		t.Fatalf("compile updated job: %v", err)
	}

	err = runtime.Upsert(newJob, base, resolution)
	if err != nil {
		t.Fatalf("upsert updated job: %v", err)
	}

	if len(runtime.jobs) != 1 {
		t.Fatalf("expected 1 job after update, got %d", len(runtime.jobs))
	}
}

func TestShardRuntime_Delete(t *testing.T) {
	runtime := newShardRuntime(1, 1)
	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	job, err := compileJob(JobSpec{
		ID:             "test-job",
		Schedule:       "*/5 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	runtime.Upsert(job, base, resolution)
	runtime.Delete("test-job")

	if len(runtime.jobs) != 0 {
		t.Fatalf("expected 0 jobs after delete, got %d", len(runtime.jobs))
	}
}

func TestShardRuntime_TakeDue(t *testing.T) {
	runtime := newShardRuntime(1, 1)
	runtime.Activate()

	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	// Job that triggers at 10:05
	job1, err := compileJob(JobSpec{
		ID:             "job-1",
		Schedule:       "5 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	// Job that triggers at 10:10
	job2, err := compileJob(JobSpec{
		ID:             "job-2",
		Schedule:       "10 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	runtime.Upsert(job1, base, resolution)
	runtime.Upsert(job2, base, resolution)

	// Take jobs due at 10:05
	slot := slotKey(base.Add(5*time.Minute), resolution)
	jobs := runtime.TakeDue(slot)

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job at 10:05, got %d", len(jobs))
	}
	if jobs[0].Spec.ID != "job-1" {
		t.Fatalf("expected job-1, got %s", jobs[0].Spec.ID)
	}

	// Slot should be empty after take
	jobs = runtime.TakeDue(slot)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs after take, got %d", len(jobs))
	}

	// Take jobs due at 10:10
	slot = slotKey(base.Add(10*time.Minute), resolution)
	jobs = runtime.TakeDue(slot)

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job at 10:10, got %d", len(jobs))
	}
	if jobs[0].Spec.ID != "job-2" {
		t.Fatalf("expected job-2, got %s", jobs[0].Spec.ID)
	}
}

func TestShardRuntime_Reschedule(t *testing.T) {
	runtime := newShardRuntime(1, 1)
	runtime.Activate()

	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	job, err := compileJob(JobSpec{
		ID:             "test-job",
		Schedule:       "*/5 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	runtime.Upsert(job, base, resolution)

	// Take job at 10:00 (since base=10:00, Upsert uses base.Add(-resolution)=09:59, Next returns 10:00)
	slot := slotKey(base, resolution)
	jobs := runtime.TakeDue(slot)

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	// Reschedule after 10:00, next should be 10:05
	runtime.Reschedule(jobs[0], base, resolution)

	slot = slotKey(base.Add(5*time.Minute), resolution)
	jobs = runtime.TakeDue(slot)

	if len(jobs) != 1 {
		t.Fatalf("expected 1 job after reschedule, got %d", len(jobs))
	}
}

func TestShardRuntime_Inactive(t *testing.T) {
	runtime := newShardRuntime(1, 1)
	// Don't call Activate(), so state is Loading

	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	job, err := compileJob(JobSpec{
		ID:             "test-job",
		Schedule:       "*/5 * * * *",
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	runtime.Upsert(job, base, resolution)

	// TakeDue should return no jobs when inactive
	slot := slotKey(base.Add(5*time.Minute), resolution)
	jobs := runtime.TakeDue(slot)

	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs when inactive, got %d", len(jobs))
	}
}

func TestShardRuntime_UpsertExpired(t *testing.T) {
	runtime := newShardRuntime(1, 1)
	runtime.Activate()

	// Base time is 2026-03-27 10:00:00
	base := time.Date(2026, 3, 27, 10, 0, 0, 0, time.UTC)
	resolution := time.Minute

	// Job with schedule that has no more triggers (e.g., specific past time)
	job, err := compileJob(JobSpec{
		ID:             "test-job",
		Schedule:       "0 9 26 3 *", // March 26 at 9:00 AM (past)
		TriggerProcess: gen.Atom("test_proc"),
	})
	if err != nil {
		t.Fatalf("compile job: %v", err)
	}

	err = runtime.Upsert(job, base, resolution)
	if err != nil {
		t.Fatalf("upsert job: %v", err)
	}

	// Job should be in the map but not scheduled in any slot
	if len(runtime.jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(runtime.jobs))
	}

	// No slots should have this job
	for slot := range 100 {
		jobs := runtime.TakeDue(int64(slot))
		if len(jobs) != 0 {
			t.Fatalf("expected 0 jobs in slot %d, got %d", slot, len(jobs))
		}
	}
}
