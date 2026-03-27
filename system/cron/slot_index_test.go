package cron

import (
	"testing"
)

func TestSlotIndex_PutAndTake(t *testing.T) {
	index := newSlotIndex()

	// Put a job in a slot
	index.Put("job-1", 100)
	index.Put("job-2", 100)
	index.Put("job-3", 200)

	// Take all jobs from slot 100
	jobs := index.Take(100)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	// Check that slot 100 is now empty
	jobs = index.Take(100)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(jobs))
	}

	// Check that slot 200 still has job-3
	jobs = index.Take(200)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0] != "job-3" {
		t.Fatalf("expected job-3, got %s", jobs[0])
	}
}

func TestSlotIndex_Remove(t *testing.T) {
	index := newSlotIndex()

	index.Put("job-1", 100)
	index.Put("job-2", 100)
	index.Put("job-3", 200)

	// Remove job-1 from slot 100
	index.Remove("job-1", 100)

	// Take remaining jobs from slot 100
	jobs := index.Take(100)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0] != "job-2" {
		t.Fatalf("expected job-2, got %s", jobs[0])
	}
}

func TestSlotIndex_RemoveJob(t *testing.T) {
	index := newSlotIndex()

	index.Put("job-1", 100)
	index.Put("job-2", 200)

	// Remove job-1 without knowing its slot
	index.RemoveJob("job-1")

	// Check that job-1 is gone from all slots
	jobs := index.Take(100)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs in slot 100, got %d", len(jobs))
	}

	// Check that job-2 is still there
	jobs = index.Take(200)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in slot 200, got %d", len(jobs))
	}
}

func TestSlotIndex_Update(t *testing.T) {
	index := newSlotIndex()

	// Put job-1 in slot 100
	index.Put("job-1", 100)

	// Move job-1 to slot 200
	index.Put("job-1", 200)

	// Slot 100 should be empty
	jobs := index.Take(100)
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs in slot 100, got %d", len(jobs))
	}

	// Slot 200 should have job-1
	jobs = index.Take(200)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in slot 200, got %d", len(jobs))
	}
	if jobs[0] != "job-1" {
		t.Fatalf("expected job-1, got %s", jobs[0])
	}
}

func TestSlotIndex_RemoveNonExistent(t *testing.T) {
	index := newSlotIndex()

	// Should not panic
	index.Remove("non-existent", 100)
	index.RemoveJob("non-existent")
}

func TestSlotIndex_MultipleSlots(t *testing.T) {
	index := newSlotIndex()

	// Put many jobs in different slots
	for i := 0; i < 100; i++ {
		slot := int64(i % 10)
		jobID := "job-" + string(rune('0'+byte(i%10))) + "-" + string(rune('a'+byte(i/10)))
		index.Put(jobID, slot)
	}

	// Take all jobs from each slot
	total := 0
	for slot := range 10 {
		jobs := index.Take(int64(slot))
		total += len(jobs)
	}

	if total != 100 {
		t.Fatalf("expected 100 jobs total, got %d", total)
	}
}
