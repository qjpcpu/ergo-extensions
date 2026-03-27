package cron

import (
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

type mockProcess struct {
	gen.Process
	sent map[gen.Atom]int
}

func (m *mockProcess) Send(to any, message any) error {
	target := to.(gen.Atom)
	if target == "fail" {
		return gen.ErrProcessUnknown
	}
	m.sent[target]++
	return nil
}

func TestTriggerBatchBug(t *testing.T) {
	trigger := LocalTrigger{Batch: true}
	p := &mockProcess{sent: make(map[gen.Atom]int)}

	jobs := []DispatchJob{
		{JobID: "1", TriggerProcess: "fail", ScheduledAt: time.Now()},
		{JobID: "2", TriggerProcess: "success", ScheduledAt: time.Now()},
	}

	failed, err := trigger.Fire(p, jobs)
	if err == nil {
		t.Fatalf("expected batch failure")
	}
	if len(failed) != 1 || failed[0].JobID != "1" {
		t.Fatalf("unexpected failed jobs: %+v", failed)
	}

	if p.sent["success"] != 1 {
		t.Errorf("expected success process to receive message")
	}
}

func TestTriggerNoBatchBug(t *testing.T) {
	trigger := LocalTrigger{Batch: false}
	p := &mockProcess{sent: make(map[gen.Atom]int)}

	jobs := []DispatchJob{
		{JobID: "1", TriggerProcess: "fail", ScheduledAt: time.Now()},
		{JobID: "2", TriggerProcess: "success", ScheduledAt: time.Now()},
	}

	failed, err := trigger.Fire(p, jobs)
	if err == nil {
		t.Fatalf("expected non-batch failure")
	}
	if len(failed) != 1 || failed[0].JobID != "1" {
		t.Fatalf("unexpected failed jobs: %+v", failed)
	}

	if p.sent["success"] != 1 {
		t.Errorf("expected success process to receive message")
	}
}
