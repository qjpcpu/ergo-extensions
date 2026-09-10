package cron

import (
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/check"
	"ergo.services/ergo/testing/unit"
)

type triggerTestProc struct{ act.Actor }

func spawnTriggerTestProcess(t *testing.T) *unit.Subject {
	t.Helper()
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior { return &triggerTestProc{} }, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn trigger test process: %v", err)
	}
	actor.OnSend(gen.Atom("fail")).Fail(gen.ErrProcessUnknown)
	return actor
}

func TestTriggerBatchBug(t *testing.T) {
	trigger := LocalTrigger{Batch: true}
	actor := spawnTriggerTestProcess(t)

	jobs := []DispatchJob{
		{JobID: "1", TriggerProcess: "fail", ScheduledAt: time.Now()},
		{JobID: "2", TriggerProcess: "success", ScheduledAt: time.Now()},
	}

	failed, err := trigger.Fire(actor.Behavior().(gen.Process), jobs)
	if err == nil {
		t.Fatalf("expected batch failure")
	}
	if len(failed) != 1 || failed[0].JobID != "1" {
		t.Fatalf("unexpected failed jobs: %+v", failed)
	}

	actor.ShouldSend().
		To(gen.Atom("success")).
		Where(func(record check.Send) bool {
			message := record.Message
			msg, ok := message.(MessageTrigger)
			return ok && msg.JobID == "2"
		}).
		Once().
		Assert()
}

func TestTriggerNoBatchBug(t *testing.T) {
	trigger := LocalTrigger{Batch: false}
	actor := spawnTriggerTestProcess(t)

	jobs := []DispatchJob{
		{JobID: "1", TriggerProcess: "fail", ScheduledAt: time.Now()},
		{JobID: "2", TriggerProcess: "success", ScheduledAt: time.Now()},
	}

	failed, err := trigger.Fire(actor.Behavior().(gen.Process), jobs)
	if err == nil {
		t.Fatalf("expected non-batch failure")
	}
	if len(failed) != 1 || failed[0].JobID != "1" {
		t.Fatalf("unexpected failed jobs: %+v", failed)
	}

	actor.ShouldSend().
		To(gen.Atom("success")).
		Where(func(record check.Send) bool {
			message := record.Message
			msg, ok := message.(MessageTrigger)
			return ok && msg.JobID == "2"
		}).
		Once().
		Assert()
}
