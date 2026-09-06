package cron

import (
	"context"
	"errors"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

func TestWatchActorInitFailures(t *testing.T) {
	wantErr := errors.New("watch failed")
	actor := &watchActor{
		provider: watchableProvider{
			watch: func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error) {
				return nil, wantErr
			},
		},
	}
	if err := actor.Init(); !errors.Is(err, wantErr) {
		t.Fatalf("expected watch error, got %v", err)
	}

	actor = &watchActor{provider: watchableProvider{}}
	if err := actor.Init(); err != gen.TerminateReasonNormal {
		t.Fatalf("expected normal termination for nil watch channel, got %v", err)
	}
}

func TestWatchActorDrainsBatchesAndTerminatesOnClosedChannel(t *testing.T) {
	parent := gen.PID{Node: "node-a@127.0.0.1", ID: 42}
	ch := make(chan JobDeltaBatch, 1)
	ch <- JobDeltaBatch{Cursor: "next", Deltas: []JobDelta{{Type: JobDeltaDelete, JobID: "job-1"}}}

	actor, err := unit.Spawn(t, newWatchFactory(parent, watchableProvider{
		watch: func(context.Context, WatchRequest) (<-chan JobDeltaBatch, error) {
			return ch, nil
		},
	}, 3, 9, WatchRequest{Shards: []uint32{3}, Since: "cursor"}))
	if err != nil {
		t.Fatalf("spawn watch actor: %v", err)
	}
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, messageWatchPoll{})
	actor.ShouldSend().
		To(parent).
		MessageMatching(func(message any) bool {
			msg, ok := message.(messageWatchBatch)
			return ok &&
				msg.shard == 3 &&
				msg.generation == 9 &&
				msg.batch.Cursor == "next" &&
				len(msg.batch.Deltas) == 1 &&
				msg.batch.Deltas[0].JobID == "job-1"
		}).
		Once().
		Assert()

	close(ch)
	if err := actor.Behavior().(*watchActor).drain(); err != gen.TerminateReasonNormal {
		t.Fatalf("expected normal termination after closed channel, got %v", err)
	}
}

func TestWatchActorNoopHandlersAndTerminate(t *testing.T) {
	cancelled := false
	actor := &watchActor{cancel: func() { cancelled = true }}

	if err := actor.HandleMessage(gen.PID{}, struct{}{}); err != nil {
		t.Fatalf("unexpected HandleMessage error: %v", err)
	}
	if response, err := actor.HandleCall(gen.PID{}, gen.Ref{}, struct{}{}); response != nil || err != gen.ErrUnsupported {
		t.Fatalf("expected unsupported call, got response=%v err=%v", response, err)
	}
	if err := actor.HandleEvent(gen.MessageEvent{}); err != nil {
		t.Fatalf("unexpected HandleEvent error: %v", err)
	}
	if got := actor.HandleInspect(gen.PID{}); got != nil {
		t.Fatalf("expected nil inspect, got %#v", got)
	}
	if err := actor.HandleLog(gen.MessageLog{}); err != nil {
		t.Fatalf("unexpected HandleLog error: %v", err)
	}
	actor.Terminate(gen.TerminateReasonShutdown)
	if !cancelled {
		t.Fatal("expected terminate to call cancel")
	}
}
