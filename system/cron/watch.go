package cron

import (
	"context"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

const watchPollInterval = 100 * time.Millisecond

type messageWatchPoll struct{}

type watchActor struct {
	act.Actor

	parent     gen.PID
	provider   JobProvider
	request    WatchRequest
	shard      uint32
	generation int64
	ch         <-chan JobDeltaBatch
	cancel     context.CancelFunc
}

func newWatchFactory(parent gen.PID, provider JobProvider, shard uint32, generation int64, request WatchRequest) gen.ProcessFactory {
	return func() gen.ProcessBehavior {
		return &watchActor{
			parent:     parent,
			provider:   provider,
			request:    request,
			shard:      shard,
			generation: generation,
		}
	}
}

func (w *watchActor) Init(args ...any) error {
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := w.provider.Watch(ctx, w.request)
	if err != nil {
		cancel()
		return err
	}
	if ch == nil {
		cancel()
		return gen.TerminateReasonNormal
	}
	w.ch = ch
	w.cancel = cancel
	_, _ = w.SendAfter(w.PID(), messageWatchPoll{}, 0)
	return nil
}

func (w *watchActor) HandleMessage(from gen.PID, message any) error {
	switch message.(type) {
	case messageWatchPoll:
		return w.drain()
	}
	return nil
}

func (w *watchActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	return nil, gen.ErrUnsupported
}

func (w *watchActor) HandleEvent(event gen.MessageEvent) error {
	return nil
}

func (w *watchActor) HandleInspect(from gen.PID, item ...string) map[string]string {
	return nil
}

func (w *watchActor) HandleLog(message gen.MessageLog) error {
	return nil
}

func (w *watchActor) Terminate(reason error) {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *watchActor) drain() error {
	for {
		select {
		case batch, ok := <-w.ch:
			if !ok {
				return gen.TerminateReasonNormal
			}
			if err := w.Send(w.parent, messageWatchBatch{
				shard:      w.shard,
				generation: w.generation,
				batch:      batch,
			}); err != nil {
				return err
			}
		default:
			_, _ = w.SendAfter(w.PID(), messageWatchPoll{}, watchPollInterval)
			return nil
		}
	}
}
