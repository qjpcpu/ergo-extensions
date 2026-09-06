package app

import (
	"context"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"testing"
	"time"
)

type deadlineStore struct {
	*testRoutePersistence
	entered, release chan struct{}
}

func (s *deadlineStore) Lookup(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	if key == "slow" {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return gen.PID{}, false, ctx.Err()
		}
		key = "target"
	}
	return s.testRoutePersistence.Lookup(ctx, key)
}

type deadlineReceiver struct {
	act.Actor
	received chan any
}

func (a *deadlineReceiver) Init(...any) error                      { return nil }
func (a *deadlineReceiver) HandleMessage(_ gen.PID, msg any) error { a.received <- msg; return nil }

func TestForwardDeadlineIncludesQueueAndDiscardsExpiredWork(t *testing.T) {
	store := &deadlineStore{testRoutePersistence: newTestRoutePersistence(), entered: make(chan struct{}), release: make(chan struct{})}
	node, err := StartSimpleNode(SimpleNodeOptions{NodeName: "forward-deadline@localhost", Registrar: mem.Create(), ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	received := make(chan any, 4)
	_, err = node.Spawn(func() gen.ProcessBehavior {
		return node.ActorRoutes().WithActorRoute("target", &deadlineReceiver{received: received})
	}, gen.ProcessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- node.ForwardSend("slow", "first") }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("lookup did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = node.ForwardSend("target", "expired", ForwardContext(ctx))
	close(store.release)
	if err != gen.ErrTimeout {
		t.Fatal("queued call did not time out", err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := node.ForwardSend("target", "last"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"first", "last"} {
		select {
		case got := <-received:
			if got != want {
				t.Fatal(got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("missing message", want)
		}
	}
}

func TestForwardReturnsWhenNodeStops(t *testing.T) {
	store := &deadlineStore{testRoutePersistence: newTestRoutePersistence(), entered: make(chan struct{}), release: make(chan struct{})}
	node, err := StartSimpleNode(SimpleNodeOptions{NodeName: "forward-stop@localhost", Registrar: mem.Create(), ActorRoutePersistence: store, NodeForwardWorker: 1, LogLevel: gen.LogLevelDisabled})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- node.ForwardSend("slow", "message") }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("lookup did not start")
	}
	stopped := make(chan struct{})
	go func() { node.Stop(); close(stopped) }()
	select {
	case err := <-result:
		if err != gen.ErrNodeTerminated {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("forward did not return on shutdown")
	}
	close(store.release)
	select {
	case <-stopped:
	case <-time.After(6 * time.Second):
		t.Fatal("node did not stop")
	}
}
