package app

import (
	"errors"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	"github.com/qjpcpu/ergo-extensions/system"
)

func TestCallerSendLocalRemoteAndUnknown(t *testing.T) {
	localNode := gen.Atom("node-a@localhost")
	remoteNode := gen.Atom("node-b@localhost")

	t.Run("local", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: localNode}
		caller := NewCaller(process)

		if err := caller.Send("worker", "ping"); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		if process.sendTo != gen.Atom("worker") || process.sendMessage != "ping" {
			t.Fatalf("unexpected local send: to=%#v msg=%#v", process.sendTo, process.sendMessage)
		}
		if process.importantTo != nil {
			t.Fatalf("local send should not use SendImportant: %#v", process.importantTo)
		}
	})

	t.Run("remote", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: remoteNode}
		caller := NewCaller(process)

		if err := caller.Send("worker", "ping"); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		want := gen.ProcessID{Node: remoteNode, Name: "worker"}
		if process.importantTo != want || process.importantMessage != "ping" {
			t.Fatalf("unexpected important send: to=%#v msg=%#v", process.importantTo, process.importantMessage)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: gen.Atom("")}
		caller := NewCaller(process)

		if err := caller.Send("worker", "ping"); !errors.Is(err, gen.ErrProcessUnknown) {
			t.Fatalf("expected ErrProcessUnknown, got %v", err)
		}
	})

	t.Run("locate error", func(t *testing.T) {
		wantErr := errors.New("locate failed")
		process := &callerProcessStub{nodeName: localNode, locateErr: wantErr}
		caller := NewCaller(process)

		if err := caller.Send("worker", "ping"); !errors.Is(err, wantErr) {
			t.Fatalf("expected locate error, got %v", err)
		}
	})
}

func TestCallerCallLocalRemoteAndUnknown(t *testing.T) {
	localNode := gen.Atom("node-a@localhost")
	remoteNode := gen.Atom("node-b@localhost")

	t.Run("local", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: localNode, callResponse: "pong"}
		caller := NewCaller(process)

		res, err := caller.Call("worker", "ping")
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}
		if res != "pong" || process.callTo != gen.Atom("worker") || process.callMessage != "ping" {
			t.Fatalf("unexpected local call: res=%#v to=%#v msg=%#v", res, process.callTo, process.callMessage)
		}
	})

	t.Run("remote", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: remoteNode, callResponse: "pong"}
		caller := NewCaller(process)

		res, err := caller.Call("worker", "ping")
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}
		want := gen.ProcessID{Node: remoteNode, Name: "worker"}
		if res != "pong" || process.callTo != want || process.callMessage != "ping" {
			t.Fatalf("unexpected remote call: res=%#v to=%#v msg=%#v", res, process.callTo, process.callMessage)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode, locateResponse: struct{}{}}
		caller := NewCaller(process)

		res, err := caller.Call("worker", "ping")
		if res != nil || !errors.Is(err, gen.ErrProcessUnknown) {
			t.Fatalf("expected ErrProcessUnknown, got res=%#v err=%v", res, err)
		}
	})

	t.Run("locate error", func(t *testing.T) {
		wantErr := errors.New("locate failed")
		process := &callerProcessStub{nodeName: localNode, locateErr: wantErr}
		caller := NewCaller(process)

		res, err := caller.Call("worker", "ping")
		if res != nil || !errors.Is(err, wantErr) {
			t.Fatalf("expected locate error, got res=%#v err=%v", res, err)
		}
	})
}

func TestRouteActorSendToNodeLocalAndRemote(t *testing.T) {
	self := gen.Atom("node-a@localhost")
	remote := gen.Atom("node-b@localhost")
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return newRouteActor(system.NewAddressBook(), newRouteHintCache(0))
	}, unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn route actor: %v", err)
	}
	route := actor.Behavior().(*routeActor)
	actor.ClearEvents()

	if err := route.sendToNode("worker", self, "local"); err != nil {
		t.Fatalf("local send failed: %v", err)
	}
	actor.ShouldSend().
		To(gen.Atom("worker")).
		Message("local").
		Once().
		Assert()

	actor.ClearEvents()
	if err := route.sendToNode("worker", remote, "remote"); err != nil {
		t.Fatalf("remote send failed: %v", err)
	}
	if !hasImportantRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "remote") {
		t.Fatalf("expected important remote send, events=%#v", actor.Events())
	}
}

func TestRouteActorForwardSendUsesCacheAndBook(t *testing.T) {
	self := gen.Atom("node-a@localhost")
	remote := gen.Atom("node-b@localhost")
	book := system.NewAddressBook()
	if err := book.SetAvailableNodes(system.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set nodes: %v", err)
	}
	if err := book.AddProcess(remote, system.ProcessInfo{Name: "worker", Node: remote}); err != nil {
		t.Fatalf("add process: %v", err)
	}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return newRouteActor(book, newRouteHintCache(0))
	}, unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn route actor: %v", err)
	}
	route := actor.Behavior().(*routeActor)

	if err := route.forwardSend("worker", "", "first"); err != nil {
		t.Fatalf("forward send failed: %v", err)
	}
	if !hasImportantRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "first") {
		t.Fatalf("expected remote forward send, events=%#v", actor.Events())
	}

	actor.ClearEvents()
	if err := route.forwardSend("worker", "", "cached"); err != nil {
		t.Fatalf("cached forward send failed: %v", err)
	}
	if !hasImportantRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "cached") {
		t.Fatalf("expected cached remote forward send, events=%#v", actor.Events())
	}

	if err := route.forwardSend("missing", "", "nope"); !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("expected ErrProcessUnknown, got %v", err)
	}
}

func hasImportantRouteSend(actor *unit.TestActor, to gen.ProcessID, message any) bool {
	for _, event := range actor.Events() {
		send, ok := event.(unit.SendEvent)
		if !ok || !send.Important || send.To != to {
			continue
		}
		if send.Message == message {
			return true
		}
	}
	return false
}

type callerProcessStub struct {
	gen.Process

	nodeName       gen.Atom
	locateResponse any
	locateErr      error
	callResponse   any
	callErr        error

	callTo      any
	callMessage any

	sendTo      any
	sendMessage any

	importantTo      any
	importantMessage any
}

func (p *callerProcessStub) Node() gen.Node {
	return callerNodeStub{name: p.nodeName}
}

func (p *callerProcessStub) Call(to any, message any) (any, error) {
	if to == system.WhereIsProcess {
		return p.locateResponse, p.locateErr
	}
	p.callTo = to
	p.callMessage = message
	return p.callResponse, p.callErr
}

func (p *callerProcessStub) Send(to any, message any) error {
	p.sendTo = to
	p.sendMessage = message
	return nil
}

func (p *callerProcessStub) SendImportant(to any, message any) error {
	p.importantTo = to
	p.importantMessage = message
	return nil
}

type callerNodeStub struct {
	gen.Node
	name gen.Atom
}

func (n callerNodeStub) Name() gen.Atom {
	return n.name
}
