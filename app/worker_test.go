package app

import (
	"errors"
	"testing"
	"time"

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

	if err := route.sendToNode("worker", self, "local", false); err != nil {
		t.Fatalf("local send failed: %v", err)
	}
	actor.ShouldSend().
		To(gen.Atom("worker")).
		Message("local").
		Once().
		Assert()

	actor.ClearEvents()
	if err := route.sendToNode("worker", remote, "remote", false); err != nil {
		t.Fatalf("remote send failed: %v", err)
	}
	if !hasRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "remote", false) {
		t.Fatalf("expected regular remote send, events=%#v", actor.Events())
	}

	actor.ClearEvents()
	if err := route.sendToNode("worker", remote, "important", true); err != nil {
		t.Fatalf("important remote send failed: %v", err)
	}
	if !hasRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "important", true) {
		t.Fatalf("expected important remote send, events=%#v", actor.Events())
	}
}

func TestRouteActorSendToPIDLocalAndRemote(t *testing.T) {
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

	localPID := gen.PID{Node: self, ID: 1, Creation: 1}
	if err := route.sendToPID(localPID, "local", false); err != nil {
		t.Fatalf("local PID send failed: %v", err)
	}
	actor.ShouldSend().
		To(localPID).
		Message("local").
		Once().
		Assert()

	actor.ClearEvents()
	remotePID := gen.PID{Node: remote, ID: 2, Creation: 1}
	if err := route.sendToPID(remotePID, "remote", false); err != nil {
		t.Fatalf("remote PID send failed: %v", err)
	}
	if !hasRouteSend(actor, remotePID, "remote", false) {
		t.Fatalf("expected regular remote PID send, events=%#v", actor.Events())
	}

	actor.ClearEvents()
	if err := route.sendToPID(remotePID, "important", true); err != nil {
		t.Fatalf("important remote PID send failed: %v", err)
	}
	if !hasRouteSend(actor, remotePID, "important", true) {
		t.Fatalf("expected important remote PID send, events=%#v", actor.Events())
	}
}

func TestRouteActorForwardSendUsesBookForRegularSend(t *testing.T) {
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

	if err := route.forwardSend("worker", "", "first", false); err != nil {
		t.Fatalf("forward send failed: %v", err)
	}
	if !hasRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "first", false) {
		t.Fatalf("expected remote forward send, events=%#v", actor.Events())
	}

	route.hints.set("worker", self, time.Now())
	actor.ClearEvents()
	if err := route.forwardSend("worker", "", "stale-hint", false); err != nil {
		t.Fatalf("forward send with stale hint failed: %v", err)
	}
	if !hasRouteSend(actor, gen.ProcessID{Node: remote, Name: "worker"}, "stale-hint", false) {
		t.Fatalf("expected regular send to ignore stale hint, events=%#v", actor.Events())
	}

	if err := route.forwardSend("missing", "", "nope", false); !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("expected ErrProcessUnknown, got %v", err)
	}
}

func TestRouteActorForwardImportantSendUsesCache(t *testing.T) {
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

	route.hints.set("worker", self, time.Now())
	if err := route.forwardSend("worker", "", "important-cached", true); err != nil {
		t.Fatalf("important cached forward send failed: %v", err)
	}
	if !hasRouteSend(actor, gen.Atom("worker"), "important-cached", true) {
		t.Fatalf("expected important send to use cached node, events=%#v", actor.Events())
	}
}

func TestRouteActorImportantCallPreservesTimeout(t *testing.T) {
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return newRouteActor(system.NewAddressBook(), newRouteHintCache(0))
	}, unit.WithNodeName("node-a@localhost"))
	if err != nil {
		t.Fatalf("spawn route actor: %v", err)
	}
	route := actor.Behavior().(*routeActor)
	actor.ClearEvents()

	to := gen.ProcessID{Node: "node-b@localhost", Name: "worker"}
	if _, err := route.callImportantWithTimeout(to, "ping", 7); err != nil {
		t.Fatalf("important call failed: %v", err)
	}
	if route.ImportantDelivery() {
		t.Fatal("important delivery flag was not restored")
	}
	if !hasRouteCallWithTimeout(actor, to, "ping", 7) {
		t.Fatalf("expected call with timeout, events=%#v", actor.Events())
	}
}

func hasRouteSend(actor *unit.TestActor, to any, message any, important bool) bool {
	for _, event := range actor.Events() {
		send, ok := event.(unit.SendEvent)
		if !ok || send.Important != important || send.To != to {
			continue
		}
		if send.Message == message {
			return true
		}
	}
	return false
}

func hasRouteCallWithTimeout(actor *unit.TestActor, to any, request any, timeout int) bool {
	for _, event := range actor.Events() {
		call, ok := event.(unit.CallEvent)
		if !ok || call.To != to {
			continue
		}
		if call.Request == request && call.Timeout == timeout {
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
