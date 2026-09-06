package app

import (
	"context"
	"errors"
	"testing"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type actorLocatorStub struct {
	routes map[gen.Atom]gen.PID
	err    error
}

func (l actorLocatorStub) Locate(_ context.Context, key gen.Atom) (gen.PID, bool, error) {
	if l.err != nil {
		return gen.PID{}, false, l.err
	}
	pid, found := l.routes[key]
	return pid, found, nil
}

func TestCallerSend(t *testing.T) {
	local, remote := gen.Atom("node-a@localhost"), gen.Atom("node-b@localhost")
	lookupErr, sendErr := errors.New("lookup failed"), errors.New("send failed")
	for _, important := range []bool{false, true} {
		method := "Send"
		if important {
			method = "SendImportant"
		}
		t.Run(method, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				node      gen.Atom
				lookupErr error
				sendErr   error
				wantErr   error
			}{
				{name: "local", node: local},
				{name: "remote", node: remote},
				{name: "unknown", wantErr: gen.ErrProcessUnknown},
				{name: "lookup failure", lookupErr: lookupErr, wantErr: lookupErr},
				{name: "send failure", node: remote, sendErr: sendErr, wantErr: sendErr},
			} {
				t.Run(tc.name, func(t *testing.T) {
					pid := gen.PID{Node: tc.node, ID: 1, Creation: 1}
					locator := actorLocatorStub{err: tc.lookupErr}
					if tc.node != "" {
						locator.routes = map[gen.Atom]gen.PID{"worker": pid}
					}
					process := &callerProcessStub{nodeName: local, sendErr: tc.sendErr}
					caller := NewCaller(process, locator)
					send := caller.Send
					if important {
						send = caller.SendImportant
					}
					if err := send("worker", "ping"); !errors.Is(err, tc.wantErr) {
						t.Fatalf("send error: got %v, want %v", err, tc.wantErr)
					}
					if tc.node == "" {
						if process.sendTo != nil || process.importantTo != nil {
							t.Fatal("sent without a resolved route")
						}
						return
					}
					to, message := process.sendTo, process.sendMessage
					if important {
						to, message = process.importantTo, process.importantMessage
					}
					if to != pid || message != "ping" {
						t.Fatalf("unexpected delivery: to=%v message=%v", to, message)
					}
				})
			}
		})
	}
}

func TestCallerCallLocalRemoteAndUnknown(t *testing.T) {
	localNode := gen.Atom("node-a@localhost")
	remoteNode := gen.Atom("node-b@localhost")

	t.Run("local", func(t *testing.T) {
		pid := gen.PID{Node: localNode, ID: 1, Creation: 1}
		process := &callerProcessStub{nodeName: localNode, callResponse: "pong"}
		caller := NewCaller(process, actorLocatorStub{routes: map[gen.Atom]gen.PID{"worker": pid}})

		res, err := caller.Call("worker", "ping")
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}
		if res != "pong" || process.callTo != pid || process.callMessage != "ping" {
			t.Fatalf("unexpected local call: res=%#v to=%#v msg=%#v", res, process.callTo, process.callMessage)
		}
	})

	t.Run("remote", func(t *testing.T) {
		pid := gen.PID{Node: remoteNode, ID: 2, Creation: 1}
		process := &callerProcessStub{nodeName: localNode, callResponse: "pong"}
		caller := NewCaller(process, actorLocatorStub{routes: map[gen.Atom]gen.PID{"worker": pid}})

		res, err := caller.Call("worker", "ping")
		if err != nil {
			t.Fatalf("Call failed: %v", err)
		}
		if res != "pong" || process.callTo != pid || process.callMessage != "ping" {
			t.Fatalf("unexpected remote call: res=%#v to=%#v msg=%#v", res, process.callTo, process.callMessage)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		process := &callerProcessStub{nodeName: localNode}
		caller := NewCaller(process, actorLocatorStub{})

		res, err := caller.Call("worker", "ping")
		if res != nil || !errors.Is(err, gen.ErrProcessUnknown) {
			t.Fatalf("expected ErrProcessUnknown, got res=%#v err=%v", res, err)
		}
	})

	t.Run("locate error", func(t *testing.T) {
		wantErr := errors.New("locate failed")
		process := &callerProcessStub{nodeName: localNode}
		caller := NewCaller(process, actorLocatorStub{err: wantErr})

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
		return newRouteActor(nil)
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
		return newRouteActor(nil)
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

func TestRouteActorForwardSendUsesLocator(t *testing.T) {
	self := gen.Atom("node-a@localhost")
	remote := gen.Atom("node-b@localhost")
	remotePID := gen.PID{Node: remote, ID: 2, Creation: 1}
	locator := actorLocatorStub{routes: map[gen.Atom]gen.PID{"worker": remotePID}}
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return newRouteActor(locator)
	}, unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn route actor: %v", err)
	}
	route := actor.Behavior().(*routeActor)

	if err := route.forwardSend("worker", "", "first", false); err != nil {
		t.Fatalf("forward send failed: %v", err)
	}
	if !hasRouteSend(actor, remotePID, "first", false) {
		t.Fatalf("expected remote forward send, events=%#v", actor.Events())
	}

	actor.ClearEvents()
	if err := route.forwardSend("worker", "", "important", true); err != nil {
		t.Fatalf("important forward send failed: %v", err)
	}
	if !hasRouteSend(actor, remotePID, "important", true) {
		t.Fatalf("expected important send to use located PID, events=%#v", actor.Events())
	}

	if err := route.forwardSend("missing", "", "nope", false); !errors.Is(err, gen.ErrProcessUnknown) {
		t.Fatalf("expected ErrProcessUnknown, got %v", err)
	}
}

func TestRouteActorImportantCallPreservesTimeout(t *testing.T) {
	actor, err := unit.Spawn(t, func() gen.ProcessBehavior {
		return newRouteActor(nil)
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

	nodeName     gen.Atom
	callResponse any
	callErr      error

	callTo      any
	callMessage any

	sendTo      any
	sendMessage any
	sendErr     error

	importantTo      any
	importantMessage any
}

func (p *callerProcessStub) Node() gen.Node {
	return callerNodeStub{name: p.nodeName}
}

func (p *callerProcessStub) Call(to any, message any) (any, error) {
	p.callTo = to
	p.callMessage = message
	return p.callResponse, p.callErr
}

func (p *callerProcessStub) Send(to any, message any) error {
	p.sendTo = to
	p.sendMessage = message
	return p.sendErr
}

func (p *callerProcessStub) SendImportant(to any, message any) error {
	p.importantTo = to
	p.importantMessage = message
	return p.sendErr
}

type callerNodeStub struct {
	gen.Node
	name gen.Atom
}

func (n callerNodeStub) Name() gen.Atom {
	return n.name
}
