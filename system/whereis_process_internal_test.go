package system

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"ergo.services/ergo/gen"
)

func findRemoteDirectoryProcess(t *testing.T, book *AddressBook, self gen.Atom) gen.Atom {
	t.Helper()
	for i := 0; i < 1024; i++ {
		name := gen.Atom(fmt.Sprintf("proc.remote.%d", i))
		owner := book.PickDirectoryNode(name)
		if owner != "" && owner != self {
			return name
		}
	}
	t.Fatal("failed to find a process name owned by a remote directory node")
	return ""
}

func TestRegisterToShardsSendFailureDoesNotScheduleTopologyDebounce(t *testing.T) {
	book := NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)

	var sendCalls int
	w := &whereis{
		book:             book,
		selfNode:         self,
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		nowFn:            func() time.Time { return time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC) },
		logSendFailureFn: func(gen.Atom, string, error) {},
		sendProcessChanged: func(pid gen.ProcessID, msg MessageProcessChanged) error {
			sendCalls++
			return errors.New("boom")
		},
	}
	w.topologyChangeID = 7

	w.registerToShards(MessageProcessChanged{
		Node:      self,
		UpProcess: []ProcessInfo{{Name: name, Node: self}},
		Version:   NewVersion(),
	})

	if sendCalls != 1 {
		t.Fatalf("expected one failed send, got %d", sendCalls)
	}
	if w.topologyChangeID != 7 {
		t.Fatalf("registerToShards should not schedule topology debounce on send failure")
	}
}

func TestSyncDirectoryShardsSendFailureDoesNotScheduleTopologyDebounce(t *testing.T) {
	book := NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)

	var sendCalls int
	w := &whereis{
		book:             book,
		selfNode:         self,
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		nowFn:            func() time.Time { return time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC) },
		logSendFailureFn: func(gen.Atom, string, error) {},
		sendProcessChanged: func(pid gen.ProcessID, msg MessageProcessChanged) error {
			sendCalls++
			return errors.New("boom")
		},
	}
	w.topologyChangeID = 11
	w.selfVersion = NewVersion()

	w.syncDirectoryShards(ProcessInfoList{{Name: name, Node: self}})

	if sendCalls != 1 {
		t.Fatalf("expected one failed full sync send, got %d", sendCalls)
	}
	if w.topologyChangeID != 11 {
		t.Fatalf("syncDirectoryShards should not schedule topology debounce on send failure")
	}
}

func TestWhereisShouldLogSendFailureThrottlesPerOwner(t *testing.T) {
	owner := gen.Atom("node-b@127.0.0.1")
	w := &whereis{
		sendFailureLogAt: make(map[gen.Atom]time.Time),
	}
	now := time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC)

	if !w.shouldLogSendFailure(owner, now) {
		t.Fatal("expected first failure to be logged")
	}
	if w.shouldLogSendFailure(owner, now.Add(10*time.Second)) {
		t.Fatal("expected second failure inside throttle window to be suppressed")
	}
	if !w.shouldLogSendFailure(owner, now.Add(31*time.Second)) {
		t.Fatal("expected failure after throttle window to be logged again")
	}
	w.clearSendFailure(owner)
	if !w.shouldLogSendFailure(owner, now.Add(32*time.Second)) {
		t.Fatal("expected cleared owner to log immediately")
	}
}
