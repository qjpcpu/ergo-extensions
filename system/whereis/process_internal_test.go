package whereis

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
)

type failingRegistrar struct {
	nodesErr error
}

func (r *failingRegistrar) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	return gen.StaticRoutes{}, nil
}

func (r *failingRegistrar) Resolver() gen.Resolver {
	return nil
}

func (r *failingRegistrar) RegisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}

func (r *failingRegistrar) UnregisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}

func (r *failingRegistrar) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}

func (r *failingRegistrar) UnregisterApplicationRoute(name gen.Atom) error {
	return gen.ErrUnsupported
}

func (r *failingRegistrar) Nodes() ([]gen.Atom, error) {
	return nil, r.nodesErr
}

func (r *failingRegistrar) Config(items ...string) (map[string]any, error) {
	return nil, gen.ErrUnsupported
}

func (r *failingRegistrar) ConfigItem(item string) (any, error) {
	return nil, gen.ErrUnsupported
}

func (r *failingRegistrar) Event() (gen.Event, error) {
	return gen.Event{}, gen.ErrUnsupported
}

func (r *failingRegistrar) Info() gen.RegistrarInfo {
	return gen.RegistrarInfo{}
}

func (r *failingRegistrar) Terminate() {}

func (r *failingRegistrar) Version() gen.Version {
	return gen.Version{}
}

func findRemoteDirectoryProcess(t *testing.T, book *core.AddressBook, self gen.Atom) gen.Atom {
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
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
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
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			return errors.New("boom")
		},
	}
	w.topologyChangeID = 7

	w.registerToShards(core.MessageProcessChanged{
		Node:      self,
		UpProcess: []core.ProcessInfo{{Name: name, Node: self}},
		Version:   core.NewVersion(),
	})

	if sendCalls != 1 {
		t.Fatalf("expected one failed send, got %d", sendCalls)
	}
	if w.topologyChangeID != 7 {
		t.Fatalf("registerToShards should not schedule topology debounce on send failure")
	}
}

func TestSyncDirectoryShardsSendFailureDoesNotScheduleTopologyDebounce(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
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
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			return errors.New("boom")
		},
	}
	w.topologyChangeID = 11
	w.selfVersion = core.NewVersion()

	w.syncDirectoryShards(core.ProcessInfoList{{Name: name, Node: self}})

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

func TestWhereisFetchAvailableBookNodesReturnsRegistrarError(t *testing.T) {
	wantErr := errors.New("nodes unavailable")
	book := core.NewAddressBook()
	w := &whereis{
		book:      book,
		selfNode:  gen.Atom("node-a@127.0.0.1"),
		registrar: &failingRegistrar{nodesErr: wantErr},
	}

	nodes, err := w.fetchAvailableBookNodes()
	if err == nil {
		t.Fatal("expected error from registrar nodes")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
	if nodes != nil {
		t.Fatalf("expected nil nodes, got %#v", nodes)
	}
	if got := book.GetAvailableNodes().Len(); got != 0 {
		t.Fatalf("expected empty available nodes cache, got %d", got)
	}
}

func TestRegisterLocalProcessFastPathSendsOnlyOwnerShard(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)

	var sendCalls int
	var sentTo gen.ProcessID
	var sentMsg core.MessageProcessChanged
	w := &whereis{
		book:             book,
		selfNode:         self,
		nameToPID:        make(map[gen.Atom]gen.PID),
		nameToBirthAt:    make(map[gen.Atom]int64),
		pidToName:        make(map[gen.PID]gen.Atom),
		processCache:     core.NewAtomicValue[core.ProcessInfoList](),
		selfVersion:      core.NewVersion(),
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		nowFn:            func() time.Time { return time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC) },
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			sentTo = pid
			sentMsg = msg
			return nil
		},
	}

	if err := w.registerLocalProcess(core.MessageRegisterLocalProcess{Name: name, BirthAt: 123}); err != nil {
		t.Fatalf("register local process: %v", err)
	}
	if sendCalls != 1 {
		t.Fatalf("expected one shard message, got %d", sendCalls)
	}
	if sentTo.Node != remote || sentTo.Name != ProcessName {
		t.Fatalf("unexpected shard target: %+v", sentTo)
	}
	if len(sentMsg.UpProcess) != 1 || sentMsg.UpProcess[0].Name != name || sentMsg.Node != self {
		t.Fatalf("unexpected shard message: %+v", sentMsg)
	}
	if node, ok := book.LocateLocal(name); !ok || node != self {
		t.Fatalf("expected local book to locate %s on %s, got %s ok=%v", name, self, node, ok)
	}
}

func TestUnregisterLocalProcessFastPathSendsOnlyOwnerShard(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)
	pid := gen.PID{Node: self, ID: 10}
	birthAt := int64(123)
	if err := book.AddProcess(self, core.ProcessInfo{Name: name, PID: pid, Node: self, BirthAt: birthAt}); err != nil {
		t.Fatalf("add process: %v", err)
	}

	var sendCalls int
	var sentTo gen.ProcessID
	var sentMsg core.MessageProcessChanged
	w := &whereis{
		book:             book,
		selfNode:         self,
		nameToPID:        map[gen.Atom]gen.PID{name: pid},
		nameToBirthAt:    map[gen.Atom]int64{name: birthAt},
		pidToName:        map[gen.PID]gen.Atom{pid: name},
		processCache:     core.NewAtomicValue[core.ProcessInfoList](),
		selfVersion:      core.NewVersion(),
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		nowFn:            func() time.Time { return time.Date(2026, 3, 27, 20, 0, 0, 0, time.UTC) },
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			sentTo = pid
			sentMsg = msg
			return nil
		},
	}

	if err := w.unregisterLocalProcess(core.MessageUnregisterLocalProcess{Name: name, PID: pid}); err != nil {
		t.Fatalf("unregister local process: %v", err)
	}
	if sendCalls != 1 {
		t.Fatalf("expected one shard message, got %d", sendCalls)
	}
	if sentTo.Node != remote || sentTo.Name != ProcessName {
		t.Fatalf("unexpected shard target: %+v", sentTo)
	}
	if len(sentMsg.DownProcess) != 1 || sentMsg.DownProcess[0].Name != name || sentMsg.DownProcess[0].PID != pid || sentMsg.Node != self {
		t.Fatalf("unexpected shard message: %+v", sentMsg)
	}
	if _, ok := book.LocateLocal(name); ok {
		t.Fatalf("expected local book to remove %s", name)
	}
	if _, ok := w.nameToPID[name]; ok {
		t.Fatalf("expected nameToPID to remove %s", name)
	}
	if _, ok := w.pidToName[pid]; ok {
		t.Fatalf("expected pidToName to remove %v", pid)
	}
}

func TestUnregisterLocalProcessIgnoresMismatchedPID(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)
	currentPID := gen.PID{Node: self, ID: 10}
	stalePID := gen.PID{Node: self, ID: 9}
	if err := book.AddProcess(self, core.ProcessInfo{Name: name, PID: currentPID, Node: self, BirthAt: 123}); err != nil {
		t.Fatalf("add process: %v", err)
	}

	var sendCalls int
	w := &whereis{
		book:             book,
		selfNode:         self,
		nameToPID:        map[gen.Atom]gen.PID{name: currentPID},
		nameToBirthAt:    map[gen.Atom]int64{name: 123},
		pidToName:        map[gen.PID]gen.Atom{currentPID: name},
		processCache:     core.NewAtomicValue[core.ProcessInfoList](),
		selfVersion:      core.NewVersion(),
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			return nil
		},
	}

	if err := w.unregisterLocalProcess(core.MessageUnregisterLocalProcess{Name: name, PID: stalePID}); err != nil {
		t.Fatalf("unregister local process: %v", err)
	}
	if sendCalls != 0 {
		t.Fatalf("expected no shard message, got %d", sendCalls)
	}
	if node, ok := book.LocateLocal(name); !ok || node != self {
		t.Fatalf("expected local book to keep %s on %s, got %s ok=%v", name, self, node, ok)
	}
	if got := w.nameToPID[name]; got != currentPID {
		t.Fatalf("expected current pid to remain %v, got %v", currentPID, got)
	}
}

func TestUnregisterLocalProcessByNameRemovesZeroPIDFastPath(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}
	name := findRemoteDirectoryProcess(t, book, self)

	var sendCalls int
	var sentMsg core.MessageProcessChanged
	w := &whereis{
		book:             book,
		selfNode:         self,
		nameToPID:        make(map[gen.Atom]gen.PID),
		nameToBirthAt:    make(map[gen.Atom]int64),
		pidToName:        make(map[gen.PID]gen.Atom),
		processCache:     core.NewAtomicValue[core.ProcessInfoList](),
		selfVersion:      core.NewVersion(),
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			sentMsg = msg
			return nil
		},
	}

	if err := w.registerLocalProcess(core.MessageRegisterLocalProcess{Name: name, BirthAt: 123}); err != nil {
		t.Fatalf("register zero-pid process: %v", err)
	}
	if _, ok := book.LocateLocal(name); !ok {
		t.Fatalf("expected local book to locate %s after register", name)
	}

	if err := w.unregisterLocalProcess(core.MessageUnregisterLocalProcess{Name: name}); err != nil {
		t.Fatalf("unregister zero-pid process by name: %v", err)
	}
	if sendCalls != 2 {
		t.Fatalf("expected register and unregister shard messages, got %d", sendCalls)
	}
	if len(sentMsg.DownProcess) != 1 || sentMsg.DownProcess[0].Name != name || sentMsg.DownProcess[0].PID != (gen.PID{}) {
		t.Fatalf("unexpected unregister shard message: %+v", sentMsg)
	}
	if _, ok := book.LocateLocal(name); ok {
		t.Fatalf("expected local book to remove %s", name)
	}
	if _, ok := w.nameToBirthAt[name]; ok {
		t.Fatalf("expected nameToBirthAt to remove %s", name)
	}
}

func TestUnregisterLocalProcessIgnoresEmptyAndUnknownName(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}

	var sendCalls int
	w := &whereis{
		book:             book,
		selfNode:         self,
		nameToPID:        make(map[gen.Atom]gen.PID),
		nameToBirthAt:    make(map[gen.Atom]int64),
		pidToName:        make(map[gen.PID]gen.Atom),
		processCache:     core.NewAtomicValue[core.ProcessInfoList](),
		selfVersion:      core.NewVersion(),
		sendFailureLogAt: make(map[gen.Atom]time.Time),
		sendProcessChanged: func(pid gen.ProcessID, msg core.MessageProcessChanged) error {
			sendCalls++
			return nil
		},
	}

	if err := w.unregisterLocalProcess(core.MessageUnregisterLocalProcess{}); err != nil {
		t.Fatalf("unregister empty name: %v", err)
	}
	if err := w.unregisterLocalProcess(core.MessageUnregisterLocalProcess{Name: gen.Atom("missing")}); err != nil {
		t.Fatalf("unregister unknown name: %v", err)
	}
	if sendCalls != 0 {
		t.Fatalf("expected no shard message, got %d", sendCalls)
	}
}
