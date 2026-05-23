package whereis

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
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

func spawnWhereisUnit(t *testing.T, book *core.AddressBook, self gen.Atom) *unit.TestActor {
	t.Helper()
	actor, err := unit.Spawn(t, FactoryWithOptions(book, Options{
		SyncInterval: time.Hour,
	}), unit.WithNodeName(self))
	if err != nil {
		t.Fatalf("spawn whereis actor: %v", err)
	}
	actor.ClearEvents()
	return actor
}

func processChangedWith(t *testing.T, node gen.Atom, verify func(core.MessageProcessChanged) bool) func(any) bool {
	t.Helper()
	return func(message any) bool {
		msg, ok := message.(core.MessageProcessChanged)
		return ok && msg.Node == node && verify(msg)
	}
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

	actor := spawnWhereisUnit(t, book, self)
	actor.SendMessage(gen.PID{}, core.MessageRegisterLocalProcess{Name: name, BirthAt: 123})
	actor.ShouldSend().
		To(gen.ProcessID{Node: remote, Name: ProcessName}).
		MessageMatching(processChangedWith(t, self, func(msg core.MessageProcessChanged) bool {
			return len(msg.UpProcess) == 1 &&
				msg.UpProcess[0].Name == name &&
				msg.UpProcess[0].Node == self &&
				msg.UpProcess[0].BirthAt == 123 &&
				len(msg.DownProcess) == 0
		})).
		Once().
		Assert()
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

	actor := spawnWhereisUnit(t, book, self)
	actor.SendMessage(gen.PID{}, core.MessageRegisterLocalProcess{Name: name, PID: pid, BirthAt: birthAt})
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, core.MessageUnregisterLocalProcess{Name: name, PID: pid})
	actor.ShouldSend().
		To(gen.ProcessID{Node: remote, Name: ProcessName}).
		MessageMatching(processChangedWith(t, self, func(msg core.MessageProcessChanged) bool {
			return len(msg.UpProcess) == 0 &&
				len(msg.DownProcess) == 1 &&
				msg.DownProcess[0].Name == name &&
				msg.DownProcess[0].PID == pid &&
				msg.DownProcess[0].Node == self &&
				msg.DownProcess[0].BirthAt == birthAt
		})).
		Once().
		Assert()
	if _, ok := book.LocateLocal(name); ok {
		t.Fatalf("expected local book to remove %s", name)
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

	actor := spawnWhereisUnit(t, book, self)
	actor.SendMessage(gen.PID{}, core.MessageRegisterLocalProcess{Name: name, PID: currentPID, BirthAt: 123})
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, core.MessageUnregisterLocalProcess{Name: name, PID: stalePID})
	actor.ShouldNotSend().Assert()
	if node, ok := book.LocateLocal(name); !ok || node != self {
		t.Fatalf("expected local book to keep %s on %s, got %s ok=%v", name, self, node, ok)
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

	actor := spawnWhereisUnit(t, book, self)
	actor.SendMessage(gen.PID{}, core.MessageRegisterLocalProcess{Name: name, BirthAt: 123})
	if _, ok := book.LocateLocal(name); !ok {
		t.Fatalf("expected local book to locate %s after register", name)
	}
	actor.ClearEvents()

	actor.SendMessage(gen.PID{}, core.MessageUnregisterLocalProcess{Name: name})
	actor.ShouldSend().
		To(gen.ProcessID{Node: remote, Name: ProcessName}).
		MessageMatching(processChangedWith(t, self, func(msg core.MessageProcessChanged) bool {
			return len(msg.UpProcess) == 0 &&
				len(msg.DownProcess) == 1 &&
				msg.DownProcess[0].Name == name &&
				msg.DownProcess[0].PID == (gen.PID{}) &&
				msg.DownProcess[0].BirthAt == 123
		})).
		Once().
		Assert()
	if _, ok := book.LocateLocal(name); ok {
		t.Fatalf("expected local book to remove %s", name)
	}
}

func TestUnregisterLocalProcessIgnoresEmptyAndUnknownName(t *testing.T) {
	book := core.NewAddressBook()
	self := gen.Atom("node-a@127.0.0.1")
	remote := gen.Atom("node-b@127.0.0.1")
	if err := book.SetAvailableNodes(core.NewNodeList(self, remote)); err != nil {
		t.Fatalf("set available nodes: %v", err)
	}

	actor := spawnWhereisUnit(t, book, self)
	actor.SendMessage(gen.PID{}, core.MessageUnregisterLocalProcess{})
	actor.SendMessage(gen.PID{}, core.MessageUnregisterLocalProcess{Name: gen.Atom("missing")})
	actor.ShouldNotSend().Assert()
}
