package membership

import (
	"errors"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
	core "github.com/qjpcpu/ergo-extensions/v2/system/internal/core"
	"github.com/qjpcpu/registrar/events"
)

func spawnMembership(t *testing.T, book *core.AddressBook, options Options) *unit.TestActor {
	t.Helper()
	actor, err := unit.Spawn(t, Factory(book, options), unit.WithNodeName("node-a@localhost"))
	if err != nil {
		t.Fatalf("spawn membership: %v", err)
	}
	registrar, err := actor.Node().Network().Registrar()
	if err != nil {
		t.Fatalf("get test registrar: %v", err)
	}
	actor.Behavior().(*membership).registrar = registrar
	actor.ClearEvents()
	return actor
}

func TestNormalizeOptions(t *testing.T) {
	defaults := DefaultOptions()
	if got := normalizeOptions(Options{}); got != defaults {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	got := normalizeOptions(Options{
		RefreshInterval: time.Second,
		DebounceMin:     2 * time.Second,
		DebounceMax:     time.Second,
		RetryMin:        4 * time.Second,
		RetryMax:        time.Second,
	})
	if got.DebounceMax != got.DebounceMin || got.RetryMax != got.RetryMin {
		t.Fatalf("invalid normalized ranges: %+v", got)
	}
}

func TestMembershipInitialAndPeriodicRefresh(t *testing.T) {
	book := core.NewAddressBook()
	actor := spawnMembership(t, book, Options{RefreshInterval: time.Second})
	actor.SendMessage(gen.PID{}, messageInit{})
	if !book.GetAvailableNodes().Exist(actor.Node().Name()) {
		t.Fatalf("self node was not added: %v", book.GetAvailableNodes().GetAll())
	}
	behavior := actor.Behavior().(*membership)
	if err := behavior.setup(); err != nil {
		t.Fatalf("setup with an existing registrar failed: %v", err)
	}
	if behavior.lastUpdate.IsZero() || behavior.refreshID == 0 {
		t.Fatalf("refresh state was not updated: %+v", behavior)
	}
	actor.ShouldSend().Message(messageRefresh{ID: behavior.refreshID}).Once().Assert()

	registrar, err := actor.Node().Network().Registrar()
	if err != nil {
		t.Fatal(err)
	}
	registrar.(*unit.TestRegistrar).AddNode("node-b@localhost", []gen.Route{{Host: "127.0.0.1", Port: 1234}})
	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, messageRefresh{ID: behavior.refreshID})
	if !book.GetAvailableNodes().Exist("node-b@localhost") {
		t.Fatalf("remote node was not refreshed: %v", book.GetAvailableNodes().GetAll())
	}
}

func TestMembershipDebouncesTopologyEventsAndIgnoresStaleMessages(t *testing.T) {
	book := core.NewAddressBook()
	actor := spawnMembership(t, book, Options{
		DebounceMin: time.Millisecond,
		DebounceMax: 2 * time.Millisecond,
	})
	behavior := actor.Behavior().(*membership)
	if err := behavior.HandleEvent(gen.MessageEvent{Message: events.EventNodeJoined{Name: "node-b@localhost"}}); err != nil {
		t.Fatal(err)
	}
	first := behavior.topologyID
	if err := behavior.HandleEvent(gen.MessageEvent{Message: events.EventNodeLeft{Name: "node-b@localhost"}}); err != nil {
		t.Fatal(err)
	}
	if behavior.topologyID != first+1 {
		t.Fatalf("expected topology generation increment, got %d", behavior.topologyID)
	}
	actor.ClearEvents()
	actor.SendMessage(gen.PID{}, messageTopologyChanged{ID: first})
	actor.ShouldNotSend().Assert()
}

func TestMembershipRefreshFailureKeepsLastSnapshot(t *testing.T) {
	book := core.NewAddressBook()
	if err := book.SetAvailableNodes(core.NewNodeList("existing@localhost")); err != nil {
		t.Fatal(err)
	}
	actor := spawnMembership(t, book, Options{RetryMin: time.Millisecond, RetryMax: time.Second})
	behavior := actor.Behavior().(*membership)
	behavior.registrar = &failingRegistrar{err: errors.New("nodes unavailable")}
	behavior.refreshAndSchedule()
	if !book.GetAvailableNodes().Exist("existing@localhost") {
		t.Fatal("failed refresh cleared the last snapshot")
	}
	if behavior.lastError == nil || behavior.retry != 1 {
		t.Fatalf("failure state not recorded: retry=%d err=%v", behavior.retry, behavior.lastError)
	}
	actor.ShouldSend().Once().Assert()
}

func TestMembershipInspectRetryBoundsAndTerminate(t *testing.T) {
	book := core.NewAddressBook()
	actor := spawnMembership(t, book, Options{
		RetryMin:    time.Second,
		RetryMax:    4 * time.Second,
		DebounceMin: time.Millisecond,
		DebounceMax: time.Millisecond,
	})
	behavior := actor.Behavior().(*membership)
	behavior.lastUpdate = time.Unix(100, 0).UTC()
	behavior.lastError = errors.New("last failure")
	inspect := behavior.HandleInspect(gen.PID{})
	if inspect["nodes"] != "0" || inspect["last_error"] != "last failure" || inspect["last_refresh"] == "" {
		t.Fatalf("unexpected inspect response: %#v", inspect)
	}

	for _, test := range []struct {
		retry int
		want  time.Duration
	}{
		{retry: 1, want: time.Second},
		{retry: 2, want: 2 * time.Second},
		{retry: 3, want: 4 * time.Second},
		{retry: 20, want: 4 * time.Second},
	} {
		behavior.retry = test.retry
		if got := behavior.retryDelay(); got != test.want {
			t.Fatalf("retry %d: expected %s, got %s", test.retry, test.want, got)
		}
	}

	behavior.scheduleRefresh(time.Hour)
	behavior.scheduleTopologyRefresh()
	if behavior.cancelRefresh == nil || behavior.cancelTopology == nil {
		t.Fatal("expected scheduled timers")
	}
	behavior.Terminate(gen.TerminateReasonNormal)
	if behavior.cancelRefresh != nil || behavior.cancelTopology != nil {
		t.Fatal("terminate did not clear timers")
	}
}

func TestMembershipIgnoresUnrelatedAndStaleMessages(t *testing.T) {
	book := core.NewAddressBook()
	actor := spawnMembership(t, book, Options{})
	behavior := actor.Behavior().(*membership)
	behavior.refreshID = 10
	behavior.topologyID = 20
	actor.SendMessage(gen.PID{}, messageRefresh{ID: 9})
	actor.SendMessage(gen.PID{}, messageTopologyChanged{ID: 19})
	actor.SendMessage(gen.PID{}, "unrelated")
	if err := behavior.HandleEvent(gen.MessageEvent{Message: "unrelated"}); err != nil {
		t.Fatal(err)
	}
	actor.ShouldNotSend().Assert()
}

type failingRegistrar struct {
	err error
}

func (f *failingRegistrar) Register(gen.NodeRegistrar, gen.RegisterRoutes) (gen.StaticRoutes, error) {
	return gen.StaticRoutes{}, f.err
}
func (f *failingRegistrar) Resolver() gen.Resolver { return nil }
func (f *failingRegistrar) RegisterProxy(gen.Atom) error {
	return gen.ErrUnsupported
}
func (f *failingRegistrar) UnregisterProxy(gen.Atom) error { return gen.ErrUnsupported }
func (f *failingRegistrar) RegisterApplicationRoute(gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}
func (f *failingRegistrar) UnregisterApplicationRoute(gen.Atom) error { return gen.ErrUnsupported }
func (f *failingRegistrar) Nodes() ([]gen.Atom, error)                { return nil, f.err }
func (f *failingRegistrar) Config(...string) (map[string]any, error)  { return nil, gen.ErrUnsupported }
func (f *failingRegistrar) ConfigItem(string) (any, error)            { return nil, gen.ErrUnsupported }
func (f *failingRegistrar) Event() (gen.Event, error)                 { return gen.Event{}, f.err }
func (f *failingRegistrar) Info() gen.RegistrarInfo                   { return gen.RegistrarInfo{} }
func (f *failingRegistrar) Terminate()                                {}
func (f *failingRegistrar) Version() gen.Version                      { return gen.Version{} }
