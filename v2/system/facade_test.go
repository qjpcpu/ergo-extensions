package system

import (
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type facadeParentProc struct{ act.Actor }

func TestFacadeConstructorsAndLauncherHelpers(t *testing.T) {
	atomicValue := NewAtomicValue[int]()
	if got := atomicValue.Store(7); got != 7 || atomicValue.Load() != 7 {
		t.Fatalf("unexpected atomic value behavior")
	}
	list := NewImmutableList([]int{1, 2})
	if !list.Exist(1) || list.Len() != 2 {
		t.Fatalf("unexpected immutable list: len=%d", list.Len())
	}
	if NewNodeList(gen.Atom("node")).Len() != 1 {
		t.Fatal("expected node list")
	}
	if NewAddressBook() == nil {
		t.Fatal("expected address book")
	}
	if DefaultDaemonOptions().LaunchTimeout == 0 || DefaultMembershipOptions().RefreshInterval == 0 {
		t.Fatal("expected default options")
	}

	launcherName := gen.Atom("facade-launcher")
	if err := RegisterLauncher(launcherName, Launcher{
		Factory: func() gen.ProcessBehavior { return &facadeParentProc{} },
	}); err != nil {
		t.Fatalf("register launcher: %v", err)
	}
	t.Cleanup(func() { UnregisterLauncher(launcherName) })
	if launcher, ok := GetLauncher(launcherName); !ok || launcher.Name != launcherName {
		t.Fatalf("get launcher failed: %+v ok=%v", launcher, ok)
	}

	parent, err := unit.Spawn(t, func() gen.ProcessBehavior { return &facadeParentProc{} })
	if err != nil {
		t.Fatalf("spawn parent: %v", err)
	}
	router, err := NewActorRouter(routeStore(t), ActorRouterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	book := NewAddressBook()
	if err := router.Bind(parent.Node()); err != nil {
		t.Fatal(err)
	}
	if err := book.BindLocator(parent.Node().Name(), router.lookup); err != nil {
		t.Fatal(err)
	}
	spawner := NewSpawner(parent.Process(), router, launcherName)
	if _, err := spawner.SpawnRegister(gen.Atom("facade-proc")); err != nil {
		t.Fatalf("spawn through facade spawner: %v", err)
	}

	iter := SingletonDaemon(gen.Atom("singleton"), []any{"arg"})()
	processes, more, err := iter()
	if err != nil || more || len(processes) != 1 || processes[0].ProcessName != "singleton" {
		t.Fatalf("unexpected singleton daemon result: %+v more=%v err=%v", processes, more, err)
	}
}
