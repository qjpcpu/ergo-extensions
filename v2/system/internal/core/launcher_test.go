package core

import (
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/ergo/testing/unit"
)

type spawnerParentProc struct{ act.Actor }

func TestLauncherRegistration(t *testing.T) {
	name := gen.Atom("test_launcher")
	factory := func() gen.ProcessBehavior { return nil }
	launcher := Launcher{
		Factory: factory,
	}

	// Test Register
	err := RegisterLauncher(name, launcher)
	if err != nil {
		t.Fatalf("failed to register launcher: %v", err)
	}

	// Test Get
	l, ok := GetLauncher(name)
	if !ok {
		t.Fatalf("failed to get launcher")
	}
	if l.Name != name {
		t.Errorf("expected name %s, got %s", name, l.Name)
	}

	// Test invalid register
	err = RegisterLauncher("invalid", Launcher{})
	if err == nil {
		t.Errorf("expected error for invalid launcher registration")
	}

	// Test get non-existent
	_, ok = GetLauncher("non_existent")
	if ok {
		t.Errorf("expected not ok for non-existent launcher")
	}

	// Test Unregister
	UnregisterLauncher(name)
	_, ok = GetLauncher(name)
	if ok {
		t.Errorf("expected not ok after unregister")
	}
}

func TestSpawner(t *testing.T) {
	launcherName := gen.Atom("spawner_launcher")
	factory := func() gen.ProcessBehavior { return nil }
	RegisterLauncher(launcherName, Launcher{Factory: factory})
	defer UnregisterLauncher(launcherName)

	parent, err := unit.Spawn(t, func() gen.ProcessBehavior { return &spawnerParentProc{} })
	if err != nil {
		t.Fatalf("spawn parent actor: %v", err)
	}
	parent.ClearEvents()
	var routeKey gen.Atom
	decorate := func(key gen.Atom, factory gen.ProcessFactory) gen.ProcessFactory {
		routeKey = key
		return factory
	}
	spawner := NewSpawner(parent.Process(), decorate, launcherName)

	procName := gen.Atom("my_proc")
	pid, err := spawner.SpawnRegister(procName)
	if err != nil {
		t.Fatalf("SpawnRegister failed: %v", err)
	}
	if pid == (gen.PID{}) {
		t.Fatal("expected non-zero pid")
	}
	parent.ShouldSpawn().Once().Assert()
	if routeKey != procName {
		t.Fatalf("expected route key %s, got %s", procName, routeKey)
	}

	// Test non-existent launcher
	spawnerInvalid := NewSpawner(parent.Process(), decorate, "non_existent")
	_, err = spawnerInvalid.SpawnRegister("any")
	if err == nil {
		t.Errorf("expected error for non-existent launcher")
	}

	spawnerWithoutRouter := NewSpawner(parent.Process(), nil, launcherName)
	if _, err := spawnerWithoutRouter.SpawnRegister("any"); err == nil {
		t.Error("expected error for missing actor router")
	}
}

func TestLauncherIterationAndSingletonDaemon(t *testing.T) {
	name := gen.Atom("iterated_launcher")
	if err := RegisterLauncher(name, Launcher{Factory: func() gen.ProcessBehavior { return &spawnerParentProc{} }}); err != nil {
		t.Fatal(err)
	}
	defer UnregisterLauncher(name)
	found := false
	RangeLaunchers(func(got gen.Atom, launcher Launcher) bool {
		if got == name {
			found = launcher.Name == name && launcher.Factory != nil
			return false
		}
		return true
	})
	if !found {
		t.Fatal("registered launcher was not visited")
	}

	iterator := SingletonDaemon("daemon", []any{"argument"})()
	processes, more, err := iterator()
	if err != nil || more || len(processes) != 1 {
		t.Fatalf("unexpected singleton iteration: processes=%+v more=%v err=%v", processes, more, err)
	}
	if processes[0].ProcessName != "daemon" || len(processes[0].Args) != 1 {
		t.Fatalf("unexpected daemon definition: %+v", processes[0])
	}
}
