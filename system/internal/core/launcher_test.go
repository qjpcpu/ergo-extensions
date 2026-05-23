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
	spawner := NewSpawner(parent.Process(), launcherName)

	procName := gen.Atom("my_proc")
	pid, err := spawner.SpawnRegister(procName)
	if err != nil {
		t.Fatalf("SpawnRegister failed: %v", err)
	}
	if pid == (gen.PID{}) {
		t.Fatal("expected non-zero pid")
	}
	parent.ShouldSpawn().Once().Assert()
	parent.ShouldSend().
		To(WhereIsProcess).
		MessageMatching(func(message any) bool {
			msg, ok := message.(MessageRegisterLocalProcess)
			return ok && msg.Name == procName && msg.PID == pid
		}).
		Once().
		Assert()

	// Test non-existent launcher
	spawnerInvalid := NewSpawner(parent.Process(), "non_existent")
	_, err = spawnerInvalid.SpawnRegister("any")
	if err == nil {
		t.Errorf("expected error for non-existent launcher")
	}
}
