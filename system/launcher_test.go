package system

import (
	"testing"

	"ergo.services/ergo/gen"
)

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
	if l.name != name {
		t.Errorf("expected name %s, got %s", name, l.name)
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

type mockProcess struct {
	gen.Process
	spawnedName gen.Atom
}

func (m *mockProcess) SpawnRegister(name gen.Atom, factory gen.ProcessFactory, options gen.ProcessOptions, args ...any) (gen.PID, error) {
	m.spawnedName = name
	return gen.PID{ID: 1}, nil
}

func TestSpawner(t *testing.T) {
	launcherName := gen.Atom("spawner_launcher")
	factory := func() gen.ProcessBehavior { return nil }
	RegisterLauncher(launcherName, Launcher{Factory: factory})
	defer UnregisterLauncher(launcherName)

	parent := &mockProcess{}
	spawner := NewSpawner(parent, launcherName)

	procName := gen.Atom("my_proc")
	pid, err := spawner.SpawnRegister(procName)
	if err != nil {
		t.Fatalf("SpawnRegister failed: %v", err)
	}
	if pid.ID != 1 {
		t.Errorf("expected pid ID 1, got %d", pid.ID)
	}
	if parent.spawnedName != procName {
		t.Errorf("expected spawned name %s, got %s", procName, parent.spawnedName)
	}

	// Test non-existent launcher
	spawnerInvalid := NewSpawner(parent, "non_existent")
	_, err = spawnerInvalid.SpawnRegister("any")
	if err == nil {
		t.Errorf("expected error for non-existent launcher")
	}
}
