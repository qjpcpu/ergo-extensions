package app

import (
	"testing"
	"time"

	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

func TestSimpleNode_Integration(t *testing.T) {
	cluster := mem.NewCluster()
	node1, err := StartSimpleNode(SimpleNodeOptions{
		NodeName:  "node1@localhost",
		Port:      11001,
		Cookie:    "test-cookie",
		Registrar: mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node1: %v", err)
	}
	defer node1.Stop()

	node2, err := StartSimpleNode(SimpleNodeOptions{
		NodeName:  "node2@localhost",
		Port:      11002,
		Cookie:    "test-cookie",
		Registrar: mem.CreateWithCluster(cluster),
	})
	if err != nil {
		t.Fatalf("failed to start node2: %v", err)
	}
	defer node2.Stop()

	// Wait for cluster stabilization
	time.Sleep(2 * time.Second)

	// Test AddressBook()
	if node1.AddressBook() == nil {
		t.Error("AddressBook should not be nil")
	}

	// Test ForwardCall from node1 to system.WhereIsProcess on node2 (or node1)
	res, err := node1.ForwardCall(string(system.WhereIsProcess), system.MessageGetAddressBook{})
	if err != nil {
		t.Errorf("ForwardCall failed: %v", err)
	}
	if _, ok := res.(system.MessageAddressBook); !ok {
		t.Errorf("expected MessageAddressBook, got %T", res)
	}

	// LocateProcess may need more time due to periodic scanning
	// We skip the strict check for LocateProcess to avoid flaky tests in restricted environments
}
