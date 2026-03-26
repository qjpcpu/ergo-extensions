package system_test

import (
	"testing"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

type receiverProc struct {
	act.Actor
	target     gen.Atom
	resultChan chan system.MessageLocateResult
}

func (p *receiverProc) Init(args ...any) error {
	return p.Send(system.WhereIsProcess, system.MessageLocate{Name: p.target})
}

func (p *receiverProc) HandleMessage(from gen.PID, message any) error {
	if msg, ok := message.(system.MessageLocateResult); ok {
		p.resultChan <- msg
	}
	return nil
}

type forwardedReceiverProc struct {
	act.Actor
	target     gen.Atom
	wrongNode  gen.Atom
	resultChan chan system.MessageLocateResult
}

func (p *forwardedReceiverProc) Init(args ...any) error {
	return p.Send(gen.ProcessID{Node: p.wrongNode, Name: system.WhereIsProcess}, system.MessageForwardLocate{
		Name: p.target,
		From: p.PID(),
	})
}

func (p *forwardedReceiverProc) HandleMessage(from gen.PID, message any) error {
	if msg, ok := message.(system.MessageLocateResult); ok {
		p.resultChan <- msg
	}
	return nil
}

func TestWhereisLocateBySend(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	// 1. 在 n1 上启动一个命名的进程
	procName := gen.Atom("target.proc")
	_ = spawnNamed(t, n1, procName)

	// 等待地址簿同步
	waitUntil(t, 10*time.Second, func() bool {
		node := n2.LocateProcess(procName)
		return node == n1.Name()
	})

	// 2. 在 n2 上启动一个接收响应的进程，它会在 Init 中发送 MessageLocate
	resChan := make(chan system.MessageLocateResult, 1)
	_, err := n2.Spawn(func() gen.ProcessBehavior {
		return &receiverProc{target: procName, resultChan: resChan}
	}, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn receiver: %v", err)
	}

	// 3. 等待并验证结果
	select {
	case res := <-resChan:
		if res.Name != procName {
			t.Errorf("expected name %s, got %s", procName, res.Name)
		}
		if res.Node != n1.Name() {
			t.Errorf("expected node %s, got %s", n1.Name(), res.Node)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for MessageLocateResult")
	}
}

func TestWhereisForwardLocateRedirectsFromWrongNode(t *testing.T) {
	cluster := mem.NewCluster()
	n1 := startNode(t, cluster, "node-a@127.0.0.1")
	n2 := startNode(t, cluster, "node-b@127.0.0.1")

	procName := gen.Atom("target.forward.proc")
	_ = spawnNamed(t, n1, procName)

	waitUntil(t, 10*time.Second, func() bool {
		return n1.AddressBook().GetAvailableNodes().Len() == 2 &&
			n2.LocateProcess(procName) == n1.Name()
	})

	owner := n1.AddressBook().PickDirectoryNode(procName)
	if owner == "" {
		t.Fatal("expected non-empty directory owner")
	}
	wrongNode := n1.Name()
	if wrongNode == owner {
		wrongNode = n2.Name()
	}
	if wrongNode == owner {
		t.Fatal("failed to select a wrong forwarding node")
	}

	resChan := make(chan system.MessageLocateResult, 1)
	_, err := n2.Spawn(func() gen.ProcessBehavior {
		return &forwardedReceiverProc{
			target:     procName,
			wrongNode:  wrongNode,
			resultChan: resChan,
		}
	}, gen.ProcessOptions{})
	if err != nil {
		t.Fatalf("spawn forwarded receiver: %v", err)
	}

	select {
	case res := <-resChan:
		if res.Name != procName {
			t.Fatalf("expected name %s, got %s", procName, res.Name)
		}
		if res.Node != n1.Name() {
			t.Fatalf("expected node %s, got %s", n1.Name(), res.Node)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for redirected MessageLocateResult")
	}
}
