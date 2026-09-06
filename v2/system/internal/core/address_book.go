package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"ergo.services/ergo/gen"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

var ErrNoAvailableNodes = errors.New("no available nodes")

// IAddressBook exposes actor location and the immutable cluster topology view.
type IAddressBook interface {
	Locate(ctx context.Context, key gen.Atom) (gen.PID, bool, error)
	PickNode(key gen.Atom) gen.Atom
	PickCoordinatorNode(key gen.Atom) gen.Atom
	GetAvailableNodes() *NodeList
	NodesVersion() int64
}

// AddressBook maintains the current node set and its consistent-hash ring.
// Actor locations deliberately do not belong here; persistence owns them.
type AddressBook struct {
	mu           sync.RWMutex
	nodes        map[gen.Atom]struct{}
	ring         *consistent.Consistent
	nodesCache   atomic.Pointer[NodeList]
	nodesVersion atomic.Int64
	locatorMu    sync.RWMutex
	self         gen.Atom
	locator      func(context.Context, gen.Atom) (gen.PID, bool, error)
}

// BindLocator connects actor persistence lookup to this node's topology view.
// It is intended for node bootstrap and is idempotent for the same node.
func (book *AddressBook) BindLocator(self gen.Atom, locator func(context.Context, gen.Atom) (gen.PID, bool, error)) error {
	if self == "" || locator == nil {
		return errors.New("address book locator and node name are required")
	}
	book.locatorMu.Lock()
	defer book.locatorMu.Unlock()
	if book.locator != nil && book.self != self {
		return fmt.Errorf("address book is already bound to node %s", book.self)
	}
	book.self = self
	book.locator = locator
	return nil
}

// Locate resolves a route key and rejects PIDs hosted by offline nodes.
func (book *AddressBook) Locate(ctx context.Context, key gen.Atom) (gen.PID, bool, error) {
	book.locatorMu.RLock()
	self, locator := book.self, book.locator
	book.locatorMu.RUnlock()
	if locator == nil {
		return gen.PID{}, false, errors.New("address book locator is not bound")
	}
	pid, found, err := locator(ctx, key)
	if err != nil || !found {
		return gen.PID{}, false, err
	}
	if pid == (gen.PID{}) || pid.Node == "" {
		return gen.PID{}, false, nil
	}
	if pid.Node == self || book.GetAvailableNodes().Exist(pid.Node) {
		return pid, true, nil
	}
	return gen.PID{}, false, nil
}

func NewAddressBook() *AddressBook {
	book := &AddressBook{
		nodes: make(map[gen.Atom]struct{}),
		ring:  makeRing(),
	}
	book.nodesCache.Store(NewNodeList())
	return book
}

// SetAvailableNodes atomically replaces the topology when its node set changes.
func (book *AddressBook) SetAvailableNodes(nodes *NodeList) error {
	if nodes == nil {
		nodes = NewNodeList()
	}
	all := canonicalNodes(nodes.GetAll())
	next := make(map[gen.Atom]struct{}, len(all))
	for _, node := range all {
		next[node] = struct{}{}
	}

	book.mu.Lock()
	if sameNodeSet(book.nodes, next) {
		book.mu.Unlock()
		return nil
	}

	for node := range book.nodes {
		if _, ok := next[node]; !ok {
			book.ring.Remove(string(node))
		}
	}
	for node := range next {
		if _, ok := book.nodes[node]; !ok {
			book.ring.Add(Member(node))
		}
	}
	book.nodes = next
	book.nodesCache.Store(NewNodeList(all...))
	book.nodesVersion.Add(1)
	book.mu.Unlock()
	return nil
}

// PickNode deterministically maps a key to an available node.
func (book *AddressBook) PickNode(key gen.Atom) gen.Atom {
	if key == "" {
		return ""
	}
	book.mu.RLock()
	member := book.ring.LocateKey([]byte(key))
	book.mu.RUnlock()
	if member == nil {
		return ""
	}
	return gen.Atom(member.String())
}

// PickCoordinatorNode maps a coordination key to its owner. It intentionally
// shares the data ring so every node computes the same owner without another
// topology or a special coordinator subset.
func (book *AddressBook) PickCoordinatorNode(key gen.Atom) gen.Atom {
	return book.PickNode(key)
}

func (book *AddressBook) GetAvailableNodes() *NodeList {
	return book.nodesCache.Load()
}

func (book *AddressBook) NodesVersion() int64 {
	return book.nodesVersion.Load()
}

func canonicalNodes(nodes []gen.Atom) []gen.Atom {
	filtered := nodes[:0]
	for _, node := range nodes {
		if node != "" {
			filtered = append(filtered, node)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i] < filtered[j] })
	if len(filtered) < 2 {
		return filtered
	}
	write := 1
	for _, node := range filtered[1:] {
		if node != filtered[write-1] {
			filtered[write] = node
			write++
		}
	}
	return filtered[:write]
}

func sameNodeSet(current, next map[gen.Atom]struct{}) bool {
	if len(current) != len(next) {
		return false
	}
	for node := range current {
		if _, ok := next[node]; !ok {
			return false
		}
	}
	return true
}

type hasher struct{}

func (hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type Member string

func (m Member) String() string {
	return string(m)
}

func makeRing(members ...consistent.Member) *consistent.Consistent {
	return consistent.New(members, consistent.Config{
		PartitionCount:    10240,
		ReplicationFactor: 40,
		Load:              1.2,
		Hasher:            hasher{},
	})
}
