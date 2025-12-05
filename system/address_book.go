package system

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"ergo.services/ergo/gen"

	builtin "ergo.services/ergo/app/system"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// IAddressBook defines address book interface
type IAddressBook interface {
	Locate(process gen.Atom) (ProcessInfo, bool)
	GetProcessList(node gen.Atom) ProcessInfoList
	PickNode(process gen.Atom) gen.Atom
	GetAvailableNodes() []gen.Atom
}

// AddressBook is a registry for all processes running on all nodes
// in the cluster. It's used to locate processes by their registered names.
type AddressBook struct {
	mu             sync.RWMutex
	nodes          map[gen.Atom]struct{} // all available nodes
	nodesCache     atomic.Value          // cache for available nodes to avoid frequent lock
	processToNodes map[gen.Atom][]gen.Atom
	nodeProcesses  map[gen.Atom]map[gen.Atom]ProcessInfo
	ring           *consistent.Consistent // consistent hashing ring
}

// NewAddressBook creates a new AddressBook
func NewAddressBook() *AddressBook {
	var c atomic.Value
	c.Store([]gen.Atom{})
	return &AddressBook{
		nodes:          make(map[gen.Atom]struct{}),
		processToNodes: make(map[gen.Atom][]gen.Atom),
		nodeProcesses:  make(map[gen.Atom]map[gen.Atom]ProcessInfo),
		ring:           makeRing(),
		nodesCache:     c,
	}
}

// Locate returns a process information by its registered name.
func (book *AddressBook) Locate(process gen.Atom) (ProcessInfo, bool) {
	book.mu.RLock()
	defer book.mu.RUnlock()
	return book.locate(process)
}

// GetProcessList returns a list of processes running on the given node.
func (book *AddressBook) GetProcessList(node gen.Atom) (list ProcessInfoList) {
	book.mu.RLock()
	defer book.mu.RUnlock()
	for _, p := range book.nodeProcesses[node] {
		list = append(list, p)
	}
	return
}

// locate is an internal method to find a process by its registered name.
// This method is not thread-safe.
func (book *AddressBook) locate(process gen.Atom) (ProcessInfo, bool) {
	nodes, ok := book.processToNodes[process]
	if !ok {
		return ProcessInfo{}, false
	}
	for _, node := range nodes {
		_, ok = book.nodes[node]
		if !ok {
			continue
		}
		processes, ok := book.nodeProcesses[node]
		if !ok {
			continue
		}
		p, ok := processes[process]
		if ok {
			return p, ok
		}
	}
	return ProcessInfo{}, false
}

// SetProcess sets a list of processes for the given node.
// It removes all previously registered processes for this node.
func (book *AddressBook) SetProcess(node gen.Atom, ps ...ProcessInfo) {
	book.mu.Lock()
	defer book.mu.Unlock()

	oldProcesses := book.nodeProcesses[node]

	newProcesses := make(map[gen.Atom]ProcessInfo, len(ps))
	for _, process := range ps {
		if name := process.Name; name != "" {
			newProcesses[name] = process
			book.processToNodes[name] = unifyNodes(append(book.processToNodes[name], node))
		}
	}

	for name := range oldProcesses {
		if _, ok := newProcesses[name]; !ok {
			book.processToNodes[name] = removeNode(book.processToNodes[name], node)
		}
	}

	book.nodeProcesses[node] = newProcesses
}

// AddProcess adds a list of processes for the given node.
func (book *AddressBook) AddProcess(node gen.Atom, ps ...ProcessInfo) {
	if len(ps) == 0 {
		return
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	processes, ok := book.nodeProcesses[node]
	if !ok {
		processes = make(map[gen.Atom]ProcessInfo)
	}

	for _, process := range ps {
		if name := process.Name; name != "" {
			processes[name] = process
			book.processToNodes[name] = unifyNodes(append(book.processToNodes[name], node))
		}
	}
	book.nodeProcesses[node] = processes
}

// RemoveProcess removes a list of processes from the given node.
func (book *AddressBook) RemoveProcess(node gen.Atom, ps ...ProcessInfo) {
	if len(ps) == 0 {
		return
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	processes, ok := book.nodeProcesses[node]
	if !ok {
		return
	}
	for _, process := range ps {
		if name := process.Name; name != "" {
			if old, ok := processes[name]; ok && old.Node == node {
				book.processToNodes[name] = removeNode(book.processToNodes[name], node)
				delete(processes, name)
			}
		}
	}
}

// SetAvailableNodes sets a list of available nodes.
func (book *AddressBook) SetAvailableNodes(nodes []gen.Atom) error {
	book.mu.Lock()
	defer book.mu.Unlock()
	newNodes := make(map[gen.Atom]struct{})
	for _, item := range nodes {
		if _, ok := book.nodes[item]; !ok {
			book.nodes[item] = struct{}{}
			book.ring.Add(Member(item))
		}
		newNodes[item] = struct{}{}
	}
	for item := range book.nodes {
		if _, ok := newNodes[item]; !ok {
			book.ring.Remove(string(item))
			delete(book.nodes, item)
		}
	}
	book.nodesCache.Store(nodes)
	return nil
}

// GetAvailableNodes returns a list of available nodes.
func (book *AddressBook) GetAvailableNodes() []gen.Atom {
	return book.nodesCache.Load().([]gen.Atom)
}

// PickNode returns a node name by the given process name using consistent hashing.
func (book *AddressBook) PickNode(process gen.Atom) gen.Atom {
	book.mu.RLock()
	defer book.mu.RUnlock()
	if m := book.ring.LocateKey([]byte(process)); m != nil {
		return gen.Atom(m.String())
	}
	return gen.Atom("")
}

// unifyNodes sorts and removes duplicates from the given list of nodes.
func unifyNodes(nodes []gen.Atom) []gen.Atom {
	sort.SliceStable(nodes, func(i int, j int) bool {
		return nodes[i] < nodes[j]
	})
	return uniqNodes(nodes)
}

func uniqNodes(nodes []gen.Atom) []gen.Atom {
	dup := make(map[gen.Atom]struct{})
	var del int
	for i, item := range nodes {
		if _, ok := dup[item]; ok {
			del++
		} else if del > 0 {
			nodes[i-del] = item
		}
		dup[item] = struct{}{}
	}
	nodes = nodes[:len(nodes)-del]
	return nodes
}

// removeNode removes a node from the given list of nodes.
func removeNode(nodes []gen.Atom, node gen.Atom) []gen.Atom {
	var del int
	for i, item := range nodes {
		if item == node {
			del++
		} else if del > 0 {
			nodes[i-del] = item
		}
	}
	nodes = nodes[:len(nodes)-del]
	return nodes
}

// interactNodes returns a list of nodes that exist in both given lists.
func interactNodes(nodes1, nodes2 []gen.Atom) (ret []gen.Atom) {
	m := make(map[gen.Atom]struct{})
	for _, n := range nodes2 {
		m[n] = struct{}{}
	}
	for _, v := range nodes1 {
		if _, ok := m[v]; ok {
			ret = append(ret, v)
		}
	}
	return
}

// mapGenProcessList converts a list of gen.ProcessShortInfo to a list of ProcessInfo.
func mapGenProcessList(node gen.Atom, ps []gen.ProcessShortInfo) (ret []ProcessInfo) {
	for _, item := range ps {
		ret = append(ret, mapGenProcess(node, item))
	}
	return
}

// filterProcessList removes local processes from the given list of processes.
func filterProcessList(ps []ProcessInfo) []ProcessInfo {
	var del int
	for i, item := range ps {
		if isLocalProc(item) {
			del++
		} else if del > 0 {
			ps[i-del] = item
		}
	}
	return ps[:len(ps)-del]
}

// isLocalProc checks if the given process is a local process.
func isLocalProc(p ProcessInfo) bool {
	if p.Application == builtin.Name || p.Name == "" {
		return true
	}
	switch p.Name {
	case WhereIsProcess, WhereIsSupervisor, DaemonMonitorProcess:
		return true
	}
	return false
}

// mapGenProcess converts a gen.ProcessShortInfo to a ProcessInfo.
func mapGenProcess(node gen.Atom, p gen.ProcessShortInfo) ProcessInfo {
	return ProcessInfo{
		Node:        node,
		PID:         p.PID,
		Name:        p.Name,
		Application: p.Application,
		State:       p.State,
		Uptime:      p.Uptime,
	}
}

// hasher is a wrapper for xxhash.Sum64
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// Member is a wrapper for string to implement consistent.Member interface.
type Member string

func (m Member) String() string {
	return string(m)
}

// makeRing creates a new consistent hashing ring.
func makeRing(members ...consistent.Member) *consistent.Consistent {
	cfg := consistent.Config{
		PartitionCount:    10240,
		ReplicationFactor: 40,
		Load:              1.2,
		Hasher:            hasher{},
	}
	return consistent.New(members, cfg)
}

// shortInfo returns a short string representation of the given list of processes.
func shortInfo(ps []ProcessInfo) string {
	var arr []string
	for i, item := range ps {
		if i >= 3 {
			arr = append(arr, "...")
			break
		}
		arr = append(arr, string(item.Name))
	}
	if len(arr) == 0 {
		return ""
	}
	return "(" + strings.Join(arr, ",") + ")"
}
