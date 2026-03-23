package system

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// QueryOption defines the options for process lookup.
type QueryOption struct {
	// Timeout specifies the query timeout in seconds.
	Timeout int
	// CacheTTL specifies the cache time to live in seconds.
	CacheTTL int
}

// IAddressBookQuery defines the interface for performing distributed queries on the address book.
type IAddressBookQuery interface {
	// Locate performs a global lookup for a process name across the cluster.
	// This operation involves network communication and may fail due to timeouts.
	Locate(processName gen.Atom) (node gen.Atom, err error)
}

// IAddressBook defines address book interface
type IAddressBook interface {
	QueryBy(caller gen.Process, option QueryOption) IAddressBookQuery
	PickNode(process gen.Atom) gen.Atom
	PickDirectoryNode(process gen.Atom) gen.Atom
	GetAvailableNodes() *NodeList
}

// call represents an in-flight or completed function call.
// Multiple goroutines can wait for the same call's result via the WaitGroup.
type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// singleFlightGroup ensures that only one function call for a given key executes at a time.
// Concurrent calls with the same key will block and share the result of the first call.
// This prevents the thundering herd problem and resource waste from duplicate work.
//
// Type parameter T is the result type of the function being executed.
type singleFlightGroup[T any] struct {
	mu sync.Mutex
	m  map[string]*call[T]
}

// Do executes fn for the given key, ensuring only one execution happens at a time.
// If multiple goroutines call Do concurrently with the same key, only the first call
// executes fn; subsequent calls wait and receive the same result.
//
// If fn panics, the panic is recovered and returned as an error.
func (g *singleFlightGroup[T]) Do(key string, fn func() (T, error)) (T, error) {
	c, created := g.getOrCreateCall(key)
	if !created {
		return g.waitForExistingCall(c)
	}
	defer g.cleanupCall(key, c)

	return g.executeCall(c, fn)
}

// getOrCreateCall returns the in-flight call for the key and whether the caller
// created it. When created is false, the returned call must be waited on instead
// of executing fn again.
func (g *singleFlightGroup[T]) getOrCreateCall(key string) (*call[T], bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.m == nil {
		g.m = make(map[string]*call[T])
	}
	if c, ok := g.m[key]; ok {
		return c, false
	}
	c := new(call[T])
	c.wg.Add(1)
	g.m[key] = c
	return c, true
}

// waitForExistingCall waits for the result of an existing in-flight call.
// Should only be called when another goroutine has already created the call.
func (g *singleFlightGroup[T]) waitForExistingCall(c *call[T]) (T, error) {
	c.wg.Wait()
	return c.val, c.err
}

// cleanupCall marks the call as complete and removes it from the group's map.
// Called via defer to ensure cleanup happens regardless of success/failure.
func (g *singleFlightGroup[T]) cleanupCall(key string, c *call[T]) {
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// executeCall runs the function, capturing its result or any panic.
// Panics are recovered and converted to errors to prevent process crashes.
func (g *singleFlightGroup[T]) executeCall(c *call[T], fn func() (T, error)) (T, error) {
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("panic in singleFlightGroup.Do: %v", r)
		}
	}()

	c.val, c.err = fn()
	return c.val, c.err
}

// AddressBook is a registry for all processes running on all nodes
// in the cluster. It's used to locate processes by their registered names.
type AddressBook struct {
	mu             sync.RWMutex
	nodes          map[gen.Atom]struct{} // all available nodes
	processToNodes map[gen.Atom][]gen.Atom
	nodeProcesses  map[gen.Atom]map[gen.Atom]ProcessInfo
	ring           *consistent.Consistent // consistent hashing ring
	dirRing        *consistent.Consistent // directory hashing ring
	nodesCache     atomic.Value                  // cache for available nodes to avoid frequent lock
	nodesVersion   atomic.Int64
	queryCache     sync.Map                     // map[gen.Atom]cacheProcess
	flightGroup    singleFlightGroup[gen.Atom]  // prevent thundering herd on query
}

// NewAddressBook creates a new AddressBook
func NewAddressBook() *AddressBook {
	var c atomic.Value
	c.Store(NewNodeList())
	return &AddressBook{
		nodes:          make(map[gen.Atom]struct{}),
		processToNodes: make(map[gen.Atom][]gen.Atom),
		nodeProcesses:  make(map[gen.Atom]map[gen.Atom]ProcessInfo),
		ring:           makeRing(),
		dirRing:        makeRing(),
		nodesCache:     c,
	}
}

// Locate returns a process information by its registered name.
func (book *AddressBook) QueryBy(caller gen.Process, option QueryOption) IAddressBookQuery {
	return newBookQuery(caller, book, option)
}

// LocateLocal returns a process information by its registered name.
// Note: this only contains local data and should not be used for global process discovery.
func (book *AddressBook) LocateLocal(process gen.Atom) (gen.Atom, bool) {
	return book.locateFromMem(process)
}

// GetProcessList returns a list of processes running on the given node.
func (book *AddressBook) GetProcessList(node gen.Atom) (list ProcessInfoList, err error) {
	book.mu.RLock()
	defer book.mu.RUnlock()
	for _, p := range book.nodeProcesses[node] {
		list = append(list, p)
	}
	return
}

func (book *AddressBook) locateFromMem(process gen.Atom) (gen.Atom, bool) {
	book.mu.RLock()
	defer book.mu.RUnlock()
	v, ok := book.locate(process)
	if ok {
		return v.Node, true
	}
	return "", false
}

// locate is an internal method to find a process by its registered name.
// This method is not thread-safe.
func (book *AddressBook) locate(process gen.Atom) (ProcessInfo, bool) {
	// Read-view convergence: processToNodes may temporarily contain multiple nodes
	// for the same process name (during concurrent updates or membership churn).
	// The slice is kept sorted and de-duplicated. Among online nodes that still
	// report this process, LocateLocal selects the oldest instance (smallest BirthAt).
	// If BirthAt is equal, the sorted node order provides a deterministic winner.
	nodes, ok := book.processToNodes[process]
	if !ok {
		return ProcessInfo{}, false
	}
	var olderp ProcessInfo
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
		if ok && (olderp.BirthAt == 0 || p.BirthAt < olderp.BirthAt) {
			olderp = p
		}
	}
	return olderp, olderp.Node != ""
}

// SetProcess sets a list of processes for the given node.
// It removes all previously registered processes for this node.
func (book *AddressBook) SetProcess(node gen.Atom, ps ...ProcessInfo) error {
	book.mu.Lock()
	defer book.mu.Unlock()

	oldProcesses := book.nodeProcesses[node]

	newProcesses := make(map[gen.Atom]ProcessInfo, len(ps))
	for _, process := range ps {
		if name := process.Name; name != "" {
			// enforce node consistency
			process.Node = node
			newProcesses[name] = process
			book.processToNodes[name] = appendNodeIfAbsent(book.processToNodes[name], node)
		}
	}

	for name := range oldProcesses {
		if _, ok := newProcesses[name]; !ok {
			// remove node and cleanup empty keys
			arr := removeNode(book.processToNodes[name], node)
			if len(arr) == 0 {
				delete(book.processToNodes, name)
			} else {
				book.processToNodes[name] = arr
			}
		}
	}

	book.nodeProcesses[node] = newProcesses
	return nil
}

// AddProcess adds a list of processes for the given node.
func (book *AddressBook) AddProcess(node gen.Atom, ps ...ProcessInfo) error {
	if len(ps) == 0 {
		return nil
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	processes, ok := book.nodeProcesses[node]
	if !ok {
		processes = make(map[gen.Atom]ProcessInfo)
	}

	for _, process := range ps {
		if name := process.Name; name != "" {
			// enforce node consistency
			process.Node = node
			processes[name] = process
			book.processToNodes[name] = appendNodeIfAbsent(book.processToNodes[name], node)
		}
	}
	book.nodeProcesses[node] = processes
	return nil
}

// RemoveProcess removes a list of processes from the given node.
func (book *AddressBook) RemoveProcess(node gen.Atom, ps ...ProcessInfo) error {
	if len(ps) == 0 {
		return nil
	}
	book.mu.Lock()
	defer book.mu.Unlock()
	processes, ok := book.nodeProcesses[node]
	if !ok {
		return nil
	}
	for _, process := range ps {
		if name := process.Name; name != "" {
			if old, ok := processes[name]; ok && old.Node == node {
				arr := removeNode(book.processToNodes[name], node)
				if len(arr) == 0 {
					delete(book.processToNodes, name)
				} else {
					book.processToNodes[name] = arr
				}
				delete(processes, name)
			}
		}
	}
	return nil
}

// SetAvailableNodes sets a list of available nodes.
func (book *AddressBook) SetAvailableNodes(nodes *NodeList) error {
	n := nodes.Len()
	allNodes := nodes.GetAll()

	// Phase 1: Compute directory node set without holding the lock.
	// Sorting and hashing can be expensive for large clusters; doing this
	// outside the critical section avoids blocking concurrent readers.
	dirNodeCount := 5
	if n > 25 {
		dirNodeCount = max(5, int(math.Sqrt(float64(n))))
	}

	hashMap := make(map[gen.Atom]uint64, len(allNodes))
	for _, nd := range allNodes {
		hashMap[nd] = xxhash.Sum64([]byte(nd))
	}
	dirCandidates := make([]gen.Atom, len(allNodes))
	copy(dirCandidates, allNodes)
	sort.Slice(dirCandidates, func(i, j int) bool {
		return hashMap[dirCandidates[i]] < hashMap[dirCandidates[j]]
	})
	dirNodes := make(map[gen.Atom]struct{}, dirNodeCount)
	for i := 0; i < dirNodeCount && i < len(dirCandidates); i++ {
		dirNodes[dirCandidates[i]] = struct{}{}
	}

	// Also pre-build a lookup set outside the lock.
	newNodeSet := make(map[gen.Atom]struct{}, len(allNodes))
	for _, nd := range allNodes {
		newNodeSet[nd] = struct{}{}
	}

	// Phase 2: Acquire lock and apply changes.
	book.mu.Lock()
	defer book.mu.Unlock()
	var isChanged bool

	// Update data ring with new nodes
	for _, item := range allNodes {
		if _, ok := book.nodes[item]; !ok {
			book.nodes[item] = struct{}{}
			book.ring.Add(Member(item))
			isChanged = true
		}
	}

	// Refresh dirRing: simplest way is to rebuild it if set of nodes changed
	// but let's be more efficient.
	currentDirNodes := make(map[gen.Atom]struct{})
	for _, m := range book.dirRing.GetMembers() {
		currentDirNodes[gen.Atom(m.String())] = struct{}{}
	}

	for node := range dirNodes {
		if _, ok := currentDirNodes[node]; !ok {
			book.dirRing.Add(Member(node))
		}
	}
	for node := range currentDirNodes {
		if _, ok := dirNodes[node]; !ok {
			book.dirRing.Remove(string(node))
		}
	}

	for item := range book.nodes {
		if _, ok := newNodeSet[item]; !ok {
			book.ring.Remove(string(item))
			delete(book.nodes, item)
			isChanged = true
			// Clean up process indices for the node that has been removed.
			// Scenario: when a node goes offline, actors (named processes) may migrate to other nodes.
			// If the old node comes back, stale mappings could make LocateLocal() return the old node,
			// causing abnormal re-location or misrouting.
			// Action: for each process previously recorded on this node, remove the node from
			// the reverse index (processToNodes) and delete the node's process table entry.
			// Effect: AddressBook remains consistent with the current membership; LocateLocal/PickNode
			// won't point to offline or outdated nodes, preventing incorrect actor placement on rejoin.
			for proc := range book.nodeProcesses[item] {
				arr := removeNode(book.processToNodes[proc], item)
				if len(arr) == 0 {
					delete(book.processToNodes, proc)
				} else {
					book.processToNodes[proc] = arr
				}
			}
			delete(book.nodeProcesses, item)

			// Clean up global query cache for the offline node
			book.queryCache.Range(func(key, value any) bool {
				if p, ok := value.(cacheProcess); ok && p.node == item {
					book.queryCache.Delete(key)
				}
				return true
			})
		}
	}
	if isChanged {
		book.nodesCache.Store(nodes)
		book.nodesVersion.Add(1)
	}

	return nil
}

// PickDirectoryNode returns a directory node name by the given process name using consistent hashing.
func (book *AddressBook) PickDirectoryNode(process gen.Atom) gen.Atom {
	if process == "" {
		return gen.Atom("")
	}
	book.mu.RLock()
	defer book.mu.RUnlock()
	if m := book.dirRing.LocateKey([]byte(process)); m != nil {
		return gen.Atom(m.String())
	}
	return gen.Atom("")
}

// PickDirectoryNodeBatch returns directory node names for all given process names
// using consistent hashing. It acquires the read lock once for all lookups,
// reducing lock overhead compared to calling PickDirectoryNode per process.
func (book *AddressBook) PickDirectoryNodeBatch(processes []gen.Atom) map[gen.Atom]gen.Atom {
	if len(processes) == 0 {
		return nil
	}
	book.mu.RLock()
	defer book.mu.RUnlock()
	result := make(map[gen.Atom]gen.Atom, len(processes))
	for _, p := range processes {
		if p == "" {
			continue
		}
		if m := book.dirRing.LocateKey([]byte(p)); m != nil {
			result[p] = gen.Atom(m.String())
		}
	}
	return result
}

// GetAvailableNodes returns a list of available nodes.
func (book *AddressBook) GetAvailableNodes() *NodeList {
	return book.nodesCache.Load().(*NodeList)
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

func (book *AddressBook) NodesVersion() int64 {
	return book.nodesVersion.Load()
}

// unifyNodes sorts and removes duplicates from the given list of nodes.
func unifyNodes(nodes []gen.Atom) []gen.Atom {
	// Fast path: for small slices (common case where a process runs on 1-2 nodes),
	// avoid map allocation by using a simple linear dedup.
	if len(nodes) <= 4 {
		sort.SliceStable(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
		j := 0
		for i, v := range nodes {
			if i == 0 || v != nodes[i-1] {
				nodes[j] = v
				j++
			}
		}
		return nodes[:j]
	}
	sort.SliceStable(nodes, func(i int, j int) bool {
		return nodes[i] < nodes[j]
	})
	return uniqNodes(nodes)
}

// appendNodeIfAbsent appends node to the sorted, deduplicated slice only when
// the node is not already present, avoiding the allocate-append-sort-dedup
// cycle that unifyNodes(append(slice, node)) would perform.
func appendNodeIfAbsent(nodes []gen.Atom, node gen.Atom) []gen.Atom {
	for _, n := range nodes {
		if n == node {
			return nodes
		}
	}
	return unifyNodes(append(nodes, node))
}

func sortNodes(n []gen.Atom) []gen.Atom {
	nodes := make([]gen.Atom, len(n))
	copy(nodes, n)
	sort.SliceStable(nodes, func(i int, j int) bool {
		return nodes[i] < nodes[j]
	})
	return nodes
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

func newBookQuery(caller ICaller, book *AddressBook, option QueryOption) IAddressBookQuery {
	return &bookQuery{caller: caller, book: book, option: option}
}

type cacheProcess struct {
	node     gen.Atom
	expireAt int64
}

type ICaller interface {
	Node() gen.Node
	CallWithTimeout(to any, request any, timeout int) (any, error)
}

type bookQuery struct {
	caller        ICaller
	book          *AddressBook
	option        QueryOption
}

func (query *bookQuery) Locate(processName gen.Atom) (node gen.Atom, err error) {
	if processName == "" {
		return "", nil
	}

	ttl := query.option.CacheTTL
	if ttl > 0 {
		if val, ok := query.book.queryCache.Load(processName); ok {
			if p, ok := val.(cacheProcess); ok && p.expireAt > time.Now().Unix() {
				return p.node, nil
			}
		}
	}

	v, err := query.book.flightGroup.Do(string(processName), func() (gen.Atom, error) {
		// Double check
		if ttl > 0 {
			if val, ok := query.book.queryCache.Load(processName); ok {
				if p, ok := val.(cacheProcess); ok && p.expireAt > time.Now().Unix() {
					return p.node, nil
				}
			}
		}

		n, e := query.locate(processName)
		if e != nil {
			return "", e
		}

		if ttl > 0 && n != "" {
			query.book.queryCache.Store(processName, cacheProcess{
				node:     n,
				expireAt: time.Now().Add(time.Second * time.Duration(ttl)).Unix(),
			})
		}
		return n, nil
	})

	if err != nil {
		return "", err
	}
	return v, nil
}

func (query *bookQuery) locate(processName gen.Atom) (node gen.Atom, err error) {
	owner := query.book.PickDirectoryNode(processName)
	if owner == "" {
		return "", ErrNoAvailableNodes
	}
	if owner == query.caller.Node().Name() {
		if n, ok := query.book.LocateLocal(processName); ok {
			return n, nil
		}
		return "", nil
	}
	res, err := query.caller.CallWithTimeout(
		gen.ProcessID{Name: WhereIsProcess, Node: owner},
		MessageLocate{Name: processName},
		max(10, query.option.Timeout),
	)
	if err != nil {
		return "", err
	}
	var ok bool
	node, ok = res.(gen.Atom)
	if !ok {
		return "", gen.ErrProcessUnknown
	}
	return node, nil
}
