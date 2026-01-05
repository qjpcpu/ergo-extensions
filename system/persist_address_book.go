package system

import (
	"sync"
	"sync/atomic"
	"time"

	"ergo.services/ergo/gen"
	"github.com/buraksezer/consistent"
)

type IAddressBookStorage interface {
	Get(process gen.Atom) (node gen.Atom, version int, err error)
	Set(node gen.Atom, process gen.Atom, version int) error
	Remove(node gen.Atom, process gen.Atom, version int) error
}

type PersistAddressBook struct {
	registrar    gen.Registrar
	st           IAddressBookStorage
	mu           sync.RWMutex
	nodes        map[gen.Atom]struct{}  // all available nodes
	nodesCache   atomic.Value           // cache for available nodes to avoid frequent lock
	ring         *consistent.Consistent // consistent hashing ring
	lastModified *atomic.Int64
}

func NewPersistAddressBook(st IAddressBookStorage) *PersistAddressBook {
	var c atomic.Value
	c.Store([]gen.Atom{})
	var lm atomic.Int64
	lm.Store(time.Now().Unix())
	return &PersistAddressBook{
		nodes:        make(map[gen.Atom]struct{}),
		ring:         makeRing(),
		nodesCache:   c,
		st:           st,
		lastModified: &lm,
	}
}

func (book *PersistAddressBook) SetProcess(node gen.Atom, version int, process ...gen.Atom) error {
	if len(process) == 0 {
		return nil
	}
	for _, p := range process {
		if p == "" || isSystemProc(p) {
			continue
		}
		if err := book.st.Set(node, p, version); err != nil {
			return err
		}
	}
	book.lastModified.Store(time.Now().Unix())
	return nil
}

func (book *PersistAddressBook) RemoveProcess(node gen.Atom, version int, process ...gen.Atom) error {
	if len(process) == 0 {
		return nil
	}
	for _, p := range process {
		if p == "" || isSystemProc(p) {
			continue
		}
		if err := book.st.Remove(node, p, version); err != nil {
			return err
		}
	}
	book.lastModified.Store(time.Now().Unix())
	return nil
}

func (book *PersistAddressBook) Locate(process gen.Atom) (gen.Atom, bool) {
	node, ver, err := book.st.Get(process)
	if err != nil || node == "" {
		return "", false
	}
	if val, err := book.nodeVersion(node); err != nil {
		return "", false
	} else if val == ver {
		return node, true
	}
	return "", false
}

func (book *PersistAddressBook) PickNode(process gen.Atom) gen.Atom {
	book.mu.RLock()
	defer book.mu.RUnlock()
	if m := book.ring.LocateKey([]byte(process)); m != nil {
		return gen.Atom(m.String())
	}
	return gen.Atom("")
}

func (book *PersistAddressBook) GetAvailableNodes() []gen.Atom {
	return book.nodesCache.Load().([]gen.Atom)
}

func (book *PersistAddressBook) SetAvailableNodes(nodes []gen.Atom) error {
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

func (book *PersistAddressBook) LastModified() int64 {
	return book.lastModified.Load()
}

func (book *PersistAddressBook) SetRegistrar(r gen.Registrar) {
	book.registrar = r
}

func (book *PersistAddressBook) nodeVersion(node gen.Atom) (int, error) {
	if r := book.registrar; r != nil {
		val, err := r.ConfigItem(string(node))
		if err != nil {
			return -1, err
		}
		if ver, ok := val.(int); ok {
			return ver, nil
		}
	}
	return -1, nil
}
