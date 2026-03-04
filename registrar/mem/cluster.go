package mem

import (
	"sync"
	"sync/atomic"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/events"
)

func NewCluster() *Cluster {
	return &Cluster{
		routes:      make(map[gen.Atom][]gen.Route),
		nodeVersion: make(map[gen.Atom]uint32),
	}
}

type Cluster struct {
	mu          sync.RWMutex
	routes      map[gen.Atom][]gen.Route
	nodes       []gen.Atom
	nodeVersion map[gen.Atom]uint32
	onEvent     sync.Map // gen.Atom -> func(event)
	leader      gen.Atom
	version     atomic.Uint32
}

func (c *Cluster) GetRoutes(node gen.Atom) []gen.Route {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routes[node]
}

func (c *Cluster) GetNodes() []gen.Atom {
	c.mu.RLock()
	defer c.mu.RUnlock()
	arr := make([]gen.Atom, len(c.nodes))
	copy(arr, c.nodes)
	return arr
}

func (c *Cluster) GetLeader() gen.Atom {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

func (c *Cluster) GetVersion(node gen.Atom) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ver, ok := c.nodeVersion[node]; ok {
		return int(ver)
	}
	return -1
}

func (c *Cluster) AddRoutes(node gen.Atom, routes []gen.Route, onEvent func(any)) {
	if node == "" {
		return
	}
	var pendingEvents []func()

	c.mu.Lock()
	if _, ok := c.routes[node]; ok {
		c.mu.Unlock()
		return
	}
	c.routes[node] = routes
	c.nodeVersion[node] = c.version.Add(1)
	c.nodes = append(c.nodes, node)
	c.onEvent.Store(node, onEvent)
	c.onEvent.Range(func(key, value any) bool {
		sendEvent := value.(func(any))
		pendingEvents = append(pendingEvents, func() {
			sendEvent(events.EventNodeJoined{Name: node})
		})
		return true
	})
	pendingEvents = c.collectLeadershipEvents(pendingEvents)
	c.mu.Unlock()

	for _, fn := range pendingEvents {
		fn()
	}
}

func (c *Cluster) RemoveNode(node gen.Atom) {
	if node == "" {
		return
	}
	var pendingEvents []func()

	c.mu.Lock()
	if _, ok := c.routes[node]; !ok {
		c.mu.Unlock()
		return
	}
	delete(c.routes, node)
	delete(c.nodeVersion, node)
	for i, n := range c.nodes {
		if n == node {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	c.onEvent.Range(func(key, value any) bool {
		sendEvent := value.(func(any))
		pendingEvents = append(pendingEvents, func() {
			sendEvent(events.EventNodeLeft{Name: node})
		})
		return true
	})
	pendingEvents = c.collectLeadershipEvents(pendingEvents)
	c.onEvent.Delete(node)
	c.mu.Unlock()

	for _, fn := range pendingEvents {
		fn()
	}
}

// collectLeadershipEvents computes leadership changes and appends event
// dispatch closures to pendingEvents. Must be called with c.mu held.
func (c *Cluster) collectLeadershipEvents(pendingEvents []func()) []func() {
	if len(c.nodes) == 0 {
		if c.leader != "" {
			oldLeader := c.leader
			if value, ok := c.onEvent.Load(oldLeader); ok {
				sendEvent := value.(func(any))
				pendingEvents = append(pendingEvents, func() {
					sendEvent(events.EventNodeSwitchedToFollower{Name: oldLeader})
				})
			}
			c.leader = ""
		}
		return pendingEvents
	}
	leader := c.nodes[0]
	if leader != c.leader {
		oldLeader := c.leader
		if value, ok := c.onEvent.Load(oldLeader); ok {
			sendEvent := value.(func(any))
			pendingEvents = append(pendingEvents, func() {
				sendEvent(events.EventNodeSwitchedToFollower{Name: oldLeader})
			})
		}
		if value, ok := c.onEvent.Load(leader); ok {
			sendEvent := value.(func(any))
			pendingEvents = append(pendingEvents, func() {
				sendEvent(events.EventNodeSwitchedToLeader{Name: leader})
			})
		}
		c.leader = leader
	}
	return pendingEvents
}
