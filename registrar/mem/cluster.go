package mem

import (
	"sync"

	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

func NewCluster() *Cluster {
	return &Cluster{
		routes: make(map[gen.Atom][]gen.Route),
	}
}

type Cluster struct {
	mu      sync.RWMutex
	routes  map[gen.Atom][]gen.Route
	nodes   []gen.Atom
	onEvent sync.Map // gen.Atom -> func(event)
	leader  gen.Atom
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

func (c *Cluster) AddRoutes(node gen.Atom, routes []gen.Route, onEvent func(any)) {
	if node == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.routes[node]; ok {
		return
	}
	c.routes[node] = routes
	c.nodes = append(c.nodes, node)
	c.onEvent.Store(node, onEvent)
	c.onEvent.Range(func(key, value any) bool {
		event := zk.EventNodeJoined{Name: node}
		value.(func(any))(event)
		return true
	})
	c.updateLeadership()
}

func (c *Cluster) RemoveNode(node gen.Atom) {
	if node == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.routes[node]; !ok {
		return
	}
	delete(c.routes, node)
	for i, n := range c.nodes {
		if n == node {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			break
		}
	}
	c.onEvent.Range(func(key, value any) bool {
		event := zk.EventNodeLeft{Name: node}
		value.(func(any))(event)
		return true
	})
	c.updateLeadership()
	c.onEvent.Delete(node)
}

func (c *Cluster) updateLeadership() {
	if len(c.nodes) == 0 {
		if c.leader != "" {
			if value, ok := c.onEvent.Load(c.leader); ok {
				sendEvent := value.(func(any))
				sendEvent(zk.EventNodeSwitchedToFollower{Name: c.leader})
			}
			c.leader = ""
		}
		return
	}
	leader := c.nodes[0]
	if leader != c.leader {
		if value, ok := c.onEvent.Load(c.leader); ok {
			sendEvent := value.(func(any))
			sendEvent(zk.EventNodeSwitchedToFollower{Name: c.leader})
		}
		if value, ok := c.onEvent.Load(leader); ok {
			sendEvent := value.(func(any))
			sendEvent(zk.EventNodeSwitchedToLeader{Name: leader})
		}
		c.leader = leader
	}
}
