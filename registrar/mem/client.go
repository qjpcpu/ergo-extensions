package mem

import (
	"ergo.services/ergo/gen"
)

type client struct {
	cluster  *Cluster
	node     gen.NodeRegistrar
	eventRef gen.Ref
	event    gen.Event
}

func CreateWithCluster(c *Cluster) gen.Registrar {
	return &client{cluster: c}
}

func Create() gen.Registrar {
	return CreateWithCluster(NewCluster())
}

func (c *client) Shutdown() (err error) {
	if node := c.node; node != nil {
		c.cluster.RemoveNode(node.Name())
	}
	return
}

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	return c.cluster.GetRoutes(name), nil
}

// ResolveApplication returns all known routes for a given application name, excluding the routes on the node itself.
func (c *client) ResolveApplication(name gen.Atom) ([]gen.ApplicationRoute, error) {
	return nil, gen.ErrNoRoute
}

func (c *client) ResolveProxy(name gen.Atom) ([]gen.ProxyRoute, error) {
	// Proxy routing is not supported in zk registrar implementation
	return nil, gen.ErrNoRoute
}

const (
	RegistrarVersion = "R1"
	RegistrarName    = "Memory Client"
)

// gen.Registrar interface implementation
func (c *client) Register(node gen.NodeRegistrar, routes gen.RegisterRoutes) (gen.StaticRoutes, error) {
	c.node = node
	eventName := gen.Atom("memory-node-event")
	eventRef, err := node.RegisterEvent(eventName, gen.EventOptions{Buffer: 64})
	if err != nil {
		return gen.StaticRoutes{}, err
	}
	c.eventRef = eventRef
	c.event = gen.Event{Name: eventName, Node: node.Name()}
	c.cluster.AddRoutes(node.Name(), routes.Routes, c.sendEvent)
	return gen.StaticRoutes{}, nil
}

func (c *client) sendEvent(evt any) {
	if node := c.node; node != nil {
		node.SendEvent(c.event.Name, c.eventRef, gen.MessageOptions{}, evt)
	}
}

func (c *client) Resolver() gen.Resolver {
	return c
}

func (c *client) RegisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}
func (c *client) UnregisterProxy(to gen.Atom) error {
	return gen.ErrUnsupported
}

func (c *client) RegisterApplicationRoute(route gen.ApplicationRoute) error {
	return gen.ErrUnsupported
}

func (c *client) UnregisterApplicationRoute(name gen.Atom) error {
	return gen.ErrUnsupported
}

// Nodes returns a list of all discovered nodes exclude self in the cluster
func (c *client) Nodes() ([]gen.Atom, error) {
	var list []gen.Atom
	for _, item := range c.cluster.GetNodes() {
		if c.node == nil || item != c.node.Name() {
			list = append(list, item)
		}
	}
	return list, nil
}

func (c *client) ConfigItem(item string) (any, error) {
	if node := c.node; node != nil {
		return c.cluster.GetLeader(), nil
	}
	return nil, gen.ErrUnsupported
}

func (c *client) Config(items ...string) (map[string]any, error) {
	return nil, gen.ErrUnsupported
}

func (c *client) Event() (gen.Event, error) {
	return c.event, nil
}

func (c *client) Info() gen.RegistrarInfo {
	return gen.RegistrarInfo{
		EmbeddedServer:             false,
		Version:                    c.Version(),
		SupportConfig:              false,
		SupportEvent:               true,
		SupportRegisterProxy:       false,
		SupportRegisterApplication: false,
	}
}

func (c *client) Version() gen.Version {
	return gen.Version{
		Name:    RegistrarName,
		Release: RegistrarVersion,
		License: gen.LicenseMIT,
	}
}

func (c *client) Terminate() {
	c.Shutdown()
}
