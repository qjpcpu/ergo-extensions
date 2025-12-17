package mem

import (
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

type client struct {
	routes   gen.RegisterRoutes
	node     gen.NodeRegistrar
	eventRef gen.Ref
	event    gen.Event
}

func Create() gen.Registrar {
	return &client{}
}

func (c *client) Shutdown() (err error) {
	return
}

//
// gen.Resolver interface implementation
//

func (c *client) Resolve(name gen.Atom) ([]gen.Route, error) {
	return c.routes.Routes, nil
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
	c.routes = routes
	eventName := gen.Atom("memory-node-event")
	eventRef, err := node.RegisterEvent(eventName, gen.EventOptions{Buffer: 64})
	if err != nil {
		return gen.StaticRoutes{}, err
	}
	c.eventRef = eventRef
	c.event = gen.Event{Name: eventName, Node: node.Name()}
	node.SendEvent(eventName, eventRef, gen.MessageOptions{}, zk.EventNodeJoined{Name: node.Name()})
	node.SendEvent(eventName, eventRef, gen.MessageOptions{}, zk.EventNodeSwitchedToLeader{Name: node.Name()})
	return gen.StaticRoutes{}, nil
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
	return []gen.Atom{}, nil
}

func (c *client) ConfigItem(item string) (any, error) {
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
		SupportRegisterApplication: true,
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
