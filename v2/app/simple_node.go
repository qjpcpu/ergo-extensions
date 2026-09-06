package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/system"
)

type nodeImpl struct {
	gen.Node
	route    gen.Atom
	book     system.IAddressBook
	router   *system.ActorRouter
	routes   ActorRoutes
	stopped  chan struct{}
	stopOnce sync.Once
}

func StartSimpleNode(opts SimpleNodeOptions) (Node, error) {
	router, err := system.NewActorRouter(opts.ActorRoutePersistence, opts.ActorRouterOptions)
	if err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			router.Close()
		}
	}()
	book := system.NewAddressBook()
	routes := newActorRoutes(book, router)
	netFamily, err := acceptorNetFamily(opts.AcceptorNetFamily)
	if err != nil {
		return nil, err
	}
	acceptorHost := acceptorHost(netFamily)
	if opts.NodeName == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		if host == "" {
			return nil, errors.New("blank NodeName")
		}
		opts.NodeName = fmt.Sprintf("node@%s", host)
	}
	var options gen.NodeOptions
	if opts.Registrar == nil {
		return nil, errors.New("missing Registrar")
	}
	options.Network.Registrar = opts.Registrar
	options.Network.Acceptors = []gen.AcceptorOptions{
		{
			Host:      acceptorHost,
			Port:      opts.Port,
			RouteHost: opts.AdvertiseHost,
			RoutePort: opts.AdvertisePort,
			TCP:       netFamily,
		},
	}
	options.Network.Cookie = str(opts.Cookie, "simple-app-cookie")
	options.Network.InsecureSkipVerify = true
	apps := []gen.ApplicationBehavior{newApp(book, router, routes, opts)}
	if opts.MoreApps != nil {
		apps = append(apps, opts.MoreApps(routes)...)
	}
	options.Applications = apps

	options.Log.Level = opts.LogLevel
	options.Log.DefaultLogger = opts.DefaultLogOptions
	if options.Log.DefaultLogger.TimeFormat == "" {
		options.Log.DefaultLogger.TimeFormat = time.DateTime
		options.Log.DefaultLogger.DisableBanner = true
	}

	// Start the node
	node, err := ergo.StartNode(gen.Atom(opts.NodeName), options)
	if err != nil {
		return nil, err
	}
	impl := &nodeImpl{Node: node, route: routeProcessName, book: book, router: router, routes: routes, stopped: make(chan struct{})}
	started = true
	return impl, nil
}

func (n *nodeImpl) Stop() {
	n.signalStop()
	n.Node.Stop()
	n.router.Close()
}

func (n *nodeImpl) StopForce() {
	n.signalStop()
	n.Node.StopForce()
	n.router.Close()
}

func (n *nodeImpl) ActorRoutes() ActorRoutes {
	return n.routes
}

func acceptorNetFamily(value string) (string, error) {
	if value == "" {
		return "tcp", nil
	}
	switch value {
	case "tcp", "tcp4", "tcp6":
		return value, nil
	default:
		return "", fmt.Errorf("invalid AcceptorNetFamily %q: supported values are tcp, tcp4, tcp6", value)
	}
}

func acceptorHost(netFamily string) string {
	if netFamily == "tcp6" {
		return "::"
	}
	return "0.0.0.0"
}

func str(list ...string) string {
	for _, e := range list {
		if e != "" {
			return e
		}
	}
	return ""
}

func (n *nodeImpl) WaitPID(pid gen.PID) error {
	ch := make(chan error, 1)
	err := n.Send(n.route, messageWaitProcess{
		PID: pid,
		Ch:  ch,
	})
	if err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-n.stopped:
		return gen.ErrNodeTerminated
	}
}

func (n *nodeImpl) ForwardSend(to string, msg any, opts ...ForwardOpts) error {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	res := n.forward(option, ch, messageNodeSend{
		to:        to,
		toNode:    option.Node,
		msg:       msg,
		important: option.Important,
		ch:        ch,
	})
	return res.err
}

func (n *nodeImpl) ForwardSendPID(to gen.PID, msg any, opts ...ForwardOpts) error {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	res := n.forward(option, ch, messagePIDSend{
		to:        to,
		msg:       msg,
		important: option.Important,
		ch:        ch,
	})
	return res.err
}

func (n *nodeImpl) ForwardCall(to string, msg any, opts ...ForwardOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	res := n.forward(option, ch, messageNodeCall{
		to:        to,
		toNode:    option.Node,
		msg:       msg,
		timeout:   option.Timeout,
		important: option.Important,
		ch:        ch,
	})
	return res.response, res.err
}

func (n *nodeImpl) ForwardCallPID(to gen.PID, msg any, opts ...ForwardOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	res := n.forward(option, ch, messagePIDCall{
		to:        to,
		msg:       msg,
		timeout:   option.Timeout,
		important: option.Important,
		ch:        ch,
	})
	return res.response, res.err
}

func (n *nodeImpl) ForwardSpawn(name string, fac gen.ProcessFactory, args ...any) (gen.PID, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts()
	res := n.forward(option, ch, messageSpawnProcess{
		Name:    gen.Atom(name),
		Factory: fac,
		Options: gen.ProcessOptions{LinkParent: true},
		Args:    args,
		Ch:      ch,
	})
	if res.err != nil {
		return gen.PID{}, res.err
	}
	pid, _ := res.response.(gen.PID)
	return pid, nil
}

func (n *nodeImpl) Topology() Topology {
	return n.book
}

func (n *nodeImpl) getOpts(options ...ForwardOpts) *forwardopts {
	o := new(forwardopts)
	for _, fn := range options {
		fn(o)
	}
	return o
}

func (n *nodeImpl) signalStop() {
	n.stopOnce.Do(func() {
		if n.stopped != nil {
			close(n.stopped)
		}
	})
}

func (n *nodeImpl) forward(options *forwardopts, ch chan nodeResult, message any) nodeResult {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = gen.DefaultRequestTimeout
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(timeout)*time.Second)
	defer cancel()
	select {
	case <-n.stopped:
		return nodeResult{err: gen.ErrNodeTerminated}
	default:
	}
	if err := ctx.Err(); err != nil {
		return nodeResult{err: forwardContextError(err)}
	}
	if err := n.Send(n.route, forwardRequest{ctx: ctx, message: message, ch: ch}); err != nil {
		return nodeResult{err: err}
	}
	select {
	case result := <-ch:
		return result
	case <-ctx.Done():
		return nodeResult{err: forwardContextError(ctx.Err())}
	case <-n.stopped:
		return nodeResult{err: gen.ErrNodeTerminated}
	}
}
func forwardContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return gen.ErrTimeout
	}
	return err
}
