package app

import (
	"errors"
	"fmt"
	"os"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
	"github.com/qjpcpu/registrar/zk"
)

type nodeImpl struct {
	gen.Node
	route        gen.Atom
	book         system.IAddressBook
	queryTimeout int
}

func StartSimpleNode(opts SimpleNodeOptions) (Node, error) {
	book := system.NewAddressBook()
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
	if len(opts.Options.Endpoints) != 0 {
		registrar, err := zk.Create(opts.Options)
		if err != nil {
			return nil, err
		}
		options.Network.Registrar = registrar
	} else if opts.Registrar != nil {
		options.Network.Registrar = opts.Registrar
	} else {
		options.Network.Registrar = mem.Create()
	}
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
	apps := []gen.ApplicationBehavior{newApp(book, opts)}
	options.Applications = append(apps, opts.MoreApps...)

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
	queryTimeout := opts.WhereIsOptions.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = system.DefaultWhereIsOptions().QueryTimeout
	}
	return &nodeImpl{Node: node, route: routeProcessName, book: book, queryTimeout: queryTimeout}, nil
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
	return <-ch
}

func (n *nodeImpl) ForwardSend(to string, msg any, opts ...ForwardOpts) error {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.Send(n.route, messageNodeSend{
		to:     to,
		toNode: option.Node,
		msg:    msg,
		ch:     ch,
	})
	if err != nil {
		return err
	}
	return (<-ch).err
}

func (n *nodeImpl) ForwardCall(to string, msg any, opts ...ForwardOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.Send(n.route, messageNodeCall{
		to:      to,
		toNode:  option.Node,
		msg:     msg,
		timeout: option.Timeout,
		ch:      ch,
	})
	if err != nil {
		return nil, err
	}
	res := <-ch
	return res.response, res.err
}

func (n *nodeImpl) ForwardSpawn(name string, fac gen.ProcessFactory, args ...any) (gen.PID, error) {
	ch := make(chan nodeResult, 1)
	err := n.Send(n.route, messageSpawnProcess{
		Name:    gen.Atom(name),
		Factory: fac,
		Options: gen.ProcessOptions{LinkParent: true},
		Args:    args,
		Ch:      ch,
	})
	if err != nil {
		return gen.PID{}, err
	}
	res := <-ch
	if res.err != nil {
		return gen.PID{}, res.err
	}
	pid, _ := res.response.(gen.PID)
	return pid, nil
}

func (n *nodeImpl) LocateProcess(process gen.Atom) gen.Atom {
	owner := n.book.PickDirectoryNode(process)
	if owner == "" {
		return ""
	}
	res, err := n.ForwardCall(
		string(system.WhereIsProcess),
		system.MessageLocate{Name: process},
		ForwardNode(owner),
		ForwardTimeout(n.queryTimeout),
	)
	if err != nil {
		return ""
	}
	if p, ok := res.(gen.Atom); ok {
		return p
	}
	return ""
}

func (n *nodeImpl) AddressBook() system.IAddressBook {
	return n.book
}

func (n *nodeImpl) getOpts(options ...ForwardOpts) *forwardopts {
	o := new(forwardopts)
	for _, fn := range options {
		fn(o)
	}
	return o
}
