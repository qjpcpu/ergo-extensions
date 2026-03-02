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
	route gen.Atom
	book  system.IAddressBook
}

func StartSimpleNode(opts SimpleNodeOptions) (Node, error) {
	book := system.NewAddressBook()
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
	if opts.DefaultRequestTimeout == 0 {
		gen.DefaultRequestTimeout = 30
	} else {
		gen.DefaultRequestTimeout = opts.DefaultRequestTimeout
	}
	options.Network.Acceptors = []gen.AcceptorOptions{
		{
			Host:      "0.0.0.0",
			Port:      opts.Port,
			RouteHost: opts.AdvertiseHost,
			RoutePort: opts.AdvertisePort,
			TCP:       "tcp",
		},
	}
	options.Network.Cookie = str(opts.Cookie, "simple-app-cookie")
	options.Network.InsecureSkipVerify = true
	router := gen.Atom("app_routes")
	opts.MemberSpecs = append(opts.MemberSpecs, gen.ApplicationMemberSpec{
		Name:    router,
		Factory: CreatePool(func() gen.ProcessBehavior { return &myworker{monitorPID: make(map[gen.PID]chan error), book: book} }, opts.NodeForwardWorker),
	})
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
	return &nodeImpl{Node: node, route: router, book: book}, nil
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

func (n *nodeImpl) ForwardSend(to string, msg any) error {
	ch := make(chan nodeResult, 1)
	err := n.Send(n.route, messageNodeSend{
		to:  to,
		msg: msg,
		ch:  ch,
	})
	if err != nil {
		return err
	}
	res := <-ch
	if res.err != nil {
		return res.err
	}
	return nil
}

func (n *nodeImpl) CallLocal(to string, msg any, opts ...CallOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.Send(n.route, messageNodeCallLocal{
		to:      to,
		msg:     msg,
		ch:      ch,
		timeout: option.Timeout,
	})
	if err != nil {
		return nil, err
	}
	res := <-ch
	if res.err != nil {
		return nil, res.err
	}
	return res.response, nil
}

func (n *nodeImpl) ForwardCall(to string, msg any, opts ...CallOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.Send(n.route, messageNodeCall{
		to:      to,
		msg:     msg,
		ch:      ch,
		timeout: option.Timeout,
	})
	if err != nil {
		return nil, err
	}
	res := <-ch
	if res.err != nil {
		return nil, res.err
	}
	return res.response, nil
}

func (n *nodeImpl) ForwardSpawnAndWait(fac gen.ProcessFactory, args ...any) error {
	ch := make(chan error, 1)
	err := n.Send(n.route, messageSpawnProcess{
		Factory: fac,
		Options: gen.ProcessOptions{LinkParent: true},
		Args:    args,
		Ch:      ch,
	})
	if err != nil {
		return err
	}
	return <-ch
}

func (n *nodeImpl) ForwardSpawn(fac gen.ProcessFactory, args ...any) error {
	return n.Send(n.route, messageSpawnProcess{
		Factory: fac,
		Options: gen.ProcessOptions{LinkParent: true},
		Args:    args,
	})
}

func (n *nodeImpl) LocateProcess(process gen.Atom) gen.Atom {
	res, err := n.CallLocal(string(system.WhereIsProcess), system.MessageLocate{Name: process}, CallTimeout(10))
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

func (n *nodeImpl) getOpts(options ...CallOpts) *callopts {
	o := new(callopts)
	for _, fn := range options {
		fn(o)
	}
	return o
}
