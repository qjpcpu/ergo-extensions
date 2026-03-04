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
	book system.IAddressBook
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
	return &nodeImpl{Node: node, book: book}, nil
}

func str(list ...string) string {
	for _, e := range list {
		if e != "" {
			return e
		}
	}
	return ""
}

func (n *nodeImpl) spawnTemp(fn func(*tempActor)) error {
	_, err := n.Spawn(func() gen.ProcessBehavior {
		return &tempActor{fn: fn, book: n.book}
	}, gen.ProcessOptions{})
	return err
}

func (n *nodeImpl) WaitPID(pid gen.PID) error {
	ch := make(chan error, 1)
	_, err := n.Spawn(func() gen.ProcessBehavior {
		return &monitorActor{
			ch:  ch,
			pid: pid,
			setup: func(w *monitorActor) error {
				return w.MonitorPID(pid)
			},
		}
	}, gen.ProcessOptions{})
	if err != nil {
		return err
	}
	return <-ch
}

func (n *nodeImpl) ForwardSend(to string, msg any, opts ...ForwardOpts) error {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.spawnTemp(func(w *tempActor) {
		if option.Node != "" {
			if option.Node == w.Node().Name() {
				ch <- nodeResult{err: w.Send(gen.Atom(to), msg)}
			} else {
				ch <- nodeResult{err: w.SendImportant(gen.ProcessID{Node: option.Node, Name: gen.Atom(to)}, msg)}
			}
		} else {
			p, locErr := w.book.QueryBy(w, system.QueryOption{}).Locate(gen.Atom(to))
			if locErr != nil || p == "" || w.Node().Name() == p {
				ch <- nodeResult{err: w.Send(gen.Atom(to), msg)}
			} else {
				ch <- nodeResult{err: w.SendImportant(gen.ProcessID{Node: p, Name: gen.Atom(to)}, msg)}
			}
		}
	})
	if err != nil {
		return err
	}
	return (<-ch).err
}

func (n *nodeImpl) ForwardCall(to string, msg any, opts ...ForwardOpts) (any, error) {
	ch := make(chan nodeResult, 1)
	option := n.getOpts(opts...)
	err := n.spawnTemp(func(w *tempActor) {
		var res any
		var callErr error
		var p gen.Atom
		if option.Node != "" {
			p = option.Node
		} else {
			p, callErr = w.book.QueryBy(w, system.QueryOption{Timeout: option.Timeout}).Locate(gen.Atom(to))
		}
		if callErr != nil || p == "" || w.Node().Name() == p {
			if option.Timeout > 0 {
				res, callErr = w.CallWithTimeout(gen.Atom(to), msg, option.Timeout)
			} else {
				res, callErr = w.Call(gen.Atom(to), msg)
			}
		} else {
			if option.Timeout > 0 {
				res, callErr = w.CallWithTimeout(gen.ProcessID{Node: p, Name: gen.Atom(to)}, msg, option.Timeout)
			} else {
				res, callErr = w.CallImportant(gen.ProcessID{Node: p, Name: gen.Atom(to)}, msg)
			}
		}
		ch <- nodeResult{response: res, err: callErr}
	})
	if err != nil {
		return nil, err
	}
	res := <-ch
	return res.response, res.err
}

func (n *nodeImpl) ForwardSpawnAndWait(fac gen.ProcessFactory, args ...any) error {
	ch := make(chan error, 1)
	_, err := n.Spawn(func() gen.ProcessBehavior {
		return &monitorActor{
			ch: ch,
			setup: func(w *monitorActor) error {
				pid, err := w.Spawn(fac, gen.ProcessOptions{LinkParent: true}, args...)
				if err != nil {
					return err
				}
				w.pid = pid
				if err := w.MonitorPID(pid); err != nil {
					w.Node().Kill(pid)
					return err
				}
				return nil
			},
		}
	}, gen.ProcessOptions{})
	if err != nil {
		return err
	}
	return <-ch
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
		ForwardTimeout(10),
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
