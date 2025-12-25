package app

import (
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

// Node is the minimal interface returned by StartSimpleNode.
//
// It wraps an Ergo gen.Node and provides helper methods to locate a named
// process via the shared address book, and to forward sends/calls to the node
// currently hosting that process.
type Node interface {
	gen.Node
	LocateProcess(process gen.Atom) gen.Atom
	ForwardCall(to string, msg any) (any, error)
	ForwardSend(to string, msg any) error
	WaitPID(pid gen.PID) error
	AddressBook() system.IAddressBook
}

type CronJob = system.CronJob

type SimpleNodeOptions struct {
	zk.Options        // ZooKeeper registrar options.
	NodeName   string // Node name.
	// Optional
	Cookie                string                      // Cluster cookie (must match across nodes).
	MoreApps              []gen.ApplicationBehavior   // Extra applications to start on the node.
	MemberSpecs           []gen.ApplicationMemberSpec // Additional application members to start.
	NodeForwardWorker     int64                       // Worker count for forwarding calls/sends.
	LogLevel              gen.LogLevel                // Node log level.
	DefaultLogOptions     gen.DefaultLoggerOptions    // Default logger configuration.
	CronJobs              []CronJob                   // Cron jobs for `system.CronJobProcess`.
	DefaultRequestTimeout int                         // Default request timeout (seconds).
	SyncProcessInterval   time.Duration               // Whereis sync interval for pulling remote changes.
	ProcessChangeBuffer   int                         // Whereis change buffer size for compacting deltas.
}

type nodeImpl struct {
	gen.Node
	forwardPID gen.PID
	book       *system.AddressBook
}

func StartSimpleNode(opts SimpleNodeOptions) (Node, error) {
	book := system.NewAddressBook()
	var options gen.NodeOptions
	if len(opts.Options.Endpoints) != 0 {
		registrar, err := zk.Create(opts.Options)
		if err != nil {
			return nil, err
		}
		options.Network.Registrar = registrar
	} else {
		options.Network.Registrar = mem.Create()
	}
	if opts.DefaultRequestTimeout == 0 {
		gen.DefaultRequestTimeout = 30
	} else {
		gen.DefaultRequestTimeout = opts.DefaultRequestTimeout
	}
	options.Network.Acceptors = []gen.AcceptorOptions{{Host: "0.0.0.0", TCP: "tcp"}}
	options.Network.Cookie = str("simple-app-cookie-123")
	options.Network.InsecureSkipVerify = true
	apps := []gen.ApplicationBehavior{&simpleApp{
		book:                    book,
		cron:                    opts.CronJobs,
		MemberSpecs:             opts.MemberSpecs,
		SyncAddressBookInterval: opts.SyncProcessInterval,
		AddressBookBuffer:       opts.ProcessChangeBuffer,
	}}
	options.Applications = append(apps, opts.MoreApps...)

	options.Log.Level = opts.LogLevel
	options.Log.DefaultLogger = opts.DefaultLogOptions
	if options.Log.DefaultLogger.TimeFormat == "" {
		options.Log.DefaultLogger.TimeFormat = time.DateTime
	}

	// Start the node
	node, err := ergo.StartNode(gen.Atom(opts.NodeName), options)
	if err != nil {
		return nil, err
	}

	forwardPID, err := node.Spawn(CreatePool(func() gen.ProcessBehavior {
		return &myworker{monitorPID: make(map[gen.PID]chan error), book: book}
	}, opts.NodeForwardWorker), gen.ProcessOptions{})
	if err != nil {
		return nil, err
	}
	return &nodeImpl{Node: node, forwardPID: forwardPID, book: book}, nil
}

type simpleApp struct {
	book                    *system.AddressBook
	cron                    []CronJob
	MemberSpecs             []gen.ApplicationMemberSpec
	SyncAddressBookInterval time.Duration
	AddressBookBuffer       int
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	members := append([]gen.ApplicationMemberSpec{
		system.ApplicationMemberSepc(system.ApplicationMemberSepcOptions{
			AddressBook:             app.book,
			CronJobs:                app.cron,
			SyncAddressBookInterval: app.SyncAddressBookInterval,
			AddressBookBuffer:       app.AddressBookBuffer,
		})},
		app.MemberSpecs...,
	)
	return gen.ApplicationSpec{
		Name:        "simple_app",
		Description: "Simple application",
		Mode:        gen.ApplicationModePermanent,
		Group:       members,
		Depends:     gen.ApplicationDepends{Network: true},
	}, nil
}

func (app *simpleApp) Start(mode gen.ApplicationMode) {}
func (app *simpleApp) Terminate(reason error)         {}

func str(list ...string) string {
	for _, e := range list {
		if e != "" {
			return e
		}
	}
	return ""
}

func CreatePool(workerFactory gen.ProcessFactory, size int64) gen.ProcessFactory {
	return func() gen.ProcessBehavior { return &myPool{size: size, fac: workerFactory} }
}

type myPool struct {
	act.Pool
	size int64
	fac  gen.ProcessFactory
}

func (p *myPool) Init(args ...any) (act.PoolOptions, error) {
	if p.size == 0 {
		p.size = 3
	}
	opts := act.PoolOptions{
		WorkerFactory: p.fac,
		PoolSize:      p.size,
	}

	return opts, nil
}

type myworker struct {
	act.Actor
	monitorPID map[gen.PID]chan error
	book       system.IAddressBook
}

func (w *myworker) Init(args ...any) error {
	return nil
}

type nodeResult struct {
	response any
	err      error
}

type messageNodeSend struct {
	to  string
	msg any
	ch  chan nodeResult
}

type messageNodeCall struct {
	to  string
	msg any
	ch  chan nodeResult
}

type messageWaitProcess struct {
	PID gen.PID
	Ch  chan error
}

func (w *myworker) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageNodeSend:
		if p, ok := w.book.Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			e.ch <- nodeResult{err: w.Send(gen.Atom(e.to), e.msg)}
		} else {
			e.ch <- nodeResult{err: w.Send(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)}
		}
	case messageNodeCall:
		if p, ok := w.book.Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			res, err := w.Call(gen.Atom(e.to), e.msg)
			e.ch <- nodeResult{response: res, err: err}
		} else {
			res, err := w.Call(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)
			e.ch <- nodeResult{response: res, err: err}
		}
	case messageWaitProcess:
		if err := w.MonitorPID(e.PID); err != nil {
			e.Ch <- err
			return nil
		} else {
			w.monitorPID[e.PID] = e.Ch
		}
	case gen.MessageDownPID:
		if ch, ok := w.monitorPID[e.PID]; ok {
			delete(w.monitorPID, e.PID)
			if e.Reason == gen.TerminateReasonNormal {
				ch <- nil
			} else {
				ch <- e.Reason
			}
			w.Log().Info("PID:%s exit with reason %v", e.PID, e.Reason)
			w.DemonitorPID(e.PID)
		}
	}
	return nil
}

func (n *nodeImpl) WaitPID(pid gen.PID) error {
	ch := make(chan error, 1)
	err := n.Send(n.forwardPID, messageWaitProcess{
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
	err := n.Send(n.forwardPID, messageNodeSend{
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

func (n *nodeImpl) ForwardCall(to string, msg any) (any, error) {
	ch := make(chan nodeResult, 1)
	err := n.Send(n.forwardPID, messageNodeCall{
		to:  to,
		msg: msg,
		ch:  ch,
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

func (n *nodeImpl) LocateProcess(process gen.Atom) gen.Atom {
	p, _ := n.book.Locate(process)
	return p.Node
}

func (n *nodeImpl) AddressBook() system.IAddressBook {
	return n.book
}
