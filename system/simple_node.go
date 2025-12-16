package system

import (
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

type SimpleNodeOptions struct {
	zk.Options
	NodeName string
	// Optional
	Cookie            string
	MoreApps          []gen.ApplicationBehavior
	MemberSpecs       []gen.ApplicationMemberSpec
	NodeForwardWorker int64
	ObserverAddress   string
	LogLevel          gen.LogLevel
	DefaultLogOptions gen.DefaultLoggerOptions
}

type Node struct {
	gen.Node
	forwardPID gen.PID
}

func StartSimpleNode(opts SimpleNodeOptions) (*Node, error) {
	book := NewAddressBook()
	var options gen.NodeOptions
	registrar, err := zk.Create(opts.Options)
	if err != nil {
		return nil, err
	}
	options.Network.Registrar = registrar
	options.Network.Acceptors = []gen.AcceptorOptions{{Host: "0.0.0.0", TCP: "tcp"}}
	options.Network.Cookie = str("simple-app-cookie-123")
	options.Network.InsecureSkipVerify = true
	apps := []gen.ApplicationBehavior{&simpleApp{book: book, MemberSpecs: opts.MemberSpecs}}
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

	forwardPID, err := node.Spawn(CreatePool(func() gen.ProcessBehavior { return &myworker{} }, opts.NodeForwardWorker), gen.ProcessOptions{})
	if err != nil {
		return nil, err
	}
	return &Node{Node: node, forwardPID: forwardPID}, nil
}

type simpleApp struct {
	book        *AddressBook
	MemberSpecs []gen.ApplicationMemberSpec
}

func (app *simpleApp) Load(node gen.Node, args ...any) (gen.ApplicationSpec, error) {
	members := append([]gen.ApplicationMemberSpec{ApplicationMemberSepc(ApplicationMemberSepcOptions{AddressBook: app.book})}, app.MemberSpecs...)
	return gen.ApplicationSpec{
		Name:        "simpleapp",
		Description: "Simple application",
		Mode:        gen.ApplicationModePermanent,
		Group:       members,
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

func (w *myworker) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageNodeSend:
		if p, ok := GetAddressBook().Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			e.ch <- nodeResult{err: w.Send(gen.Atom(e.to), e.msg)}
		} else {
			e.ch <- nodeResult{err: w.Send(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)}
		}
	case messageNodeCall:
		if p, ok := GetAddressBook().Locate(gen.Atom(e.to)); !ok || w.Node().Name() == p.Node {
			res, err := w.Call(gen.Atom(e.to), e.msg)
			e.ch <- nodeResult{response: res, err: err}
		} else {
			res, err := w.Call(gen.ProcessID{Node: p.Node, Name: gen.Atom(e.to)}, e.msg)
			e.ch <- nodeResult{response: res, err: err}
		}
	}
	return nil
}

func (n *Node) ForwardSend(to string, msg any) error {
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

func (n *Node) ForwardCall(to string, msg any) (any, error) {
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

func (n *Node) LocateProcess(process gen.Atom) gen.Atom {
	p, _ := GetAddressBook().Locate(process)
	return p.Node
}
