package app

import (
	"os"
	"strconv"
	"time"

	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
)

type nodeImpl struct {
	gen.Node
	route gen.Atom
	book  system.IAddressBook
}

func StartSimpleNode(opts SimpleNodeOptions) (Node, error) {
	book := system.NewAddressBook(opts.AddressBookStorage)
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
	options.Network.Acceptors = []gen.AcceptorOptions{{Host: "0.0.0.0", Port: opts.Port, TCP: "tcp"}}
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
	}

	// Start the node
	node, err := ergo.StartNode(gen.Atom(opts.NodeName), options)
	if err != nil {
		return nil, err
	}
	return &nodeImpl{Node: node, route: router, book: book}, nil
}

// GetAdvertiseAddressByENV creates a RoutesMapper that determines the advertise address/port
// by inspecting environment variables.
// It iterates through the provided environment variable names (hostEnv and portEnv)
// and uses the first non-empty value found.
func GetAdvertiseAddressByENV(hostEnv []string, portEnv []string) zk.RoutesMapper {
	var host string
	var port int
	for _, e := range hostEnv {
		if val := os.Getenv(e); val != "" {
			host = val
			break
		}
	}
	for _, e := range portEnv {
		if p, _ := strconv.Atoi(os.Getenv(e)); p > 0 {
			port = p
			break
		}
	}
	return zk.MapRoutesByAdvertiseAddress(host, port)
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

func (n *nodeImpl) ForwardCall(to string, msg any) (any, error) {
	ch := make(chan nodeResult, 1)
	err := n.Send(n.route, messageNodeCall{
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
	p, _ := n.book.Locate(process)
	return p
}

func (n *nodeImpl) AddressBook() system.IAddressBook {
	return n.book
}
