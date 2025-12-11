package system

import (
	"ergo.services/ergo"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

type SimpleNodeOptions struct {
	zk.Options
	NodeName string
	// Optional
	Cookie      string
	MemberSpecs []gen.ApplicationMemberSpec
}

type Node struct {
	gen.Node
	book *AddressBook
}

func (node *Node) GetAddressBook() IAddressBook {
	return node.book
}

func StartSimpleNode(opts SimpleNodeOptions) (gen.Node, error) {
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
	options.Applications = apps
	options.Log.DefaultLogger.Disable = true

	// Start the node
	node, err := ergo.StartNode(gen.Atom(opts.NodeName), options)
	if err != nil {
		return nil, err
	}
	return &Node{Node: node, book: book}, nil
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
