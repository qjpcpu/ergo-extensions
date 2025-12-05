package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/app/system/inspect"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

const WhereIsProcess = gen.Atom("whereis")

const all_nodes = gen.Atom("*")

type whereis struct {
	act.Actor
	book             *AddressBook
	broadcastNodeSet map[gen.Atom]struct{}
	nextBroadcastAt  int64
	registrar        gen.Registrar
}

func factory_whereis(book *AddressBook) gen.ProcessFactory {
	return func() gen.ProcessBehavior { return &whereis{book: book, broadcastNodeSet: make(map[gen.Atom]struct{})} }
}

func (w *whereis) Init(args ...any) error {
	w.Log().Info("whereis process start up")
	w.SendAfter(w.PID(), start_init{}, time.Second*1)
	return nil
}

func (w *whereis) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case start_init:
		w.setupRegistrarMonitoring()
	case rebroadcast:
		if w.nextBroadcastAt > time.Now().Unix() || len(w.broadcastNodeSet) == 0 {
			return nil
		}
		nodes, err := w.fetchAvailableBookNodes()
		if err != nil {
			w.Log().Error("fetch nodes fail %v", err)
			w.SendAfter(w.PID(), e, time.Second*3)
			return nil
		}
		if _, ok := w.broadcastNodeSet[all_nodes]; ok {
			w.broadcastProcesses(nodes, w.Node().Name(), w.book.GetProcessList(w.Node().Name()))
		} else if len(w.broadcastNodeSet) > 0 {
			w.broadcastProcesses(interactNodes(nodes, w.getBroadcastNodeList()), w.Node().Name(), w.book.GetProcessList(w.Node().Name()))
		}
	case MessageProcesses:
		if _, err := w.fetchAvailableBookNodes(); err != nil {
			w.Log().Error("fetch nodes fail %v", err)
			return err
		}
		w.book.SetProcess(e.Node, e.ProcessList...)
		w.Log().Info("received %d process snapshot on %s %s", len(e.ProcessList), e.Node, shortInfo(e.ProcessList))
	case MessageProcessChanged:
		w.book.AddProcess(e.Node, e.UpProcess...)
		w.book.RemoveProcess(e.Node, e.DownProcess...)
		w.Log().Info("received +%d%s/-%d%s process on %s", len(e.UpProcess), shortInfo(e.UpProcess), len(e.DownProcess), shortInfo(e.DownProcess), e.Node)
	}
	return nil
}

func (w *whereis) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch e := request.(type) {
	case MessageLocate:
		if p, ok := w.book.Locate(e.Name); ok {
			return p.Node, nil
		} else {
			return gen.Atom(""), nil
		}
	case MessageGetAddressBook:
		return MessageAddressBook{Book: w.book}, nil
	}
	return w.PID(), nil
}

func (w *whereis) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
	case inspect.MessageInspectProcessList:
		nodes, err := w.fetchAvailableBookNodes()
		if err != nil {
			w.Log().Error("fetch nodes fail %v", err)
			return err
		}
		processList := filterProcessList(mapGenProcessList(e.Node, e.Processes))
		w.diffAndBroadcast(nodes, e.Node, processList)
	case zk.EventNodeJoined:
		node := w.Node().Name()
		list := w.book.GetProcessList(node)
		if err := w.SendImportant(gen.ProcessID{Node: e.Name, Name: WhereIsProcess}, MessageProcesses{Node: node, ProcessList: list}); err != nil {
			if err != gen.ErrProcessUnknown {
				w.Log().Info("broadcast %d process to new node %s fail %v", len(list), e.Name, err)
			}
			w.broadcastLater(time.Second*3, e.Name)
		} else {
			w.Log().Info("broadcast %d process to new node %s OK", len(list), e.Name)
		}
	}
	return nil
}

// Terminate invoked on a termination process
func (w *whereis) Terminate(reason error) {
	w.Log().Info("whereis process terminated with reason: %s", reason)
}

func (w *whereis) diffAndBroadcast(nodes []gen.Atom, selfNode gen.Atom, newList []ProcessInfo) {
	oldList := w.book.GetProcessList(selfNode)

	newMap := make(map[gen.Atom]ProcessInfo)
	for _, p := range newList {
		newMap[p.Name] = p
	}

	oldMap := make(map[gen.Atom]ProcessInfo)
	for _, p := range oldList {
		oldMap[p.Name] = p
	}

	msg := MessageProcessChanged{Node: selfNode}
	for name, p := range newMap {
		if _, ok := oldMap[name]; !ok {
			msg.UpProcess = append(msg.UpProcess, p)
		}
	}

	for name, p := range oldMap {
		if _, ok := newMap[name]; !ok {
			msg.DownProcess = append(msg.DownProcess, p)
		}
	}

	w.book.SetProcess(selfNode, newList...)
	if len(msg.DownProcess) > 0 {
		w.book.RemoveProcess(selfNode, msg.DownProcess...)
	}
	if len(msg.UpProcess) > 0 || len(msg.DownProcess) > 0 {
		w.broadcastProcessChanged(nodes, msg)
	}
}

func (w *whereis) setupRegistrarMonitoring() error {
	info, err := w.Call(inspect.Name, inspect.RequestInspectProcessList{})
	if err != nil {
		w.Log().Error("inspect processlist fail %v", err)
		w.SendAfter(w.PID(), start_init{}, time.Second)
		return err
	}
	psInfo := info.(inspect.ResponseInspectProcessList)
	evt := psInfo.Event
	if _, err := w.MonitorEvent(evt); err != nil {
		w.Log().Error("monitor processlist fail %v", err)
		w.SendAfter(w.PID(), start_init{}, time.Second)
		return err
	}
	return nil

}

func (w *whereis) fetchAvailableBookNodes() ([]gen.Atom, error) {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return nil, err
		} else {
			w.registrar = registrar
		}
		event, err := registrar.Event()
		if err != nil {
			return nil, err
		}
		if _, err = w.MonitorEvent(event); err != nil {
			return nil, err
		}
	}
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodes = uniqNodes(append(nodes, w.Node().Name()))
	w.book.SetAvailableNodes(nodes)
	return nodes, nil
}

func (w *whereis) broadcastProcesses(nodes []gen.Atom, node gen.Atom, ps []ProcessInfo) (err error) {
	var failedNodes []gen.Atom
	for _, tnode := range nodes {
		if tnode != w.Node().Name() {
			if err0 := w.SendImportant(gen.ProcessID{Node: tnode, Name: WhereIsProcess}, MessageProcesses{Node: node, ProcessList: ps}); err0 != nil {
				failedNodes = append(failedNodes, tnode)
				w.Log().Warning("push process list to %s fail %v", tnode, err0)
				err = err0
			}
		}
	}

	clear(w.broadcastNodeSet)
	w.nextBroadcastAt = 0
	if len(failedNodes) > 0 {
		w.broadcastLater(time.Second*3, failedNodes...)
	}
	return err
}

func (w *whereis) broadcastProcessChanged(nodes []gen.Atom, msg MessageProcessChanged) {
	var failedNodes []gen.Atom
	for _, tnode := range nodes {
		if tnode != w.Node().Name() {
			t := tnode
			if err := w.SendImportant(gen.ProcessID{Node: t, Name: WhereIsProcess}, msg); err != nil {
				failedNodes = append(failedNodes, t)
				w.Log().Warning("push process changed to %s fail %v", t, err)
			}
		}
	}
	if len(failedNodes) > 0 {
		w.broadcastLater(time.Second*3, failedNodes...)
	}
}

func (w *whereis) getBroadcastNodeList() (ret []gen.Atom) {
	for k := range w.broadcastNodeSet {
		ret = append(ret, k)
	}
	return
}

func (w *whereis) broadcastLater(dur time.Duration, nodes ...gen.Atom) {
	w.nextBroadcastAt = max(w.nextBroadcastAt, time.Now().Add(dur).Unix())
	for _, node := range nodes {
		w.broadcastNodeSet[node] = struct{}{}
	}
	w.SendAfter(w.PID(), rebroadcast{}, dur)
}
