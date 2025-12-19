package system

import (
	"sync/atomic"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

const WhereIsProcess = gen.Atom("whereis")

const all_nodes = gen.Atom("*")

type whereis struct {
	act.Actor
	book              *AddressBook
	broadcastNodeSet  map[gen.Atom]struct{}
	broadcastSeq      uint64
	cancelRebroadcast gen.CancelFunc
	registrar         gen.Registrar

	pid_to_name  map[gen.PID]gen.Atom
	name_to_pid  map[gen.Atom]gen.PID
	processCache atomic.Value
}

func factory_whereis(book *AddressBook) gen.ProcessFactory {
	var v atomic.Value
	v.Store(ProcessInfoList{})
	return func() gen.ProcessBehavior {
		return &whereis{
			book:             book,
			broadcastNodeSet: make(map[gen.Atom]struct{}),
			pid_to_name:      make(map[gen.PID]gen.Atom),
			name_to_pid:      make(map[gen.Atom]gen.PID),
			processCache:     v,
		}
	}
}

func (w *whereis) Init(args ...any) error {
	w.Log().Info("whereis process start up")
	w.SendAfter(w.PID(), inspect_process_list{}, time.Second*1)
	return nil
}

func (w *whereis) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case inspect_process_list:
		w.inspectProcessList()
		w.SendAfter(w.PID(), inspect_process_list{}, time.Second*1)
	case rebroadcast:
		if e.seq != w.broadcastSeq || len(w.broadcastNodeSet) == 0 {
			return nil
		}
		nodes, err := w.fetchAvailableBookNodes()
		if err != nil {
			w.Log().Error("fetch nodes fail %v", err)
			w.broadcastLater(time.Second*3, w.getBroadcastNodeList()...)
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

// Rationale for event handling:
//   - This process only reacts to node joins to speed up convergence on new nodes
//     by immediately pushing the local process snapshot.
//   - Node leaves/offline are not explicitly handled here. The periodic inspector
//     (`inspect_process_list`) invokes `fetchAvailableBookNodes` and then
//     `book.SetAvailableNodes(nodes)`, which computes a diff, removes absent nodes,
//     and cleans their process indices. This keeps `Locate`/`PickNode` from pointing
//     to offline or outdated nodes without requiring leave-event broadcasts.
//   - Recovery/launch logic upon membership changes is handled by `daemon_monitor`.
//   - If stricter real-time reaction to leaves is desired, a `zk.EventNodeLeft`
//     branch can be added to trigger an immediate refresh; however, the snapshot-based
//     cleanup is simpler and resilient to churn and transient registrar events.
func (w *whereis) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
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

func (w *whereis) inspectProcessList() error {
	if err := w.collectProcessList(); err != nil {
		return err
	}
	nodes, err := w.fetchAvailableBookNodes()
	if err != nil {
		w.Log().Error("fetch nodes fail %v", err)
		return err
	}
	w.diffAndBroadcast(nodes, w.Node().Name(), w.processCache.Load().(ProcessInfoList))
	return nil
}

// collectProcessList gets all processes from the current node,
// finds the newly started and recently stopped processes,
// updates the internal cache, and stores the full process list
// into the processCache.
func (w *whereis) collectProcessList() error {
	// Get the list of all running process PIDs on the current node.
	pidList, err := w.Node().ProcessList()
	if err != nil {
		return err
	}

	pidMap := make(map[gen.PID]struct{})
	var added, del []gen.PID
	// Iterate through the current process list to find newly added processes.
	for _, pid := range pidList {
		pidMap[pid] = struct{}{}
		if _, ok := w.pid_to_name[pid]; !ok {
			added = append(added, pid)
		}
	}
	// Iterate through the old process list (pid_to_name) to find deleted (terminated) processes.
	for pid := range w.pid_to_name {
		if _, ok := pidMap[pid]; !ok {
			del = append(del, pid)
		}
	}

	// If there are no added or deleted processes, there is nothing to do.
	if len(added) == 0 && len(del) == 0 {
		return nil
	}

	node := w.Node()
	// Remove deleted processes from the lookup maps.
	for _, pid := range del {
		name := w.pid_to_name[pid]
		// Ensure we only delete the entry if the PID matches,
		// avoiding issues with stale/reused process names.
		if w.name_to_pid[name] == pid {
			delete(w.name_to_pid, name)
		}
		delete(w.pid_to_name, pid)
	}

	// Add new processes to the lookup maps.
	for _, pid := range added {
		if info, err := node.ProcessInfo(pid); err != nil {
			return err
		} else {
			w.pid_to_name[pid] = info.Name
			if info.Name != "" {
				w.name_to_pid[info.Name] = pid
			}
		}
	}

	// Rebuild the full process list from the updated name_to_pid map.
	procList := make(ProcessInfoList, 0, len(w.name_to_pid))
	for name, pid := range w.name_to_pid {
		procList = append(procList, ProcessInfo{
			Name: name,
			PID:  pid,
			Node: node.Name(),
		})
	}

	// Atomically update the process cache with the new list.
	w.processCache.Store(procList)
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
	if cancel := w.cancelRebroadcast; cancel != nil {
		cancel()
		w.cancelRebroadcast = nil
	}
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
	if cancel := w.cancelRebroadcast; cancel != nil {
		cancel()
		w.cancelRebroadcast = nil
	}
	for _, node := range nodes {
		w.broadcastNodeSet[node] = struct{}{}
	}
	w.broadcastSeq++
	if c, err := w.SendAfter(w.PID(), rebroadcast{seq: w.broadcastSeq}, dur); err == nil {
		w.cancelRebroadcast = c
	}
}
