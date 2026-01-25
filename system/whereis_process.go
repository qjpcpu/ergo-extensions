package system

import (
	"math/rand"
	"strconv"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

const (
	WhereIsProcess = gen.Atom("extensions_whereis")
)

type whereis struct {
	act.Actor
	book      *AddressBook
	registrar gen.Registrar

	selfVersion  ProcessVersion
	nodeVersions map[gen.Atom]ProcessVersion

	pidToName     map[gen.PID]gen.Atom
	nameToBirthAt map[gen.Atom]int64
	nameToPID     map[gen.Atom]gen.PID
	// only includes named processes
	processCache       *zk.AtomicValue[ProcessInfoList]
	inspectInterval    time.Duration
	antiEntropyCounter int
	topologyChangeID   int64
}

func factoryWhereIs(book *AddressBook, inspectInterval time.Duration) gen.ProcessFactory {
	if inspectInterval == 0 {
		inspectInterval = time.Second * 3
	}
	return func() gen.ProcessBehavior {
		return &whereis{
			book:            book,
			pidToName:       make(map[gen.PID]gen.Atom),
			nameToPID:       make(map[gen.Atom]gen.PID),
			nameToBirthAt:   make(map[gen.Atom]int64),
			processCache:    zk.NewAtomicValue[ProcessInfoList](),
			selfVersion:     NewVersion(),
			nodeVersions:    make(map[gen.Atom]ProcessVersion),
			inspectInterval: inspectInterval,
		}
	}
}

func (w *whereis) Init(args ...any) error {
	w.SendAfter(w.PID(), messageInit{}, time.Second)
	return nil
}

func (w *whereis) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageInit:
		if err := w.setup(); err != nil {
			w.SendAfter(w.PID(), messageInit{}, time.Second)
			return nil
		}
		delay := w.inspectInterval + time.Duration(rand.Intn(int(w.inspectInterval/10)+1))
		w.SendAfter(w.PID(), messageInspectProcess{}, delay)
	case messageInspectProcess:
		w.inspectProcessList()
		delay := w.inspectInterval + time.Duration(rand.Intn(int(w.inspectInterval/10)+1))
		w.SendAfter(w.PID(), messageInspectProcess{}, delay)
	case messageTopologyChange:
		if e.ID == w.topologyChangeID {
			w.handleTopologyChange(w.processCache.Load())
		}
	case MessageProcessChanged:
		return w.handleProcessChanged(e)
	case MessageForwardLocate:
		var node gen.Atom
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.Node().Name() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				node = p
			}
		}
		w.SendResponse(e.From, e.Ref, node)
	}
	return nil
}

func (w *whereis) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch e := request.(type) {
	case MessageLocate:
		if e.Name == "" {
			return gen.Atom(""), nil
		}
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.Node().Name() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				return p, nil
			}
			return gen.Atom(""), nil
		}
		if owner == "" {
			return gen.Atom(""), nil
		}
		w.Send(gen.ProcessID{Node: owner, Name: WhereIsProcess}, MessageForwardLocate{
			Name: e.Name,
			From: from,
			Ref:  ref,
		})
		return nil, nil
	case MessageGetAddressBook:
		return MessageAddressBook{Book: w.book, Owner: w.PID()}, nil
	}
	return w.PID(), nil
}

func (w *whereis) HandleEvent(event gen.MessageEvent) error {
	switch event.Message.(type) {
	case zk.EventNodeJoined, zk.EventNodeLeft:
		nodeList, _ := w.fetchAvailableBookNodes()
		n := 1
		if nodeList != nil {
			n = nodeList.Len()
		}
		// Stagger the sync based on cluster size to prevent sync storms.
		// Base delay 100ms, max delay proportional to node count (10ms per node).
		// Cap the max random delay at 20 seconds.
		maxRand := min(max(n*10, 1000), 20000)
		delay := time.Duration(100+rand.Intn(maxRand)) * time.Millisecond
		w.topologyChangeID++
		w.SendAfter(w.PID(), messageTopologyChange{ID: w.topologyChangeID}, delay)
	}
	return nil
}

func (w *whereis) syncLocalChanges(newList []ProcessInfo) error {
	selfNode := w.Node().Name()
	oldList, err := w.book.GetProcessList(selfNode)
	if err != nil {
		return err
	}

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
		if old, ok := oldMap[name]; !ok || old.PID != p.PID || old.BirthAt != p.BirthAt {
			msg.UpProcess = append(msg.UpProcess, p)
		}
	}

	for name, p := range oldMap {
		if newP, ok := newMap[name]; !ok || newP.PID != p.PID || newP.BirthAt != p.BirthAt {
			msg.DownProcess = append(msg.DownProcess, p)
		}
	}

	w.book.SetProcess(selfNode, newList...)
	if len(msg.UpProcess) > 0 || len(msg.DownProcess) > 0 {
		ver := w.selfVersion.Incr()
		msg.Version = ver
		w.selfVersion = ver
		w.registerToShards(msg)
	}
	return nil
}

func (w *whereis) registerToShards(msg MessageProcessChanged) {
	shards := make(map[gen.Atom]*MessageProcessChanged)
	for _, p := range msg.UpProcess {
		owner := w.book.PickDirectoryNode(p.Name)
		if owner == "" {
			continue
		}
		if _, ok := shards[owner]; !ok {
			shards[owner] = &MessageProcessChanged{
				Node:     w.Node().Name(),
				Version:  msg.Version,
				FullSync: msg.FullSync,
			}
		}
		shards[owner].UpProcess = append(shards[owner].UpProcess, p)
	}
	for _, p := range msg.DownProcess {
		owner := w.book.PickDirectoryNode(p.Name)
		if owner == "" {
			continue
		}
		if _, ok := shards[owner]; !ok {
			shards[owner] = &MessageProcessChanged{
				Node:     w.Node().Name(),
				Version:  msg.Version,
				FullSync: msg.FullSync,
			}
		}
		shards[owner].DownProcess = append(shards[owner].DownProcess, p)
	}

	for owner, shardMsg := range shards {
		if owner == w.Node().Name() {
			w.handleProcessChanged(*shardMsg)
		} else {
			w.Send(gen.ProcessID{Node: owner, Name: WhereIsProcess}, *shardMsg)
		}
	}
}

func (w *whereis) inspectProcessList() error {
	if err := w.collectProcessList(); err != nil {
		return err
	}

	w.antiEntropyCounter++
	if w.antiEntropyCounter >= 100 {
		w.antiEntropyCounter = 0
		w.selfVersion = w.selfVersion.Incr()
		localProcs := w.processCache.Load()
		w.book.SetProcess(w.Node().Name(), localProcs...)
		w.handleTopologyChange(localProcs)
	} else {
		w.syncLocalChanges(w.processCache.Load())
	}
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
		if _, ok := w.pidToName[pid]; !ok {
			added = append(added, pid)
		}
	}
	// Iterate through the old process list (pidToName) to find deleted (terminated) processes.
	for pid := range w.pidToName {
		if _, ok := pidMap[pid]; !ok {
			del = append(del, pid)
		}
	}

	// If there are no added or deleted processes, there is nothing to do.
	if len(added) == 0 && len(del) == 0 {
		return nil
	}

	node := w.Node()
	nodeVersion, err := w.getNodeVersion(node.Name())
	if err != nil {
		return err
	}
	// Remove deleted processes from the lookup maps.
	for _, pid := range del {
		name := w.pidToName[pid]
		// Ensure we only delete the entry if the PID matches,
		// avoiding issues with stale/reused process names.
		if w.nameToPID[name] == pid {
			delete(w.nameToPID, name)
			delete(w.nameToBirthAt, name)
		}
		delete(w.pidToName, pid)
	}

	// Add new processes to the lookup maps.
	for _, pid := range added {
		if info, err := node.ProcessInfo(pid); err != nil {
			return err
		} else {
			w.pidToName[pid] = info.Name
			if info.Name != "" {
				w.nameToPID[info.Name] = pid
				w.nameToBirthAt[info.Name] = time.Now().Unix() - info.Uptime
			}
		}
	}

	// Rebuild the full process list from the updated nameToPID map.
	procList := make(ProcessInfoList, 0, len(w.nameToPID))
	for name, pid := range w.nameToPID {
		procList = append(procList, ProcessInfo{
			Name:    name,
			PID:     pid,
			Node:    node.Name(),
			BirthAt: w.nameToBirthAt[name],
			Version: nodeVersion,
		})
	}

	// Atomically update the process cache with the new list.
	w.processCache.Store(procList)
	return nil
}

func (w *whereis) setup() error {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return err
		} else {
			w.registrar = registrar
		}
		event, err := registrar.Event()
		if err != nil {
			return err
		}
		if _, err := w.MonitorEvent(event); err != nil {
			return err
		}
		// Initial fetch of available nodes
		w.fetchAvailableBookNodes()
	}
	return nil
}

func (w *whereis) fetchAvailableBookNodes() (*NodeList, error) {
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodeList := NewNodeList(sortNodes(uniqNodes(append(nodes, w.Node().Name())))...)
	w.book.SetAvailableNodes(nodeList)
	return nodeList, nil
}

func (w *whereis) HandleInspect(from gen.PID, item ...string) map[string]string {
	nodes := w.book.GetAvailableNodes()
	stats := map[string]string{
		"nodes": strconv.FormatInt(int64(nodes.Len()), 10),
	}
	return stats
}

func (w *whereis) handleProcessChanged(e MessageProcessChanged) error {
	if version, ok := w.nodeVersions[e.Node]; ok && version.GreaterThan(e.Version) {
		return nil
	}
	if e.FullSync {
		w.book.SetProcess(e.Node, e.UpProcess...)
	} else {
		w.book.AddProcess(e.Node, e.UpProcess...)
		w.book.RemoveProcess(e.Node, e.DownProcess...)
	}
	w.nodeVersions[e.Node] = e.Version
	return nil
}

func (w *whereis) getNodeVersion(node gen.Atom) (int, error) {
	if r := w.registrar; r != nil {
		val, err := r.ConfigItem(string(node))
		if err != nil {
			return -1, err
		}
		if ver, ok := val.(int); ok {
			return ver, nil
		}
	}
	return -1, nil
}

func (w *whereis) Terminate(reason error) {
}

func (w *whereis) handleTopologyChange(localProcs ProcessInfoList) {
	msg := MessageProcessChanged{
		Node:      w.Node().Name(),
		UpProcess: localProcs,
		Version:   w.selfVersion,
		FullSync:  true,
	}
	w.registerToShards(msg)
}
