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
		if owner != w.Node().Name() {
			w.Send(gen.ProcessID{Node: owner, Name: WhereIsProcess}, *shardMsg)
		}
	}
}

func (w *whereis) inspectProcessList() error {
	up, down, all, err := w.collectProcessList()
	if err != nil {
		return err
	}

	w.antiEntropyCounter++
	if w.antiEntropyCounter >= 100 {
		w.antiEntropyCounter = 0
		w.selfVersion = w.selfVersion.Incr()
		w.book.SetProcess(w.Node().Name(), all...)
		w.handleTopologyChange(all)
	} else if len(up) > 0 || len(down) > 0 {
		w.book.AddProcess(w.Node().Name(), up...)
		w.book.RemoveProcess(w.Node().Name(), down...)
		w.selfVersion = w.selfVersion.Incr()
		w.registerToShards(MessageProcessChanged{
			Node:        w.Node().Name(),
			UpProcess:   up,
			DownProcess: down,
			Version:     w.selfVersion,
		})
	}
	return nil
}

// collectProcessList gets all processes from the current node,
// finds the newly started and recently stopped processes,
// updates the internal cache, and returns incremental and full process lists.
func (w *whereis) collectProcessList() (up, down, all ProcessInfoList, err error) {
	// Get the list of all running process PIDs on the current node.
	pidList, err := w.Node().ProcessList()
	if err != nil {
		return
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

	if len(added) == 0 && len(del) == 0 {
		return nil, nil, w.processCache.Load(), nil
	}

	node := w.Node()
	// Remove deleted processes from the lookup maps.
	for _, pid := range del {
		name := w.pidToName[pid]
		// Ensure we only delete the entry if the PID matches,
		// avoiding issues with stale/reused process names.
		if name != "" && w.nameToPID[name] == pid {
			down = append(down, ProcessInfo{
				Name:    name,
				PID:     pid,
				Node:    node.Name(),
				BirthAt: w.nameToBirthAt[name],
			})
			delete(w.nameToPID, name)
			delete(w.nameToBirthAt, name)
		}
		delete(w.pidToName, pid)
	}

	// Add new processes to the lookup maps.
	for _, pid := range added {
		if info, err0 := node.ProcessInfo(pid); err0 == nil {
			w.pidToName[pid] = info.Name
			if info.Name != "" {
				birthAt := time.Now().Unix() - info.Uptime
				w.nameToPID[info.Name] = pid
				w.nameToBirthAt[info.Name] = birthAt
				up = append(up, ProcessInfo{
					Name:    info.Name,
					PID:     pid,
					Node:    node.Name(),
					BirthAt: birthAt,
				})
			}
		}
	}

	// Rebuild the full process list from the updated nameToPID map.
	all = make(ProcessInfoList, 0, len(w.nameToPID))
	for name, pid := range w.nameToPID {
		all = append(all, ProcessInfo{
			Name:    name,
			PID:     pid,
			Node:    node.Name(),
			BirthAt: w.nameToBirthAt[name],
		})
	}

	// Atomically update the process cache with the new list.
	w.processCache.Store(all)
	return
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
	if version, ok := w.nodeVersions[e.Node]; ok && version.GreaterThanOrEq(e.Version) {
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
