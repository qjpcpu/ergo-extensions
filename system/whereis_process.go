package system

import (
	"math/rand"
	"strconv"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/registrar/events"
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
	processCache       *AtomicValue[ProcessInfoList]
	inspectInterval    time.Duration
	antiEntropyCounter int
	topologyChangeID   int64
	sendFailureLogAt   map[gen.Atom]time.Time
	selfNode           gen.Atom
	nowFn              func() time.Time
	sendProcessChanged func(gen.ProcessID, MessageProcessChanged) error
	logSendFailureFn   func(gen.Atom, string, error)
}

func factoryWhereIs(book *AddressBook, inspectInterval time.Duration) gen.ProcessFactory {
	if inspectInterval == 0 {
		inspectInterval = time.Second * 3
	}
	return func() gen.ProcessBehavior {
		return &whereis{
			book:             book,
			pidToName:        make(map[gen.PID]gen.Atom),
			nameToPID:        make(map[gen.Atom]gen.PID),
			nameToBirthAt:    make(map[gen.Atom]int64),
			processCache:     NewAtomicValue[ProcessInfoList](),
			selfVersion:      NewVersion(),
			nodeVersions:     make(map[gen.Atom]ProcessVersion),
			inspectInterval:  inspectInterval,
			sendFailureLogAt: make(map[gen.Atom]time.Time),
			nowFn:            func() time.Time { return time.Now().UTC() },
		}
	}
}

func (w *whereis) selfNodeName() gen.Atom {
	if w.selfNode != "" {
		return w.selfNode
	}
	return w.Node().Name()
}

func (w *whereis) now() time.Time {
	if w.nowFn != nil {
		return w.nowFn()
	}
	return time.Now().UTC()
}

func (w *whereis) sendProcessChangedMessage(pid gen.ProcessID, msg MessageProcessChanged) error {
	if w.sendProcessChanged != nil {
		return w.sendProcessChanged(pid, msg)
	}
	return w.Send(pid, msg)
}

func (w *whereis) shouldLogSendFailure(owner gen.Atom, now time.Time) bool {
	if w.sendFailureLogAt == nil {
		w.sendFailureLogAt = make(map[gen.Atom]time.Time)
	}
	last, ok := w.sendFailureLogAt[owner]
	if ok && now.Sub(last) < 30*time.Second {
		return false
	}
	w.sendFailureLogAt[owner] = now
	return true
}

func (w *whereis) clearSendFailure(owner gen.Atom) {
	if w.sendFailureLogAt == nil {
		return
	}
	delete(w.sendFailureLogAt, owner)
}

func (w *whereis) logSendFailure(owner gen.Atom, kind string, err error) {
	if !w.shouldLogSendFailure(owner, w.now()) {
		return
	}
	if w.logSendFailureFn != nil {
		w.logSendFailureFn(owner, kind, err)
		return
	}
	w.Log().Warning("whereis %s send to %s failed on %s: %v", kind, owner, w.selfNodeName(), err)
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
		w.inspectProcessList()
		delay := w.inspectInterval + time.Duration(rand.Intn(int(w.inspectInterval/10)+1))
		w.SendAfter(w.PID(), messageInspectProcess{}, delay)
	case messageInspectProcess:
		w.inspectProcessList()
		delay := w.inspectInterval + time.Duration(rand.Intn(int(w.inspectInterval/10)+1))
		w.SendAfter(w.PID(), messageInspectProcess{}, delay)
	case messageTopologyChange:
		if e.ID == w.topologyChangeID {
			procs := w.processCache.Load()
			// Update ring with current membership
			nodeList, _ := w.fetchAvailableBookNodes()
			// Purge stale nodeVersions for nodes no longer in the cluster
			if nodeList != nil {
				for node := range w.nodeVersions {
					if !nodeList.Exist(node) {
						delete(w.nodeVersions, node)
					}
				}
			}
			// Refresh local address book entry
			w.book.SetProcess(w.selfNodeName(), procs...)
			// Push authoritative shards to every current directory node so
			// previous owners clear stale state after rebalancing.
			w.selfVersion = w.selfVersion.Incr()
			w.syncDirectoryShards(procs)
		}
	case MessageProcessChanged:
		return w.handleProcessChanged(e)
	case MessageLocate:
		if e.Name == "" {
			return nil
		}
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.selfNodeName() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				w.Send(from, MessageLocateResult{Name: e.Name, Node: p})
				return nil
			}
			w.Send(from, MessageLocateResult{Name: e.Name})
			return nil
		}
		if owner == "" {
			w.Send(from, MessageLocateResult{Name: e.Name})
			return nil
		}
		w.Send(gen.ProcessID{Node: owner, Name: WhereIsProcess}, MessageForwardLocate{
			Name: e.Name,
			From: from,
		})
	case MessageForwardLocate:
		var node gen.Atom
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.selfNodeName() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				node = p
			}
		} else if owner != "" && e.Hops < 2 {
			e.Hops++
			w.Send(gen.ProcessID{Node: owner, Name: WhereIsProcess}, e)
			return nil
		}
		if e.Ref.ID[0] == 0 && e.Ref.ID[1] == 0 && e.Ref.ID[2] == 0 {
			// it's a Send request
			w.Send(e.From, MessageLocateResult{Name: e.Name, Node: node})
		} else {
			w.SendResponse(e.From, e.Ref, node)
		}
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
		if owner == w.selfNodeName() {
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
	case events.EventNodeJoined, events.EventNodeLeft:
		// Use cached node count for delay calculation to avoid acquiring
		// a write lock (via fetchAvailableBookNodes) on every event.
		// The actual ring update is deferred to messageTopologyChange,
		// which is debounced by topologyChangeID.
		n := max(1, w.book.GetAvailableNodes().Len())
		// Stagger the sync based on cluster size to prevent sync storms.
		// Base delay 100ms, max delay proportional to node count (10ms per node).
		// Cap the max random delay at 2 seconds (reduced from 20s for test stability).
		maxRand := min(max(n*10, 500), 2000)
		delay := time.Duration(100+rand.Intn(maxRand)) * time.Millisecond
		w.scheduleTopologyDebounce(delay)
	}
	return nil
}

func (w *whereis) registerToShards(msg MessageProcessChanged) {
	// Batch lookup directory owners to acquire the lock only once
	// instead of per-process, reducing lock overhead in large clusters.
	names := make([]gen.Atom, 0, len(msg.UpProcess)+len(msg.DownProcess))
	for _, p := range msg.UpProcess {
		names = append(names, p.Name)
	}
	for _, p := range msg.DownProcess {
		names = append(names, p.Name)
	}
	owners := w.book.PickDirectoryNodeBatch(names)

	shards := make(map[gen.Atom]*MessageProcessChanged)
	for _, p := range msg.UpProcess {
		owner := owners[p.Name]
		if owner == "" {
			continue
		}
		if _, ok := shards[owner]; !ok {
			shards[owner] = &MessageProcessChanged{
				Node:     w.selfNodeName(),
				Version:  msg.Version,
				FullSync: msg.FullSync,
			}
		}
		shards[owner].UpProcess = append(shards[owner].UpProcess, p)
	}
	for _, p := range msg.DownProcess {
		owner := owners[p.Name]
		if owner == "" {
			continue
		}
		if _, ok := shards[owner]; !ok {
			shards[owner] = &MessageProcessChanged{
				Node:     w.selfNodeName(),
				Version:  msg.Version,
				FullSync: msg.FullSync,
			}
		}
		shards[owner].DownProcess = append(shards[owner].DownProcess, p)
	}

	for owner, shardMsg := range shards {
		if owner != w.selfNodeName() {
			if err := w.sendProcessChangedMessage(gen.ProcessID{Node: owner, Name: WhereIsProcess}, *shardMsg); err != nil {
				w.logSendFailure(owner, "incremental shard sync", err)
				continue
			}
			w.clearSendFailure(owner)
		}
	}
}

func (w *whereis) inspectProcessList() error {
	up, down, all, err := w.collectProcessList()
	if err != nil {
		return err
	}

	w.antiEntropyCounter++
	if w.antiEntropyCounter >= w.antiEntropyThreshold() {
		w.antiEntropyCounter = 0
		w.selfVersion = w.selfVersion.Incr()
		w.book.SetProcess(w.selfNodeName(), all...)
		w.handleTopologyChange(all)
	} else if len(up) > 0 || len(down) > 0 {
		w.book.AddProcess(w.selfNodeName(), up...)
		w.book.RemoveProcess(w.selfNodeName(), down...)
		w.selfVersion = w.selfVersion.Incr()
		w.registerToShards(MessageProcessChanged{
			Node:        w.selfNodeName(),
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
		if _, err := w.fetchAvailableBookNodes(); err != nil {
			return err
		}
	}
	return nil
}

func (w *whereis) fetchAvailableBookNodes() (*NodeList, error) {
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodeList := NewNodeList(sortNodes(uniqNodes(append(nodes, w.selfNodeName())))...)
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

// antiEntropyThreshold returns the number of inspection cycles between
// full anti-entropy syncs, scaled by cluster size. Larger clusters use
// longer intervals to avoid excessive FullSync traffic.
func (w *whereis) antiEntropyThreshold() int {
	n := w.book.GetAvailableNodes().Len()
	return min(max(100, n/2), 2000)
}

// syncDirectoryShards sends an authoritative shard snapshot to every current
// directory node. Empty shards are sent too, so previous owners clear stale
// state after topology changes.
func (w *whereis) syncDirectoryShards(procs ProcessInfoList) {
	dirNodes := w.book.DirectoryNodes()
	if len(dirNodes) == 0 {
		return
	}

	names := make([]gen.Atom, len(procs))
	for i, p := range procs {
		names[i] = p.Name
	}
	owners := w.book.PickDirectoryNodeBatch(names)

	shards := make(map[gen.Atom]*MessageProcessChanged)
	for _, owner := range dirNodes {
		shards[owner] = &MessageProcessChanged{
			Node:     w.selfNodeName(),
			Version:  w.selfVersion,
			FullSync: true,
		}
	}

	for _, p := range procs {
		owner := owners[p.Name]
		if owner == "" {
			continue
		}
		shards[owner].UpProcess = append(shards[owner].UpProcess, p)
	}

	for owner, msg := range shards {
		if owner != w.selfNodeName() {
			if err := w.sendProcessChangedMessage(gen.ProcessID{Node: owner, Name: WhereIsProcess}, *msg); err != nil {
				w.logSendFailure(owner, "topology full sync", err)
				continue
			}
			w.clearSendFailure(owner)
		}
	}
}

func (w *whereis) handleTopologyChange(localProcs ProcessInfoList) {
	msg := MessageProcessChanged{
		Node:      w.selfNodeName(),
		UpProcess: localProcs,
		Version:   w.selfVersion,
		FullSync:  true,
	}
	w.registerToShards(msg)
}

func (w *whereis) scheduleTopologyDebounce(delay time.Duration) {
	w.topologyChangeID++
	w.SendAfter(w.PID(), messageTopologyChange{ID: w.topologyChangeID}, delay)
}
