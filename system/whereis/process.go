package whereis

import (
	"math/rand"
	"strconv"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	core "github.com/qjpcpu/ergo-extensions/system/internal/core"
	"github.com/qjpcpu/registrar/events"
)

const ProcessName = gen.Atom("extensions_whereis")

type Options struct {
	SyncInterval        time.Duration
	TopologyDebounceMin time.Duration
	TopologyDebounceMax time.Duration
	QueryTimeout        int
}

func DefaultOptions() Options {
	return Options{
		SyncInterval:        2 * time.Second,
		TopologyDebounceMin: 100 * time.Millisecond,
		TopologyDebounceMax: 2 * time.Second,
		QueryTimeout:        3,
	}
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = defaults.SyncInterval
	}
	if opts.TopologyDebounceMin <= 0 {
		opts.TopologyDebounceMin = defaults.TopologyDebounceMin
	}
	if opts.TopologyDebounceMax <= 0 {
		opts.TopologyDebounceMax = defaults.TopologyDebounceMax
	}
	if opts.TopologyDebounceMax < opts.TopologyDebounceMin {
		opts.TopologyDebounceMax = opts.TopologyDebounceMin
	}
	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = defaults.QueryTimeout
	}
	return opts
}

type messageInit struct{}
type messageInspectProcess struct{}
type messageTopologyChange struct {
	ID int64
}

type whereis struct {
	act.Actor
	book      *core.AddressBook
	registrar gen.Registrar

	selfVersion  core.ProcessVersion
	nodeVersions map[gen.Atom]core.ProcessVersion

	pidToName     map[gen.PID]gen.Atom
	nameToBirthAt map[gen.Atom]int64
	nameToPID     map[gen.Atom]gen.PID
	// only includes named processes
	processCache       *core.AtomicValue[core.ProcessInfoList]
	inspectInterval    time.Duration
	options            Options
	antiEntropyCounter int
	topologyChangeID   int64
	sendFailureLogAt   map[gen.Atom]time.Time
	selfNode           gen.Atom
	nowFn              func() time.Time
	sendProcessChanged func(gen.ProcessID, core.MessageProcessChanged) error
	logSendFailureFn   func(gen.Atom, string, error)
}

func Factory(book *core.AddressBook, inspectInterval time.Duration) gen.ProcessFactory {
	opts := DefaultOptions()
	if inspectInterval > 0 {
		opts.SyncInterval = inspectInterval
	}
	return FactoryWithOptions(book, opts)
}

func FactoryWithOptions(book *core.AddressBook, opts Options) gen.ProcessFactory {
	opts = normalizeOptions(opts)
	return func() gen.ProcessBehavior {
		return &whereis{
			book:             book,
			pidToName:        make(map[gen.PID]gen.Atom),
			nameToPID:        make(map[gen.Atom]gen.PID),
			nameToBirthAt:    make(map[gen.Atom]int64),
			processCache:     core.NewAtomicValue[core.ProcessInfoList](),
			selfVersion:      core.NewVersion(),
			nodeVersions:     make(map[gen.Atom]core.ProcessVersion),
			inspectInterval:  opts.SyncInterval,
			options:          opts,
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

func pidIsZero(pid gen.PID) bool {
	return pid == gen.PID{}
}

func (w *whereis) sendProcessChangedMessage(pid gen.ProcessID, msg core.MessageProcessChanged) error {
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
	case core.MessageProcessChanged:
		return w.handleProcessChanged(e)
	case core.MessageRegisterLocalProcess:
		return w.registerLocalProcess(e)
	case core.MessageLocate:
		if e.Name == "" {
			return nil
		}
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.selfNodeName() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				w.Send(from, core.MessageLocateResult{Name: e.Name, Node: p})
				return nil
			}
			w.Send(from, core.MessageLocateResult{Name: e.Name})
			return nil
		}
		if owner == "" {
			w.Send(from, core.MessageLocateResult{Name: e.Name})
			return nil
		}
		w.Send(gen.ProcessID{Node: owner, Name: ProcessName}, core.MessageForwardLocate{
			Name: e.Name,
			From: from,
		})
	case core.MessageForwardLocate:
		var node gen.Atom
		owner := w.book.PickDirectoryNode(e.Name)
		if owner == w.selfNodeName() {
			if p, ok := w.book.LocateLocal(e.Name); ok {
				node = p
			}
		} else if owner != "" && e.Hops < 2 {
			e.Hops++
			w.Send(gen.ProcessID{Node: owner, Name: ProcessName}, e)
			return nil
		}
		if e.Ref.ID[0] == 0 && e.Ref.ID[1] == 0 && e.Ref.ID[2] == 0 {
			// it's a Send request
			w.Send(e.From, core.MessageLocateResult{Name: e.Name, Node: node})
		} else {
			w.SendResponse(e.From, e.Ref, node)
		}
	}
	return nil
}

func (w *whereis) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch e := request.(type) {
	case core.MessageLocate:
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
		w.Send(gen.ProcessID{Node: owner, Name: ProcessName}, core.MessageForwardLocate{
			Name: e.Name,
			From: from,
			Ref:  ref,
		})
		return nil, nil
	case core.MessageGetAddressBook:
		return core.MessageAddressBook{Book: w.book, Owner: w.PID()}, nil
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
		window := w.options.TopologyDebounceMax - w.options.TopologyDebounceMin
		if window < 0 {
			window = 0
		}
		sizeCap := time.Duration(min(max(n*10, 500), 2000)) * time.Millisecond
		if sizeCap < window {
			window = sizeCap
		}
		delay := w.options.TopologyDebounceMin
		if window > 0 {
			delay += time.Duration(rand.Int63n(int64(window)))
		}
		w.scheduleTopologyDebounce(delay)
	}
	return nil
}

func (w *whereis) registerToShards(msg core.MessageProcessChanged) {
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

	shards := make(map[gen.Atom]*core.MessageProcessChanged)
	for _, p := range msg.UpProcess {
		owner := owners[p.Name]
		if owner == "" {
			continue
		}
		if _, ok := shards[owner]; !ok {
			shards[owner] = &core.MessageProcessChanged{
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
			shards[owner] = &core.MessageProcessChanged{
				Node:     w.selfNodeName(),
				Version:  msg.Version,
				FullSync: msg.FullSync,
			}
		}
		shards[owner].DownProcess = append(shards[owner].DownProcess, p)
	}

	for owner, shardMsg := range shards {
		if owner != w.selfNodeName() {
			if err := w.sendProcessChangedMessage(gen.ProcessID{Node: owner, Name: ProcessName}, *shardMsg); err != nil {
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
		w.registerToShards(core.MessageProcessChanged{
			Node:        w.selfNodeName(),
			UpProcess:   up,
			DownProcess: down,
			Version:     w.selfVersion,
		})
	}
	return nil
}

func (w *whereis) registerLocalProcess(msg core.MessageRegisterLocalProcess) error {
	if msg.Name == "" {
		return nil
	}
	pid := msg.PID
	birthAt := msg.BirthAt
	if !pidIsZero(pid) {
		if info, err := w.Node().ProcessInfo(pid); err == nil {
			if info.Name != "" {
				msg.Name = info.Name
			}
			if birthAt == 0 {
				birthAt = time.Now().Unix() - info.Uptime
			}
		}
	}
	if birthAt == 0 {
		birthAt = time.Now().Unix()
	}

	var down core.ProcessInfoList
	if oldPID, ok := w.nameToPID[msg.Name]; ok && oldPID != pid {
		down = append(down, core.ProcessInfo{
			Name:    msg.Name,
			PID:     oldPID,
			Node:    w.selfNodeName(),
			BirthAt: w.nameToBirthAt[msg.Name],
		})
		delete(w.pidToName, oldPID)
	}

	if !pidIsZero(pid) {
		w.pidToName[pid] = msg.Name
		w.nameToPID[msg.Name] = pid
	}
	w.nameToBirthAt[msg.Name] = birthAt

	up := core.ProcessInfoList{{
		Name:    msg.Name,
		PID:     pid,
		Node:    w.selfNodeName(),
		BirthAt: birthAt,
	}}
	w.rebuildProcessCache()
	w.book.RemoveProcess(w.selfNodeName(), down...)
	w.book.AddProcess(w.selfNodeName(), up...)
	w.selfVersion = w.selfVersion.Incr()
	w.registerToShards(core.MessageProcessChanged{
		Node:        w.selfNodeName(),
		UpProcess:   up,
		DownProcess: down,
		Version:     w.selfVersion,
	})
	return nil
}

func (w *whereis) rebuildProcessCache() {
	all := make(core.ProcessInfoList, 0, len(w.nameToPID))
	for name, pid := range w.nameToPID {
		all = append(all, core.ProcessInfo{
			Name:    name,
			PID:     pid,
			Node:    w.selfNodeName(),
			BirthAt: w.nameToBirthAt[name],
		})
	}
	w.processCache.Store(all)
}

// collectProcessList gets all processes from the current node,
// finds the newly started and recently stopped processes,
// updates the internal cache, and returns incremental and full process lists.
func (w *whereis) collectProcessList() (up, down, all core.ProcessInfoList, err error) {
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
			down = append(down, core.ProcessInfo{
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
				up = append(up, core.ProcessInfo{
					Name:    info.Name,
					PID:     pid,
					Node:    node.Name(),
					BirthAt: birthAt,
				})
			}
		}
	}

	// Rebuild the full process list from the updated nameToPID map.
	all = make(core.ProcessInfoList, 0, len(w.nameToPID))
	for name, pid := range w.nameToPID {
		all = append(all, core.ProcessInfo{
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

func (w *whereis) fetchAvailableBookNodes() (*core.NodeList, error) {
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodeList := core.NewNodeList(core.SortNodes(core.UniqNodes(append(nodes, w.selfNodeName())))...)
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

func (w *whereis) handleProcessChanged(e core.MessageProcessChanged) error {
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
func (w *whereis) syncDirectoryShards(procs core.ProcessInfoList) {
	dirNodes := w.book.DirectoryNodes()
	if len(dirNodes) == 0 {
		return
	}

	names := make([]gen.Atom, len(procs))
	for i, p := range procs {
		names[i] = p.Name
	}
	owners := w.book.PickDirectoryNodeBatch(names)

	shards := make(map[gen.Atom]*core.MessageProcessChanged)
	for _, owner := range dirNodes {
		shards[owner] = &core.MessageProcessChanged{
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
			if err := w.sendProcessChangedMessage(gen.ProcessID{Node: owner, Name: ProcessName}, *msg); err != nil {
				w.logSendFailure(owner, "topology full sync", err)
				continue
			}
			w.clearSendFailure(owner)
		}
	}
}

func (w *whereis) handleTopologyChange(localProcs core.ProcessInfoList) {
	msg := core.MessageProcessChanged{
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
