package system

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

const (
	WhereIsProcess = gen.Atom("sysext_whereis")
)

type whereis struct {
	act.Actor
	book      *AddressBook
	registrar gen.Registrar

	changeBufferCap  int
	procChangeBuffer []MessageProcessChanged
	procChangeStart  int
	procChangeCount  int

	selfVersion  ProcessVersion
	nodeVersions map[gen.Atom]ProcessVersion

	pid_to_name      map[gen.PID]gen.Atom
	name_to_birth_at map[gen.Atom]int64
	name_to_pid      map[gen.Atom]gen.PID
	// only includes named processes
	processCache     atomic.Value
	inspect_interval time.Duration

	// stats
	inspect_self_times          uint64
	send_fetch_proc_times       uint64
	respond_fetch_proc_times    uint64
	receive_proc_snapshot_times uint64
	receive_incr_proc_times     uint64
}

func factory_whereis(book *AddressBook, inspect_interval time.Duration, changeBuffer int) gen.ProcessFactory {
	var v atomic.Value
	v.Store(ProcessInfoList{})
	if inspect_interval == 0 {
		inspect_interval = time.Second * 3
	}
	if changeBuffer <= 0 {
		changeBuffer = 16
	}
	return func() gen.ProcessBehavior {
		return &whereis{
			book:             book,
			pid_to_name:      make(map[gen.PID]gen.Atom),
			name_to_pid:      make(map[gen.Atom]gen.PID),
			name_to_birth_at: make(map[gen.Atom]int64),
			processCache:     v,
			selfVersion:      NewVersion(),
			nodeVersions:     make(map[gen.Atom]ProcessVersion),
			inspect_interval: inspect_interval,
			changeBufferCap:  changeBuffer,
		}
	}
}

func (w *whereis) Init(args ...any) error {
	w.SendAfter(w.PID(), inspect_process_list{}, w.inspect_interval)
	return nil
}

func (w *whereis) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case inspect_process_list:
		w.inspectProcessList()
		w.SendAfter(w.PID(), inspect_process_list{}, w.inspect_interval)
	case MessageFetchProcessList:
		w.respondMessageFetchProcessList(from, e.Version)
	case MessageProcesses:
		w.receive_proc_snapshot_times++
		if version, ok := w.nodeVersions[e.Node]; ok && version.GreaterThan(e.Version) {
			w.Log().Warning("received a process version %v from %s, abandon", e.Version, e.Node)
			return nil
		}
		if _, err := w.fetchAvailableBookNodes(); err != nil {
			w.Log().Error("fetch nodes fail %v", err)
			return err
		}
		w.book.SetProcess(e.Node, e.ProcessList...)
		w.Log().Debug("received %d process snapshot on %s %s", len(e.ProcessList), e.Node, shortInfo(e.ProcessList))
		w.nodeVersions[e.Node] = e.Version
	case MessageProcessChanged:
		w.receive_incr_proc_times++
		if version, ok := w.nodeVersions[e.Node]; ok && version.GreaterThan(e.Version) {
			w.Log().Warning("received a process version %v from %s, abandon", e.Version, e.Node)
			return nil
		}
		w.book.AddProcess(e.Node, e.UpProcess...)
		w.book.RemoveProcess(e.Node, e.DownProcess...)
		w.Log().Debug("received +%d%s/-%d%s process on %s", len(e.UpProcess), shortInfo(e.UpProcess), len(e.DownProcess), shortInfo(e.DownProcess), e.Node)
		w.nodeVersions[e.Node] = e.Version
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

func (w *whereis) diff(newList []ProcessInfo) error {
	selfNode := w.Node().Name()
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
		ver := w.selfVersion.Incr()
		msg.Version = ver
		w.selfVersion = ver
		w.appendBuffer(msg)
	}
	return nil
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
	w.diff(w.processCache.Load().(ProcessInfoList))
	w.inspect_self_times++
	return w.fetchOtherNodeProcess(nodes)
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
			delete(w.name_to_birth_at, name)
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
				w.name_to_birth_at[info.Name] = time.Now().Unix() - info.Uptime
			}
		}
	}

	// Rebuild the full process list from the updated name_to_pid map.
	procList := make(ProcessInfoList, 0, len(w.name_to_pid))
	for name, pid := range w.name_to_pid {
		procList = append(procList, ProcessInfo{
			Name:    name,
			PID:     pid,
			Node:    node.Name(),
			BirthAt: w.name_to_birth_at[name],
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
	}
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodes = uniqNodes(append(nodes, w.Node().Name()))
	w.book.SetAvailableNodes(nodes)
	return nodes, nil
}

func (w *whereis) HandleInspect(from gen.PID, item ...string) map[string]string {
	tostr := func(i uint64) string {
		return strconv.FormatUint(i, 10)
	}
	nodes := w.book.GetAvailableNodes()
	stats := map[string]string{
		"nodes":                       strconv.FormatInt(int64(len(nodes)), 10),
		"inspect_self_times":          tostr(w.inspect_self_times),
		"send_fetch_proc_times":       tostr(w.send_fetch_proc_times),
		"respond_fetch_proc_times":    tostr(w.respond_fetch_proc_times),
		"receive_proc_snapshot_times": tostr(w.receive_proc_snapshot_times),
		"receive_incr_proc_times":     tostr(w.receive_incr_proc_times),
	}
	for _, node := range nodes {
		procs := w.book.GetProcessList(node)
		stats[fmt.Sprintf("%s.process", string(node))] = strconv.FormatInt(int64(len(procs)), 10)
	}
	return stats
}

func (w *whereis) appendBuffer(msg MessageProcessChanged) {
	if w.changeBufferCap <= 0 {
		return
	}
	if w.procChangeBuffer == nil {
		w.procChangeBuffer = make([]MessageProcessChanged, w.changeBufferCap)
		w.procChangeStart = 0
		w.procChangeCount = 0
	}
	if w.procChangeCount < w.changeBufferCap {
		idx := (w.procChangeStart + w.procChangeCount) % w.changeBufferCap
		w.procChangeBuffer[idx] = msg
		w.procChangeCount++
		return
	}
	w.procChangeBuffer[w.procChangeStart] = msg
	w.procChangeStart = (w.procChangeStart + 1) % w.changeBufferCap
}

func (w *whereis) fetchOtherNodeProcess(nodes []gen.Atom) error {
	nodeSet := make(map[gen.Atom]ProcessVersion)
	for _, node := range nodes {
		if node != w.Node().Name() {
			if version, ok := w.nodeVersions[node]; ok {
				nodeSet[node] = version
			} else {
				nodeSet[node] = NewEmptyVersion()
			}
		}
	}

	for node, version := range nodeSet {
		if err := w.Send(gen.ProcessID{Node: node, Name: WhereIsProcess}, MessageFetchProcessList{Version: version}); err != nil {
			w.Log().Warning("send fetch  request to node:%s process failure %v", node, err)
		}
		w.send_fetch_proc_times++
	}

	w.nodeVersions = nodeSet
	return nil
}

func (w *whereis) respondMessageFetchProcessList(from gen.PID, version ProcessVersion) error {
	if version == w.selfVersion {
		return nil
	}
	size := w.procChangeCount
	for i := 0; i < size; i++ {
		idx := (w.procChangeStart + i) % w.changeBufferCap
		ver := w.procChangeBuffer[idx]
		if ver.Version == version {
			if i < size-1 {
				err := w.Send(from, w.compactProcessChangeRing(i+1, size))
				if err != nil {
					w.Log().Warning("send process info to node:%s failure %v", from.Node, err)
				}
				w.respond_fetch_proc_times++
			}
			return nil
		}
	}
	err := w.Send(from, MessageProcesses{
		Node:        w.Node().Name(),
		ProcessList: w.book.GetProcessList(w.Node().Name()),
		Version:     w.selfVersion,
	})
	if err != nil {
		w.Log().Warning("send process info to node:%s failure %v", from.Node, err)
	}
	w.respond_fetch_proc_times++
	return err
}

func (w *whereis) compactProcessChangeRing(start, end int) MessageProcessChanged {
	if start >= end || w.procChangeCount == 0 {
		return MessageProcessChanged{}
	}
	add, del := make(map[gen.Atom]ProcessInfo), make(map[gen.Atom]ProcessInfo)
	for j := start; j < end; j++ {
		idx := (w.procChangeStart + j) % w.changeBufferCap
		item := w.procChangeBuffer[idx]
		for _, val := range item.UpProcess {
			add[val.Name] = val
			delete(del, val.Name)
		}
		for _, val := range item.DownProcess {
			del[val.Name] = val
			delete(add, val.Name)
		}
	}
	toList := func(v map[gen.Atom]ProcessInfo) []ProcessInfo {
		var arr []ProcessInfo
		for _, item := range v {
			arr = append(arr, item)
		}
		return arr
	}
	lastIdx := (w.procChangeStart + end - 1) % w.changeBufferCap
	return MessageProcessChanged{
		Node:        w.Node().Name(),
		UpProcess:   toList(add),
		DownProcess: toList(del),
		Version:     w.procChangeBuffer[lastIdx].Version,
	}
}
