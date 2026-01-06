package system

import (
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

type persist_whereis struct {
	act.Actor
	book      *PersistAddressBook
	registrar gen.Registrar

	pid_to_name map[gen.PID]gen.Atom
	name_to_pid map[gen.Atom]gen.PID
	// only includes named processes
	inspect_interval time.Duration
}

func factory_persist_whereis(book *PersistAddressBook, inspect_interval time.Duration) gen.ProcessFactory {
	if inspect_interval == 0 {
		inspect_interval = time.Second * 3
	}
	return func() gen.ProcessBehavior {
		return &persist_whereis{
			book:             book,
			pid_to_name:      make(map[gen.PID]gen.Atom),
			name_to_pid:      make(map[gen.Atom]gen.PID),
			inspect_interval: inspect_interval,
		}
	}
}

func (w *persist_whereis) Init(args ...any) error {
	w.SendAfter(w.PID(), start_init{}, w.inspect_interval)
	return nil
}

func (w *persist_whereis) HandleMessage(from gen.PID, message any) error {
	switch msg := message.(type) {
	case start_init:
		if err := w.init(); err != nil {
			w.SendAfter(w.PID(), start_init{}, w.inspect_interval)
			return nil
		}
		w.inspectProcessList()
		w.SendAfter(w.PID(), inspect_process_list{}, w.inspect_interval)
	case inspect_process_list:
		if err := w.inspectProcessList(); err != nil {
			w.Log().Error("inspect process list failed: %v", err)
		}
		w.SendAfter(w.PID(), inspect_process_list{}, w.inspect_interval)
	case MessageFlushProcess:
		p := from
		if msg.PID.Node != "" && msg.PID.ID != 0 {
			p = msg.PID
		}
		w.flushProcess(p)
	}
	return nil
}

func (w *persist_whereis) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch e := request.(type) {
	case MessageLocate:
		if p, ok := w.book.Locate(e.Name); ok {
			return p, nil
		} else {
			return gen.Atom(""), nil
		}
	case MessageGetAddressBook:
		return MessageAddressBook{Book: w.book, Owner: w.PID()}, nil
	}
	return w.PID(), nil
}

func (w *persist_whereis) inspectProcessList() error {
	if _, err := w.fetchAvailableBookNodes(); err != nil {
		return err
	}
	if err := w.collectProcessList(); err != nil {
		return err
	}
	return nil
}

// collectProcessList gets all processes from the current node,
// finds the newly started and recently stopped processes,
// updates the internal cache, and stores the full process list
// into the processCache.
func (w *persist_whereis) collectProcessList() error {
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
	version, err := w.getNodeVersion(node.Name())
	if err != nil {
		return err
	}

	// Remove deleted processes from the lookup maps.
	for _, pid := range del {
		name := w.pid_to_name[pid]
		if name != "" {
			if err := w.book.RemoveProcess(node.Name(), version, name); err != nil {
				return err
			}
		}
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
			if info.Name != "" {
				if err := w.book.SetProcess(node.Name(), version, info.Name); err != nil {
					return err
				}
			}
			w.pid_to_name[pid] = info.Name
			if info.Name != "" {
				w.name_to_pid[info.Name] = pid
			}
		}
	}

	return nil
}

func (w *persist_whereis) init() error {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return err
		} else {
			w.registrar = registrar
		}
		w.book.SetRegistrar(registrar)
	}
	return nil
}

func (w *persist_whereis) fetchAvailableBookNodes() ([]gen.Atom, error) {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return nil, err
		}
		w.registrar = registrar
		w.book.SetRegistrar(registrar)
	}
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return nil, err
	}
	nodes = uniqNodes(append(nodes, w.Node().Name()))
	if err := w.book.SetAvailableNodes(nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (w *persist_whereis) flushProcess(pid gen.PID) error {
	info, err := w.Node().ProcessInfo(pid)
	if err != nil {
		return err
	}
	if info.Name != "" {
		version, err := w.getNodeVersion(w.Node().Name())
		if err != nil {
			return err
		}
		if err := w.book.SetProcess(w.Node().Name(), version, info.Name); err != nil {
			return err
		}
	}
	return nil
}

func (w *persist_whereis) getNodeVersion(node gen.Atom) (int, error) {
	return w.book.nodeVersion(node)
}
