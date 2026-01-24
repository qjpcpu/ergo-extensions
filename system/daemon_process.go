package system

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
	"ergo.services/registrar/zk"
)

const DaemonMonitorProcess = gen.Atom("extensions_daemon")

var ErrNoAvailableNodes = errors.New("no available nodes")

type daemon struct {
	act.Actor
	book            IAddressBook
	registrar       gen.Registrar
	isLeader        bool
	cancelLaunchAll gen.CancelFunc
	recovered       map[gen.Atom]struct{}
}

func factoryDaemon(book IAddressBook) gen.ProcessFactory {
	return func() gen.ProcessBehavior { return &daemon{book: book, recovered: make(map[gen.Atom]struct{})} }
}

func (w *daemon) Init(args ...any) error {
	w.SendAfter(w.PID(), messageInit{}, time.Second*1)
	return nil
}

func (w *daemon) HandleMessage(from gen.PID, message any) error {
	switch e := message.(type) {
	case messageInit:
		if err := w.setupRegistrarMonitoring(); err != nil {
			w.SendAfter(w.PID(), messageInit{}, time.Second*1)
		} else {
			w.launchAllAfter(time.Second * 10)
		}
	case MessageLaunchAllDaemon:
		if err := w.leaderShouldRecoverDaemon(); err != nil {
			w.launchAllAfter(time.Second * 60)
		} else {
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(time.Minute * 15)
		}
	case MessageLaunchOneDaemon:
		val, ok := launchers.Load(e.Launcher)
		if !ok {
			w.Log().Info("can't find launcher by %s", e.Launcher)
			return nil
		}
		w.launchDaemonOnNode(w.Node().Name(), val.(Launcher), e.Process)
	}
	return nil
}

func (w *daemon) HandleEvent(event gen.MessageEvent) error {
	switch e := event.Message.(type) {
	case zk.EventNodeSwitchedToLeader:
		if e.Name == w.Node().Name() {
			w.isLeader = true
			w.recovered = make(map[gen.Atom]struct{})
			w.launchAllAfter(time.Second * 10)
			return nil
		}
	case zk.EventNodeSwitchedToFollower:
		if e.Name == w.Node().Name() {
			w.isLeader = false
			w.recovered = make(map[gen.Atom]struct{})
			return nil
		}
	case zk.EventNodeLeft:
		w.recovered = make(map[gen.Atom]struct{})
		w.launchAllAfter(time.Second * 60)
	}
	return nil
}

func (w *daemon) launchAllAfter(duration time.Duration) {
	if cancel := w.cancelLaunchAll; cancel != nil {
		cancel()
		w.cancelLaunchAll = nil
	}
	// Add jitter to avoid synchronized leader recovery in case of registrar issues
	// or multiple nodes triggering at once.
	duration += time.Duration(rand.Intn(1000)) * time.Millisecond
	if duration <= 0 {
		w.Send(w.PID(), MessageLaunchAllDaemon{})
	} else {
		if c, err := w.SendAfter(w.PID(), MessageLaunchAllDaemon{}, duration); err == nil {
			w.cancelLaunchAll = c
		}
	}
}

func (w *daemon) setupRegistrarMonitoring() error {
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
		} else {
			if n, err := registrar.ConfigItem(zk.LeaderNodeConfigItem); err != nil {
				return err
			} else if node, ok := n.(gen.Atom); ok {
				w.isLeader = node == w.Node().Name()
			}
		}
	}
	return nil
}

func (w *daemon) leaderShouldRecoverDaemon() (err error) {
	if !w.isLeader {
		return nil
	}
	launchers.Range(func(key any, value any) bool {
		if err0 := w.recoverDaemon(value.(Launcher)); err0 != nil {
			err = err0
		}
		return true
	})
	return
}

func (w *daemon) recoverDaemon(launcher Launcher) error {
	fac := launcher.RecoveryScanner
	if fac == nil {
		return nil
	}
	next := fac()
	var retErr error
	for {
		processList, hasMore, err := next()
		if err != nil {
			return err
		}
		for _, proc := range processList {
			if _, ok := w.recovered[proc.ProcessName]; ok {
				continue
			}
			if err = w.pickNodeAndLaunchDaemon(launcher, proc); err != nil {
				retErr = err
			} else {
				w.recovered[proc.ProcessName] = struct{}{}
			}
		}
		if !hasMore {
			break
		}
		if len(processList) == 0 && hasMore {
			w.Log().Warning("launcher %s fetch empty process list but hasMore=true", launcher.name)
			break
		}
	}
	return retErr
}

func (w *daemon) pickNodeAndLaunchDaemon(launcher Launcher, proc DaemonProcess) error {
	node := w.book.PickNode(proc.ProcessName)
	if node == "" {
		return ErrNoAvailableNodes
	}
	return w.launchDaemonOnNode(node, launcher, proc)
}

func (w *daemon) launchDaemonOnNode(node gen.Atom, launcher Launcher, proc DaemonProcess) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when launch daemon %s: %v", launcher.name, r)
		}
	}()
	runningNode, err := w.book.QueryBy(w, QueryOption{Timeout: 15}).Locate(proc.ProcessName)
	if err != nil {
		w.Log().Warning("locate daemon process %s fail: %v, will retry later", proc.ProcessName, err)
		return err
	}
	if runningNode != "" {
		w.Log().Info("daemon process %s already exists on %s", proc.ProcessName, runningNode)
		return nil
	}
	if node == w.Node().Name() {
		_, err = w.SpawnRegister(proc.ProcessName, launcher.Factory, launcher.Option, proc.Args...)
		if err != nil {
			if err == gen.ErrTaken {
				w.Log().Info("launch daemon process %s on %s OK", proc.ProcessName, node)
				return nil
			}
			w.Log().Error("launch daemon process %s on %s fail %v", proc.ProcessName, node, err)
			return
		} else {
			w.Log().Info("launch daemon process %s on %s OK", proc.ProcessName, node)
		}
	} else {
		if err = w.SendImportant(gen.ProcessID{Name: DaemonMonitorProcess, Node: node}, MessageLaunchOneDaemon{Launcher: launcher.name, Process: proc}); err != nil {
			w.Log().Error("spawn remote daemon process %s on %s fail %v", proc.ProcessName, node, err)
			return
		} else {
			w.Log().Info("spawn remote daemon process %s on %s OK", proc.ProcessName, node)
		}
	}
	return
}

func (w *daemon) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	if s, ok := request.(string); ok && s == "inspect" {
		return w.HandleInspect(from), nil
	}
	return nil, nil
}

func (w *daemon) HandleInspect(from gen.PID, item ...string) map[string]string {
	stats := map[string]string{
		"is_leader":       strconv.FormatBool(w.isLeader),
		"recovered_count": strconv.Itoa(len(w.recovered)),
	}
	var daemonNames []string
	launchers.Range(func(key, value any) bool {
		daemonNames = append(daemonNames, string(key.(gen.Atom)))
		return true
	})
	if len(daemonNames) > 0 {
		stats["daemons"] = strings.Join(daemonNames, ",")
	}
	if r := w.registrar; r != nil {
		if n, err := r.ConfigItem(zk.LeaderNodeConfigItem); err == nil {
			if node, ok := n.(gen.Atom); ok {
				stats["leader"] = string(node)
			}
		}
	}
	if info, err := w.Node().Info(); err == nil {
		stats["uptime"] = strconv.Itoa(int(info.Uptime))
		stats["process_running"] = strconv.Itoa(int(info.ProcessesRunning))
		stats["process_total"] = strconv.Itoa(int(info.ProcessesTotal))
		stats["process_zombee"] = strconv.Itoa(int(info.ProcessesZombee))
		stats["memory_alloc"] = strconv.Itoa(int(info.MemoryAlloc))
		stats["memory_used"] = strconv.Itoa(int(info.MemoryUsed))
	}
	stats["gorountine"] = strconv.Itoa(runtime.NumGoroutine())
	return stats
}
