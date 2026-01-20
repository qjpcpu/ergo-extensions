package system

import (
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"

	"ergo.services/registrar/zk"
	"github.com/buraksezer/consistent"
)

type CronJobLocation int

const (
	CronJobLocationBeijing CronJobLocation = iota
	CronJobLocationUTC
)

type CronJobScope int

const (
	CronJobScopeCluster CronJobScope = iota
	CronJobScopeNode
)

const CronJobProcess = gen.Atom("extensions_cron")

// CronJob defines a cron job configuration.
type CronJob struct {
	// Name is the unique name of the cron job.
	Name gen.Atom
	// Spec is the cron expression (e.g., "* * * * *").
	Spec string
	// Location specifies the timezone for the schedule.
	Location CronJobLocation
	// TriggerProcess is the name of the process to receive the trigger message.
	TriggerProcess gen.Atom
	// Scope defines whether the job runs on a single node or across the cluster.
	Scope CronJobScope
}

type cron struct {
	act.Actor
	registrar       gen.Registrar
	prevNodes       map[gen.Atom]struct{}
	ring            *consistent.Consistent // consistent hashing ring
	local, cluster  []CronJob
	startOnSelfJobs map[gen.Atom]struct{}
	cancelSchedule  gen.CancelFunc
	locBJ           *time.Location
}

func factoryCron(jobs []CronJob) gen.ProcessFactory {
	local, cluster := splitJobs(jobs)
	return func() gen.ProcessBehavior {
		return &cron{
			ring:            makeRing(),
			local:           local,
			cluster:         cluster,
			prevNodes:       make(map[gen.Atom]struct{}),
			startOnSelfJobs: make(map[gen.Atom]struct{}),
		}
	}
}

func splitJobs(jobs []CronJob) (local []CronJob, cluster []CronJob) {
	for _, item := range jobs {
		if item.Scope == CronJobScopeNode {
			local = append(local, item)
		} else {
			cluster = append(cluster, item)
		}
	}
	return
}

func (w *cron) Init(args ...any) error {
	if len(w.local) == 0 && len(w.cluster) == 0 {
		return nil
	}
	// Add jitter to avoid synchronized initialization
	delay := time.Second*3 + time.Duration(rand.Intn(1000))*time.Millisecond
	w.SendAfter(w.PID(), messageInit{}, delay)
	return nil
}

func (w *cron) HandleMessage(from gen.PID, message any) error {
	switch message.(type) {
	case messageInit:
		if err := w.setupRegistrarMonitoring(); err != nil {
			w.SendAfter(w.PID(), messageInit{}, time.Second*5)
		} else {
			w.turnOnLocalCronJobs()
			if err := w.turnOnClusterCronJobs(); err != nil {
				w.Log().Error("turn on cluster cron jobs fail: %v, retry in 5s", err)
				w.SendAfter(w.PID(), messageInit{}, time.Second*5)
			}
		}
	case messageScheduleCron:
		if err := w.turnOnClusterCronJobs(); err != nil {
			w.Log().Error("schedule cluster cron jobs fail: %v, retry in 5s", err)
			w.scheduleClusterCronJobs()
		}
	}
	return nil
}

func (w *cron) setupRegistrarMonitoring() error {
	if w.registrar == nil {
		registrar, err := w.Node().Network().Registrar()
		if err != nil {
			return err
		}
		event, err := registrar.Event()
		if err != nil {
			return err
		}
		if _, err := w.MonitorEvent(event); err != nil {
			return err
		}
		w.registrar = registrar
	}
	return nil
}

func (w *cron) turnOnLocalCronJobs() error {
	if len(w.local) == 0 {
		return nil
	}
	c := w.Node().Cron()
	for _, job := range w.local {
		genjob := gen.CronJob{
			Name:     job.Name,
			Spec:     job.Spec,
			Location: w.getLoc(job.Location),
			Action:   gen.CreateCronActionMessage(job.TriggerProcess, gen.MessagePriorityHigh),
		}
		if err := c.AddJob(genjob); err == nil {
			w.Log().Debug("turn on cron job %s", job.Name)
			w.startOnSelfJobs[job.Name] = struct{}{}
		}
	}
	return nil
}

func (w *cron) scheduleClusterCronJobs() error {
	if w.cancelSchedule != nil {
		w.cancelSchedule()
		w.cancelSchedule = nil
	}
	// Add jitter to avoid synchronized cluster cron jobs scheduling
	delay := time.Second*5 + time.Duration(rand.Intn(1000))*time.Millisecond
	if cancel, err := w.SendAfter(w.PID(), messageScheduleCron{}, delay); err == nil {
		w.cancelSchedule = cancel
	}
	return nil
}

func (w *cron) turnOnClusterCronJobs() error {
	if len(w.cluster) == 0 {
		return nil
	}
	if w.registrar == nil {
		return errors.New("no registrar found")
	}
	nodes, err := w.registrar.Nodes()
	if err != nil {
		return err
	}
	nodesMap := make(map[gen.Atom]struct{})
	for _, node := range nodes {
		w.ring.Add(Member(node))
		nodesMap[node] = struct{}{}
	}
	node := w.Node().Name()
	w.ring.Add(Member(node))
	nodesMap[node] = struct{}{}
	for n := range w.prevNodes {
		if _, ok := nodesMap[n]; !ok {
			w.ring.Remove(string(n))
		}
	}
	w.prevNodes = nodesMap

	c := w.Node().Cron()
	for _, job := range w.cluster {
		if target := w.ring.LocateKey([]byte(job.Name)); target != nil && gen.Atom(target.String()) == node {
			genjob := gen.CronJob{
				Name:     job.Name,
				Spec:     job.Spec,
				Location: w.getLoc(job.Location),
				Action:   gen.CreateCronActionMessage(job.TriggerProcess, gen.MessagePriorityHigh),
			}
			if err = c.AddJob(genjob); err == nil {
				w.Log().Debug("turn on cron job %s", job.Name)
				w.startOnSelfJobs[job.Name] = struct{}{}
			}
		} else {
			if err = c.RemoveJob(job.Name); err == nil {
				w.Log().Debug("turn off cron job %s", job.Name)
				delete(w.startOnSelfJobs, job.Name)
			}
		}
	}
	return nil
}

func (w *cron) getLoc(loc CronJobLocation) *time.Location {
	switch loc {
	case CronJobLocationUTC:
		return time.UTC
	default:
		if w.locBJ == nil {
			bj, _ := time.LoadLocation("Asia/Shanghai")
			w.locBJ = bj
		}
		return w.locBJ
	}
}

func (w *cron) HandleEvent(event gen.MessageEvent) error {
	switch event.Message.(type) {
	case zk.EventNodeJoined, zk.EventNodeLeft:
		w.scheduleClusterCronJobs()
	}
	return nil
}

func (w *cron) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	switch request.(type) {
	case string:
		if request.(string) == "inspect" {
			return w.HandleInspect(from), nil
		}
	case messageInspectProcess:
		return w.HandleInspect(from), nil
	}
	return nil, gen.ErrUnsupported
}

func (w *cron) HandleInspect(from gen.PID, item ...string) map[string]string {
	var jobs []string
	for name := range w.startOnSelfJobs {
		jobs = append(jobs, string(name))
	}
	toStr := func(v any) string {
		bs, _ := json.Marshal(v)
		return string(bs)
	}
	return map[string]string{
		"jobs": toStr(jobs),
	}
}
