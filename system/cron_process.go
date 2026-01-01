package system

import (
	"encoding/json"
	"errors"
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

const CronJobProcess = gen.Atom("sysext_cron")

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
}

func factory_cron(jobs []CronJob) gen.ProcessFactory {
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
	w.SendAfter(w.PID(), start_init{}, time.Second*3)
	return nil
}

func (w *cron) HandleMessage(from gen.PID, message any) error {
	switch message.(type) {
	case start_init:
		if err := w.setupRegistrarMonitoring(); err != nil {
			w.SendAfter(w.PID(), start_init{}, time.Second*5)
		} else {
			w.turnOnLocalCronJobs()
			w.turnOnClusterCronJobs()
		}
	case schedule_cronjob:
		w.turnOnClusterCronJobs()
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
	if cancel, err := w.SendAfter(w.PID(), schedule_cronjob{}, time.Second*5); err == nil {
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
		bj, _ := time.LoadLocation("Asia/Shanghai")
		return bj
	}
}

func (w *cron) HandleEvent(event gen.MessageEvent) error {
	switch event.Message.(type) {
	case zk.EventNodeJoined, zk.EventNodeLeft:
		w.scheduleClusterCronJobs()
	}
	return nil
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
