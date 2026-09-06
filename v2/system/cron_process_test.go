package system_test

import (
	"strconv"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/v2/app"
	"github.com/qjpcpu/ergo-extensions/v2/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/v2/system"
	cronpkg "github.com/qjpcpu/ergo-extensions/v2/system/cron"
)

func TestCronShardsAssignedToSingleNode(t *testing.T) {
	cluster := mem.NewCluster()
	const shardCount = 32

	source := cronpkg.NewStaticSource(shardCount,
		cronpkg.JobSpec{
			ID:             "job-a",
			ShardKey:       "job-a",
			Schedule:       "* * * * *",
			Location:       cronpkg.LocationUTC,
			TriggerProcess: gen.Atom("trigger_proc"),
		},
		cronpkg.JobSpec{
			ID:             "job-b",
			ShardKey:       "job-b",
			Schedule:       "* * * * *",
			Location:       cronpkg.LocationUTC,
			TriggerProcess: gen.Atom("trigger_proc"),
		},
	)
	store := cronpkg.NewMemoryKVStore()

	startCronNode := func(name string) app.Node {
		name = uniqueNodeName(name)
		n, err := app.StartSimpleNode(app.SimpleNodeOptions{
			ActorRoutePersistence: sharedIntegrationRoutes,
			NodeName:              name,
			Cookie:                "cron-test-cookie",
			Registrar:             mem.CreateWithCluster(cluster),
			CronSource:            cronpkg.NewManagedSource(source, store),
			CronSchedulerOptions: cronpkg.SchedulerOptions{
				ShardCount:     shardCount,
				RebalanceDelay: 200 * time.Millisecond,
				InitDelay:      50 * time.Millisecond,
			},
		})
		if err != nil {
			t.Fatalf("start node %s: %v", name, err)
		}
		t.Cleanup(func() { n.Stop() })
		return n
	}

	n1 := startCronNode("node-a@127.0.0.1")
	n2 := startCronNode("node-b@127.0.0.1")

	waitUntil(t, 8*time.Second, func() bool {
		return loadedJobs(t, n1)+loadedJobs(t, n2) == 2
	})

	n3 := startCronNode("node-c@127.0.0.1")
	waitUntil(t, 8*time.Second, func() bool {
		total := loadedJobs(t, n1) + loadedJobs(t, n2) + loadedJobs(t, n3)
		return total == 2
	})
}

func loadedJobs(t *testing.T, n app.Node) int {
	t.Helper()
	res, err := n.ForwardCall(string(system.CronJobProcess), "inspect", app.ForwardNode(n.Name()))
	if err != nil {
		t.Fatalf("inspect cron process on node %s: %v", n.Name(), err)
	}
	stats, ok := res.(map[string]string)
	if !ok {
		t.Fatalf("unexpected inspect payload: %T", res)
	}
	value, err := strconv.Atoi(trimJSON(stats["loaded_jobs"]))
	if err != nil {
		t.Fatalf("parse loaded_jobs: %v", err)
	}
	return value
}

func trimJSON(v string) string {
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		return v[1 : len(v)-1]
	}
	return v
}
