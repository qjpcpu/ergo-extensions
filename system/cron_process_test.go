package system_test

import (
	"strings"
	"testing"
	"time"

	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/registrar/mem"
	"github.com/qjpcpu/ergo-extensions/system"
	"github.com/qjpcpu/ergo-extensions/app"
)

func TestCronJobs(t *testing.T) {
	cluster := mem.NewCluster()

	localJobName := gen.Atom("local_job")
	clusterJobName := gen.Atom("cluster_job")
	triggerProc := gen.Atom("trigger_proc")

	jobs := []system.CronJob{
		{
			Name:           localJobName,
			Spec:           "* * * * *",
			TriggerProcess: triggerProc,
			Scope:          system.CronJobScopeNode,
		},
		{
			Name:           clusterJobName,
			Spec:           "* * * * *",
			TriggerProcess: triggerProc,
			Scope:          system.CronJobScopeCluster,
		},
	}

	// Helper to start node with cron jobs
	startCronNode := func(name string) app.Node {
		name = uniqueNodeName(name)
		n, err := app.StartSimpleNode(app.SimpleNodeOptions{
			NodeName:            name,
			Cookie:              "cron-test-cookie",
			Registrar:           mem.CreateWithCluster(cluster),
			CronJobs:            jobs,
			SyncProcessInterval: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("start node %s: %v", name, err)
		}
		t.Cleanup(func() { n.Stop() })
		return n
	}

	n1 := startCronNode("node-a@127.0.0.1")
	n2 := startCronNode("node-b@127.0.0.1")

	// Wait for initialization (3s delay in cron_process.go + jitter)
	time.Sleep(12 * time.Second)

	// Check local jobs: should be on both nodes
	checkJob := func(n app.Node, jobName gen.Atom, expected bool) {
		t.Helper()
		found := containsJob(t, n, jobName)
		// The inspect returns a JSON array of started jobs
		if expected {
			if !found {
				t.Errorf("node %s should have job %s", n.Name(), jobName)
			}
		} else {
			if found {
				t.Errorf("node %s should NOT have job %s", n.Name(), jobName)
			}
		}
	}

	checkJob(n1, localJobName, true)
	checkJob(n2, localJobName, true)

	// Check cluster job: should be on exactly one node
	waitUntil(t, 20*time.Second, func() bool {
		on1 := containsJob(t, n1, clusterJobName)
		on2 := containsJob(t, n2, clusterJobName)
		return (on1 && !on2) || (!on1 && on2)
	})

	// Add node 3, cluster job might migrate
	n3 := startCronNode("node-c@127.0.0.1")
	
	waitUntil(t, 30*time.Second, func() bool {
		on1 := containsJob(t, n1, clusterJobName)
		on2 := containsJob(t, n2, clusterJobName)
		on3 := containsJob(t, n3, clusterJobName)
		count := 0
		if on1 { count++ }
		if on2 { count++ }
		if on3 { count++ }
		return count == 1
	})
}

func containsJob(t *testing.T, n app.Node, jobName gen.Atom) bool {
	res, err := n.CallLocal(string(system.CronJobProcess), "inspect")
	if err != nil {
		return false
	}
	m, ok := res.(map[string]string)
	if !ok {
		return false
	}
	return contains(m["jobs"], string(jobName))
}

func contains(jsonArr string, item string) bool {
	return strings.Contains(jsonArr, "\""+item+"\"")
}
