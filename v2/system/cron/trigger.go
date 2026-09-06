package cron

import (
	"time"

	"ergo.services/ergo/gen"
)

type DispatchJob struct {
	JobID          string
	TriggerProcess gen.Atom
	ScheduledAt    time.Time
	DispatchKey    string
}

type Trigger interface {
	Fire(process gen.Process, jobs []DispatchJob) ([]DispatchJob, error)
}

type LocalTrigger struct {
	Batch bool
}

func (t LocalTrigger) Fire(process gen.Process, jobs []DispatchJob) ([]DispatchJob, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	var failed []DispatchJob
	var lastErr error
	if !t.Batch {
		for _, job := range jobs {
			if err := process.Send(job.TriggerProcess, MessageTrigger{
				JobID:       job.JobID,
				ScheduledAt: job.ScheduledAt,
				DispatchKey: job.DispatchKey,
			}); err != nil {
				lastErr = err
				failed = append(failed, job)
			}
		}
		return failed, lastErr
	}

	grouped := make(map[gen.Atom][]MessageTrigger)
	groupedJobs := make(map[gen.Atom][]DispatchJob)
	for _, job := range jobs {
		grouped[job.TriggerProcess] = append(grouped[job.TriggerProcess], MessageTrigger{
			JobID:       job.JobID,
			ScheduledAt: job.ScheduledAt,
			DispatchKey: job.DispatchKey,
		})
		groupedJobs[job.TriggerProcess] = append(groupedJobs[job.TriggerProcess], job)
	}
	for target, batch := range grouped {
		if len(batch) == 1 {
			if err := process.Send(target, batch[0]); err != nil {
				lastErr = err
				failed = append(failed, groupedJobs[target]...)
			}
			continue
		}
		if err := process.Send(target, MessageTriggerBatch{Jobs: batch}); err != nil {
			lastErr = err
			failed = append(failed, groupedJobs[target]...)
		}
	}
	return failed, lastErr
}
