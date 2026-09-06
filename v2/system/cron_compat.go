package system

import cronpkg "github.com/qjpcpu/ergo-extensions/v2/system/cron"

const (
	CronJobProcess         = cronpkg.ProcessName
	CronJobLocationUTC     = cronpkg.LocationUTC
	CronJobLocationBeijing = "Asia/Shanghai"
)

type CronJob = cronpkg.JobSpec
