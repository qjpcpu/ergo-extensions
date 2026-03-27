package cron

import (
	"testing"
	"time"
)

func TestCompiledScheduleNext(t *testing.T) {
	schedule, err := compileSchedule("*/15 * * * *", LocationUTC)
	if err != nil {
		t.Fatalf("compile schedule: %v", err)
	}
	next, ok := schedule.Next(time.Date(2026, 3, 27, 10, 7, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("expected next schedule")
	}
	expected := time.Date(2026, 3, 27, 10, 15, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("unexpected next schedule: got %s want %s", next, expected)
	}
}

func TestCompiledScheduleNext_Simple(t *testing.T) {
	tests := []struct {
		name     string
		spec     string
		from     time.Time
		expected time.Time
	}{
		{
			name:     "every minute",
			spec:     "* * * * *",
			from:     time.Date(2026, 3, 27, 10, 7, 30, 0, time.UTC),
			expected: time.Date(2026, 3, 27, 10, 8, 0, 0, time.UTC),
		},
		{
			name:     "hourly",
			spec:     "0 * * * *",
			from:     time.Date(2026, 3, 27, 10, 7, 30, 0, time.UTC),
			expected: time.Date(2026, 3, 27, 11, 0, 0, 0, time.UTC),
		},
		{
			name:     "daily at midnight",
			spec:     "0 0 * * *",
			from:     time.Date(2026, 3, 27, 10, 7, 30, 0, time.UTC),
			expected: time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "monday at 9am",
			spec:     "0 9 * * 1",
			from:     time.Date(2026, 3, 27, 10, 7, 30, 0, time.UTC),
			expected: time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC),
		},
		{
			name:     "first of month",
			spec:     "0 0 1 * *",
			from:     time.Date(2026, 3, 27, 10, 7, 30, 0, time.UTC),
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, err := compileSchedule(tt.spec, LocationUTC)
			if err != nil {
				t.Fatalf("compile schedule: %v", err)
			}
			next, ok := schedule.Next(tt.from)
			if !ok {
				t.Fatalf("expected next schedule")
			}
			if !next.Equal(tt.expected) {
				t.Fatalf("unexpected next schedule: got %s want %s", next, tt.expected)
			}
		})
	}
}

func TestCompileSchedule_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{"too few fields", "* * * *", true},
		{"too many fields", "* * * * * *", true},
		{"invalid minute", "60 * * * *", true},
		{"invalid hour", "* 24 * * *", true},
		{"invalid day", "* * 32 * *", true},
		{"invalid month", "* * * 13 *", true},
		{"invalid weekday", "* * * * 8", true},
		{"empty field", "* * * *", true},
		{"invalid step", "*/0 * * * *", true},
		{"invalid range", "31-30 * * * *", true},
		{"invalid location", "* * * * *", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compileSchedule(tt.spec, LocationUTC)
			if (err != nil) != tt.wantErr {
				t.Errorf("compileSchedule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCompileSchedule_Complex(t *testing.T) {
	tests := []struct {
		name string
		spec string
	}{
		{"multiple values", "5,10,15 * * * *"},
		{"range", "0-5 * * * *"},
		{"range with step", "0-10/2 * * * *"},
		{"all fields with ranges", "0-59/5 0-23/2 1-31 1-12 0-6"},
		{"every 5 minutes", "*/5 * * * *"},
		{"every 2 hours", "0 */2 * * *"},
		{"weekdays and weekends", "0 9 * * 1-5"},
		{"sunday as 0", "0 0 * * 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compileSchedule(tt.spec, LocationUTC)
			if err != nil {
				t.Errorf("compileSchedule() failed: %v", err)
			}
		})
	}
}

func TestCompileSchedule_TimeLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		wantErr  bool
	}{
		{"UTC", "UTC", false},
		{"Local", "Local", false},
		{"Beijing", "Asia/Shanghai", false},
		{"New York", "America/New_York", false},
		{"invalid", "Invalid/Location", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compileSchedule("* * * * *", tt.location)
			if (err != nil) != tt.wantErr {
				t.Errorf("compileSchedule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCompileJob(t *testing.T) {
	tests := []struct {
		name    string
		job     JobSpec
		wantErr bool
	}{
		{
			name: "valid job",
			job: JobSpec{
				ID:             "test-job",
				Schedule:       "* * * * *",
				TriggerProcess: "test_proc",
			},
			wantErr: false,
		},
		{
			name: "missing ID",
			job: JobSpec{
				Schedule:       "* * * * *",
				TriggerProcess: "test_proc",
			},
			wantErr: true,
		},
		{
			name: "missing trigger",
			job: JobSpec{
				ID:             "test-job",
				Schedule:       "* * * * *",
				TriggerProcess: "",
			},
			wantErr: true,
		},
		{
			name: "invalid schedule",
			job: JobSpec{
				ID:             "test-job",
				Schedule:       "invalid",
				TriggerProcess: "test_proc",
			},
			wantErr: true,
		},
		{
			name: "shard key defaults to ID",
			job: JobSpec{
				ID:             "test-job",
				Schedule:       "* * * * *",
				TriggerProcess: "test_proc",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := compileJob(tt.job)
			if (err != nil) != tt.wantErr {
				t.Errorf("compileJob() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && err == nil {
				if job.Spec.ShardKey == "" && tt.job.ShardKey == "" {
					// ShardKey should default to ID
					if job.Spec.ShardKey != tt.job.ID {
						t.Errorf("ShardKey should default to ID, got %q", job.Spec.ShardKey)
					}
				}
			}
		})
	}
}

func TestCompiledScheduleDayAndWeekdayUseCronOR(t *testing.T) {
	schedule, err := compileSchedule("0 9 1 * 1", LocationUTC)
	if err != nil {
		t.Fatalf("compile schedule: %v", err)
	}

	next, ok := schedule.Next(time.Date(2026, 3, 30, 9, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatalf("expected next schedule")
	}

	expected := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("unexpected next schedule: got %s want %s", next, expected)
	}
}
