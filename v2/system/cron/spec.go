package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type CompiledJob struct {
	Spec     JobSpec
	Schedule compiledSchedule
}

type compiledSchedule struct {
	location *time.Location
	minute   fieldMatcher
	hour     fieldMatcher
	day      fieldMatcher
	month    fieldMatcher
	weekday  fieldMatcher
}

type fieldMatcher struct {
	values   map[int]struct{}
	wildcard bool
}

func compileJob(job JobSpec) (*CompiledJob, error) {
	if job.ID == "" {
		return nil, fmt.Errorf("cron job missing id")
	}
	if job.ShardKey == "" {
		job.ShardKey = job.ID
	}
	if job.TriggerProcess == "" {
		return nil, fmt.Errorf("cron job %s missing trigger process", job.ID)
	}
	schedule, err := compileSchedule(job.Schedule, job.Location)
	if err != nil {
		return nil, err
	}
	return &CompiledJob{Spec: job, Schedule: schedule}, nil
}

func compileSchedule(spec string, location string) (compiledSchedule, error) {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return compiledSchedule{}, fmt.Errorf("invalid cron spec %q", spec)
	}
	loc := time.UTC
	if location != "" {
		loaded, err := time.LoadLocation(location)
		if err != nil {
			return compiledSchedule{}, fmt.Errorf("load cron location %q: %w", location, err)
		}
		loc = loaded
	}
	minute, err := parseField(fields[0], 0, 59)
	if err != nil {
		return compiledSchedule{}, err
	}
	hour, err := parseField(fields[1], 0, 23)
	if err != nil {
		return compiledSchedule{}, err
	}
	day, err := parseField(fields[2], 1, 31)
	if err != nil {
		return compiledSchedule{}, err
	}
	month, err := parseField(fields[3], 1, 12)
	if err != nil {
		return compiledSchedule{}, err
	}
	weekday, err := parseField(fields[4], 0, 7)
	if err != nil {
		return compiledSchedule{}, err
	}
	if _, ok := weekday.values[7]; ok {
		delete(weekday.values, 7)
		weekday.values[0] = struct{}{}
	}
	return compiledSchedule{
		location: loc,
		minute:   minute,
		hour:     hour,
		day:      day,
		month:    month,
		weekday:  weekday,
	}, nil
}

func (c compiledSchedule) Next(after time.Time) (time.Time, bool) {
	start := after.In(c.location).Truncate(time.Minute).Add(time.Minute)
	const maxMinutes = 366 * 24 * 60 * 5
	limit := start.Add(maxMinutes * time.Minute)
	for t := start; t.Before(limit); {
		var next time.Time
		switch {
		case !c.month.match(int(t.Month())):
			next = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, c.location)
		case !c.matchesDay(t):
			next = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, c.location)
		case !c.hour.match(t.Hour()):
			next = t.Add(time.Duration(60-t.Minute()) * time.Minute)
		case !c.minute.match(t.Minute()):
			next = t.Add(time.Minute)
		default:
			return t.UTC(), true
		}
		// Recheck calendar fields at offset changes, including repeated DST hours.
		_, end := t.ZoneBounds()
		if !end.IsZero() && end.Before(next) {
			next = end.Truncate(time.Minute)
			if next.Before(end) {
				next = next.Add(time.Minute)
			}
		}
		if !next.After(t) {
			next = t.Add(time.Minute)
		}
		t = next
	}
	return time.Time{}, false
}

func (c compiledSchedule) IsDueAt(t time.Time) bool {
	return c.matches(t.In(c.location))
}

func (c compiledSchedule) matches(t time.Time) bool {
	return c.minute.match(t.Minute()) &&
		c.hour.match(t.Hour()) &&
		c.month.match(int(t.Month())) &&
		c.matchesDay(t)
}

func (c compiledSchedule) matchesDay(t time.Time) bool {
	dayMatches := c.day.match(t.Day())
	weekdayMatches := c.weekday.match(int(t.Weekday()))
	dayConstraintMatches := false
	switch {
	case c.day.wildcard && c.weekday.wildcard:
		dayConstraintMatches = true
	case c.day.wildcard:
		dayConstraintMatches = weekdayMatches
	case c.weekday.wildcard:
		dayConstraintMatches = dayMatches
	default:
		dayConstraintMatches = dayMatches || weekdayMatches
	}

	return dayConstraintMatches
}

func (m fieldMatcher) match(v int) bool {
	_, ok := m.values[v]
	return ok
}

func parseField(expr string, min int, max int) (fieldMatcher, error) {
	values := make(map[int]struct{}, max-min+1)
	wildcard := expr == "*"
	for _, part := range strings.Split(expr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return fieldMatcher{}, fmt.Errorf("invalid cron field %q", expr)
		}
		if err := addFieldValues(values, part, min, max); err != nil {
			return fieldMatcher{}, err
		}
	}
	if len(values) == 0 {
		return fieldMatcher{}, fmt.Errorf("empty cron field %q", expr)
	}
	return fieldMatcher{values: values, wildcard: wildcard}, nil
}

func addFieldValues(values map[int]struct{}, expr string, min int, max int) error {
	step := 1
	rangeExpr := expr
	if strings.Contains(expr, "/") {
		parts := strings.Split(expr, "/")
		if len(parts) != 2 {
			return fmt.Errorf("invalid stepped cron field %q", expr)
		}
		rangeExpr = parts[0]
		parsedStep, err := strconv.Atoi(parts[1])
		if err != nil || parsedStep <= 0 {
			return fmt.Errorf("invalid step in cron field %q", expr)
		}
		step = parsedStep
	}

	start := min
	end := max
	switch {
	case rangeExpr == "*":
	case strings.Contains(rangeExpr, "-"):
		parts := strings.Split(rangeExpr, "-")
		if len(parts) != 2 {
			return fmt.Errorf("invalid range in cron field %q", expr)
		}
		var err error
		start, err = strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("invalid range start in cron field %q", expr)
		}
		end, err = strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid range end in cron field %q", expr)
		}
	default:
		value, err := strconv.Atoi(rangeExpr)
		if err != nil {
			return fmt.Errorf("invalid cron field %q", expr)
		}
		start = value
		end = value
	}
	if start < min || end > max || start > end {
		return fmt.Errorf("cron field %q out of range", expr)
	}
	for value := start; value <= end; value += step {
		values[value] = struct{}{}
	}
	return nil
}
