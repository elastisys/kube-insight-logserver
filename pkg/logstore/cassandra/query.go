package cassandra

import (
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/logstore"
)

// timePeriod represents a time span with a start and end-time (inclusive)
type timePeriod struct {
	start time.Time
	end   time.Time
}

func (p timePeriod) String() string {
	return "[" + p.start.Format(time.RFC3339Nano) + ", " + p.end.Format(time.RFC3339Nano) + "]"
}

// divideByDays takes a timePeriod and breaks it into sub-timePeriods
// on every date border. For instance, the time-period
//   ["2018-10-10T23:00:00Z", "2018-10-12T01:00:00Z"]
// would be divided into
//   [
//     ["2018-10-10T23:00:00Z", "2018-10-10T23:59:59.999999999Z"],
//     ["2018-10-11T00:00:00Z", "2018-10-11T23:59:59.999999999Z"],
//     ["2018-10-12T00:00:00Z", "2018-10-12T01:00:00Z"]
//   ]
func (p timePeriod) divideByDays() []timePeriod {
	// time-period does not cross any date borders
	if date(p.start) == date(p.end) {
		return []timePeriod{p}
	}

	dayPeriods := make([]timePeriod, 0)
	t := p.start
	for date(t).Before(date(p.end)) {
		dayStart := t
		dayEnd := date(t).Add(24 * time.Hour).Add(-1 * time.Nanosecond)
		dayPeriods = append(dayPeriods, timePeriod{dayStart, dayEnd})

		t = date(t).Add(24 * time.Hour)
	}
	// add period for remaining day
	dayPeriods = append(dayPeriods, timePeriod{date(p.end), p.end})

	return dayPeriods
}

// date returns a Time that is truncated down to the date (year-month-day)
// with all other time fields zeroed.
func date(t time.Time) time.Time {
	return t.Truncate(24 * time.Hour)
}

// querySplitter builds a range of queries divided into sub-queries for each
// day that the query interval spans. The querySplitter assumes that its
// wrapped Query is valid.
type querySplitter struct {
	*logstore.Query
}

// Build constructs the queries necessary to fetch the log entries requested
// by a QueryBuilder. This includes validating inputs and breaking the query
// into multiple sub-queries in case the time interval spans date borders.
func (s *querySplitter) Split() (subQueries []*logstore.Query) {
	subQueries = make([]*logstore.Query, 0)

	// divide into separate queries for each day that the query interval covers
	queryDays := timePeriod{start: s.StartTime, end: s.EndTime}.divideByDays()
	for _, queryDay := range queryDays {
		subQueries = append(subQueries, &logstore.Query{
			Namespace:     s.Namespace,
			PodName:       s.PodName,
			ContainerName: s.ContainerName,
			StartTime:     queryDay.start,
			EndTime:       queryDay.end,
		})
	}

	return subQueries
}
