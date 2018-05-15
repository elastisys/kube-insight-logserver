package cassandra

import (
	"reflect"
	"testing"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/logstore"
	"github.com/stretchr/testify/assert"
)

func MustParse(isoTime string) time.Time {
	t, _ := time.Parse(time.RFC3339, isoTime)
	return t
}

// equal checks that two timePeriod slices are equal
func timePeriodsEqual(periods1, periods2 []timePeriod) bool {
	if len(periods1) != len(periods2) {
		return false
	}
	for i := range periods1 {
		if periods1[i].start != periods2[i].start {
			return false
		}
		if periods1[i].end != periods2[i].end {
			return false
		}
	}
	return true
}

// Verify that timePeriod.divideByDays correctly splits a time period that spans
// date borders into multiple time-periods (one per day).
func TestTimePeriodDivideByDays(t *testing.T) {
	tests := []struct {
		// period to be split (potentially covering multiple days)
		period timePeriod
		// expected split result
		expectedDaySplit []timePeriod
	}{
		{
			// no date borders crossed: should not be split
			period: timePeriod{MustParse("2018-05-01T12:00:00Z"), MustParse("2018-05-01T14:00:00Z")},
			expectedDaySplit: []timePeriod{
				timePeriod{MustParse("2018-05-01T12:00:00Z"), MustParse("2018-05-01T14:00:00Z")},
			},
		},
		{
			// crosses single date border: should be split
			period: timePeriod{MustParse("2018-05-01T12:00:00Z"), MustParse("2018-05-02T11:00:00Z")},
			expectedDaySplit: []timePeriod{
				timePeriod{MustParse("2018-05-01T12:00:00Z"), MustParse("2018-05-01T23:59:59.999999999Z")},
				timePeriod{MustParse("2018-05-02T00:00:00Z"), MustParse("2018-05-02T11:00:00Z")},
			},
		},
		{
			// crosses two date borders: should be split
			period: timePeriod{MustParse("2018-05-01T23:59:00Z"), MustParse("2018-05-03T01:00:00Z")},
			expectedDaySplit: []timePeriod{
				timePeriod{MustParse("2018-05-01T23:59:00Z"), MustParse("2018-05-01T23:59:59.999999999Z")},
				timePeriod{MustParse("2018-05-02T00:00:00Z"), MustParse("2018-05-02T23:59:59.999999999Z")},
				timePeriod{MustParse("2018-05-03T00:00:00Z"), MustParse("2018-05-03T01:00:00Z")},
			},
		},
	}

	for _, test := range tests {
		daySplit := test.period.divideByDays()
		if !timePeriodsEqual(daySplit, test.expectedDaySplit) {
			t.Errorf("unexpected day split for time period %s: expected: %v, was: %v", test.period, test.expectedDaySplit, daySplit)
		}
	}
}

// Verify that a timePeriod is a Stringer
func TestTimePeriodToString(t *testing.T) {
	p := &timePeriod{MustParse("2018-05-01T23:59:00Z"), MustParse("2018-05-01T23:59:59.999999999Z")}
	expected := "[2018-05-01T23:59:00Z, 2018-05-01T23:59:59.999999999Z]"
	if p.String() != expected {
		t.Errorf("unexepcted timePeriod.String() result: expected: %s, was: %s", expected, p.String())
	}
}

func query(startTime, endTime time.Time) *logstore.Query {
	return &logstore.Query{
		Namespace:     "ns",
		PodName:       "nginx-abcde",
		ContainerName: "nginx",
		StartTime:     startTime,
		EndTime:       endTime,
	}
}

func queriesEqual(queries1, queries2 []*logstore.Query) bool {
	if len(queries1) != len(queries2) {
		return false
	}
	for i := range queries1 {
		if !reflect.DeepEqual(*queries1[i], *queries2[i]) {
			return false
		}
	}

	return true
}

// Ensure proper behavior of querySplitter.Split(): that is, make sure that a
// query that spans days are split into several sub-queries (one per day), which
// is required to send each sub-query to its Cassandra partition.
func TestQuerySplitter(t *testing.T) {
	tests := []struct {
		query         *logstore.Query
		expectedSplit []*logstore.Query
	}{
		{
			// no day-border crossed => no split
			query: query(MustParse("2018-01-01T00:00:00.000Z"), MustParse("2018-01-01T23:59:59.999Z")),
			expectedSplit: []*logstore.Query{
				query(MustParse("2018-01-01T00:00:00.000Z"), MustParse("2018-01-01T23:59:59.999Z")),
			},
		},
		{
			// day-border crossed => split once
			query: query(MustParse("2018-01-01T23:59:00.000Z"), MustParse("2018-01-02T00:01:00.000Z")),
			expectedSplit: []*logstore.Query{
				query(MustParse("2018-01-01T23:59:00.000Z"), MustParse("2018-01-01T23:59:59.999999999Z")),
				query(MustParse("2018-01-02T00:00:00.000Z"), MustParse("2018-01-02T00:01:00.000Z")),
			},
		},
		{
			// two day-borders crossed => split twice
			query: query(MustParse("2018-01-01T23:59:00.000Z"), MustParse("2018-01-03T00:01:00.000Z")),
			expectedSplit: []*logstore.Query{
				query(MustParse("2018-01-01T23:59:00.000Z"), MustParse("2018-01-01T23:59:59.999999999Z")),
				query(MustParse("2018-01-02T00:00:00.000Z"), MustParse("2018-01-02T23:59:59.999999999Z")),
				query(MustParse("2018-01-03T00:00:00.000Z"), MustParse("2018-01-03T00:01:00.000Z")),
			},
		},
	}
	for _, test := range tests {
		splitter := &querySplitter{test.query}
		subQueries := splitter.Split()
		if !queriesEqual(test.expectedSplit, subQueries) {
			t.Errorf("unexpected query split: expected %v, was: %v", test.expectedSplit, subQueries)
		}
	}
}

func newMap(json string) ReplicationFactorMap {
	m, _ := NewReplicationFactorMap(json)
	return m
}

// ReplicationFactorMap to String conversion.
func TestReplicationFactorMapString(t *testing.T) {
	tests := []struct {
		factorMap      ReplicationFactorMap
		expectedString string
	}{
		{
			newMap("{}"),
			"",
		},
		{
			newMap(`{"dc1": 1}`),
			"'dc1': 1",
		},
		{
			newMap(`{"dc1": 1, "dc2": 2}`),
			"'dc1': 1, 'dc2': 2",
		},
	}

	for _, test := range tests {
		assert.Equalf(t, test.expectedString, test.factorMap.String(), "unexpected string representation")
	}
}
