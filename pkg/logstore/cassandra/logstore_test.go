package cassandra

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
	"github.com/elastisys/kube-insight-logserver/pkg/logstore"
	api "github.com/elastisys/kube-insight-logserver/pkg/logstore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// MockedCQLDriver is a mocked object that implements CQLDriver.
type MockedCQLDriver struct {
	mock.Mock
}

func (m *MockedCQLDriver) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedCQLDriver) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedCQLDriver) Reachable() (bool, error) {
	args := m.Called()
	return args.Bool(0), args.Error(1)
}

func (m *MockedCQLDriver) Execute(statement string, placeholders ...interface{}) error {
	args := m.Called(statement, placeholders)
	return args.Error(0)
}

func (m *MockedCQLDriver) Query(query string, placeholders ...interface{}) (CQLRows, error) {
	args := m.Called(query, placeholders)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(CQLRows), args.Error(1)
}

func options() *Options {
	return &Options{
		Hosts:               []string{"localhost"},
		CQLPort:             9042,
		Keyspace:            "keyspace",
		LogTableName:        "logtable",
		ReplicationStrategy: "",
		ReplicationFactors:  map[string]int{"cluster": 3},
		WriteConcurrency:    4,
	}
}

// Verify that LogStore.Connect(..) creates the keyspace and table if they don't
// already exist.
func TestLogStoreConnect(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	//
	// set up mock expectations
	//
	// LogStore should connect to Cassandra
	mockCQLDriver.On("Connect").Return(nil)
	var emptyPlaceholders []interface{}
	// LogStore should create keyspace if it doesn't exist already
	mockCQLDriver.On("Execute", logStore.keyspaceDeclaration(), emptyPlaceholders).Return(nil)
	// LogStore should create log table if it doesn't exist already
	mockCQLDriver.On("Execute", logStore.tableDeclaration(), emptyPlaceholders).Return(nil)

	//
	// make call
	//
	err := logStore.Connect()
	require.Nilf(t, err, "connect not expected to return error")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Connect(..) creates the a keyspace with
// NetworkTopologyStrategy and replication factors when its options dictate so.
func TestLogStoreConnectWithNetworkTopologyStrategy(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	opts := options()
	opts.ReplicationStrategy = NetworkTopologyStrategy
	opts.ReplicationFactors = map[string]int{"dc1": 3, "dc2": 4}
	logStore := NewLogStore(mockCQLDriver, opts)

	assert.Containsf(t, logStore.keyspaceDeclaration(), "NetworkTopologyStrategy",
		"expected keyspace declaration to use NetworkTopologyStrategy")
	assert.Containsf(t, logStore.keyspaceDeclaration(), `'dc1': 3`,
		"expected keyspace declaration to contain replication factor 'dc1': 3")
	assert.Containsf(t, logStore.keyspaceDeclaration(), `'dc2': 4`,
		"expected keyspace declaration to contain replication factor 'dc2': 4")

	//
	// set up mock expectations
	//
	// LogStore should connect to Cassandra
	mockCQLDriver.On("Connect").Return(nil)
	var emptyPlaceholders []interface{}
	// LogStore should create keyspace if it doesn't exist already
	mockCQLDriver.On("Execute", logStore.keyspaceDeclaration(), emptyPlaceholders).Return(nil)
	// LogStore should create log table if it doesn't exist already
	mockCQLDriver.On("Execute", logStore.tableDeclaration(), emptyPlaceholders).Return(nil)

	//
	// make call
	//
	err := logStore.Connect()
	require.Nilf(t, err, "connect not expected to return error")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Connect(..) creates the keyspace and table if they don't
// already exist.
func TestLogStoreConnectOnDriverConnectError(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	//
	// set up mock expectations
	//
	// LogStore should connect to Cassandra
	driverErr := fmt.Errorf("connection refused")
	mockCQLDriver.On("Connect").Return(driverErr)

	//
	// make call
	//
	err := logStore.Connect()
	require.Equalf(t, driverErr, err, "expected connect to fail")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Disconnect(..) disconnects from the driver.
func TestLogStoreDisconnect(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	//
	// set up mock expectations
	//
	mockCQLDriver.On("Close").Return(nil)

	//
	// make call
	//
	err := logStore.Disconnect()
	require.Nilf(t, err, "disconnect not expected to return error")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Connect(..) returns a SchemaError on failure to create
// keyspace.
func TestLogStoreOnKeyspaceCreateError(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	//
	// set up mock expectations
	//

	// LogStore should connect to Cassandra
	mockCQLDriver.On("Connect").Return(nil)
	var emptyPlaceholders []interface{}

	// driver will fail keyspace creation
	driverErr := fmt.Errorf("internal error")
	mockCQLDriver.On("Execute", logStore.keyspaceDeclaration(), emptyPlaceholders).Return(driverErr)

	//
	// make call
	//
	err := logStore.Connect()
	expectedErr := SchemaError{message: "failed to create keyspace", cause: driverErr}
	require.Equalf(t, expectedErr, err, "expected connect to fail with schema creation error")
	require.Equalf(t, "schema creation failed: failed to create keyspace: internal error", err.Error(), "unexpected error message")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Connect(..) returns a SchemaError on failure to create
// the log table.
func TestLogStoreOnTableCreateError(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	//
	// set up mock expectations
	//

	// LogStore should connect to Cassandra
	mockCQLDriver.On("Connect").Return(nil)
	var emptyPlaceholders []interface{}
	// LogStore should create keyspace
	mockCQLDriver.On("Execute", logStore.keyspaceDeclaration(), emptyPlaceholders).Return(nil)
	// driver will fail log table creation
	driverErr := fmt.Errorf("internal error")
	mockCQLDriver.On("Execute", logStore.tableDeclaration(), emptyPlaceholders).Return(driverErr)

	//
	// make call
	//
	err := logStore.Connect()
	expectedErr := SchemaError{message: "failed to create log table", cause: driverErr}
	require.Equalf(t, expectedErr, err, "expected connect to fail with schema creation error")
	require.Equalf(t, "schema creation failed: failed to create log table: internal error", err.Error(), "unexpected error message")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Ready(..) queries the Driver.
func TestLogStoreReadyProbeOnSuccess(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())
	//
	// set up mock expectations
	//
	mockCQLDriver.On("Reachable").Return(true, nil)

	//
	// make call
	//
	ok, err := logStore.Ready()
	assert.True(t, ok, "expected logStore.Ready() to be false")
	assert.Nil(t, err, "expected logStore.Ready() to not return error")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that logStore.Ready(..) queries the Driver.
func TestLogStoreReadyProbeOnFailure(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())
	//
	// set up mock expectations
	//
	mockCQLDriver.On("Reachable").Return(false, fmt.Errorf("connection refused"))

	//
	// make call
	//
	ok, err := logStore.Ready()
	assert.False(t, ok, "expected LogStore.Ready() to be false")
	assert.Equalf(t, fmt.Errorf("connection refused"), err, "expected connection refused error")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Query(..) sends the expected query/queries  to the
// backend and converts the reponses to expected results.
func TestLogStoreQuery(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	queryStart := MustParse("2018-01-01T12:00:00.000Z")
	queryEnd := MustParse("2018-01-01T14:00:00.000Z")
	query := &api.Query{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "container",
		StartTime:     queryStart,
		EndTime:       queryEnd,
	}
	//
	// set up mock expectations
	//

	// query result
	queryResult := CQLRows([]map[string]interface{}{
		{"time": MustParse("2018-01-01T12:30:00.000Z"), "message": "event 1"},
		{"time": MustParse("2018-01-01T13:00:00.000Z"), "message": "event 2"},
	})
	queryDate := query.StartTime.Format("2006-01-02")
	expectedPlaceholders := []interface{}{
		query.Namespace, query.PodName, query.ContainerName, queryDate, query.StartTime, query.EndTime,
	}
	mockCQLDriver.On("Query", logStore.logQueryStatement(), expectedPlaceholders).Return(queryResult, nil)

	//
	// make call
	//
	results, err := logStore.Query(query)
	assert.Nil(t, err, "expected error return to be nil")
	expectedRows := []logstore.LogRow{
		logstore.LogRow{Time: MustParse("2018-01-01T12:30:00.000Z"), Log: "event 1"},
		logstore.LogRow{Time: MustParse("2018-01-01T13:00:00.000Z"), Log: "event 2"},
	}
	assert.Truef(t, reflect.DeepEqual(expectedRows, results.LogRows),
		"unexpected result set: expected: %#v, was: %#v", expectedRows, results.LogRows)

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that LogStore.Query(..) splits a query spanning a date border into two
// sub-queries, one for each date (in order to correctly query a single
// partition with each query).
func TestLogStoreQueryThatCrossesDateBorder(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())
	// query spans a date border
	queryStart := MustParse("2018-01-01T23:59:00.000Z")
	queryEnd := MustParse("2018-01-02T00:01:00.000Z")
	query := &api.Query{
		Namespace:     "ns",
		PodName:       "pod",
		ContainerName: "container",
		StartTime:     queryStart,
		EndTime:       queryEnd,
	}
	//
	// set up mock expectations
	//

	// set up return values for each sub-query
	query1Result := CQLRows([]map[string]interface{}{
		{"time": MustParse("2018-01-01T23:59:59.100Z"), "message": "day 1, event 1"},
		{"time": MustParse("2018-01-01T23:59:59.200Z"), "message": "day 1, event 2"},
	})
	query2Result := CQLRows([]map[string]interface{}{
		{"time": MustParse("2018-01-02T00:00:30.000Z"), "message": "day 2, event 1"},
		{"time": MustParse("2018-01-02T00:00:45.000Z"), "message": "day 2, event 2"},
	})

	// start/end times of the expected query split
	firstQueryStart := MustParse("2018-01-01T23:59:00.000Z")
	firstQueryEnd := MustParse("2018-01-01T23:59:59.999999999Z")
	secondQueryStart := MustParse("2018-01-02T00:00:00.000Z")
	secondQueryEnd := MustParse("2018-01-02T00:01:00.000Z")

	// expect two calls to Driver.Query()
	query1Date := firstQueryStart.Format("2006-01-02")
	query1ExpectedPlaceholders := []interface{}{
		query.Namespace, query.PodName, query.ContainerName, query1Date, firstQueryStart, firstQueryEnd,
	}
	query2Date := secondQueryStart.Format("2006-01-02")
	query2ExpectedPlaceholders := []interface{}{
		query.Namespace, query.PodName, query.ContainerName, query2Date, secondQueryStart, secondQueryEnd,
	}
	mockCQLDriver.On("Query", logStore.logQueryStatement(), query1ExpectedPlaceholders).Return(query1Result, nil)
	mockCQLDriver.On("Query", logStore.logQueryStatement(), query2ExpectedPlaceholders).Return(query2Result, nil)

	//
	// make call
	//
	results, err := logStore.Query(query)
	assert.Nil(t, err, "expected error return to be nil")
	// verify that rows are returned in the right order
	expectedRows := []logstore.LogRow{
		logstore.LogRow{Time: MustParse("2018-01-01T23:59:59.100Z"), Log: "day 1, event 1"},
		logstore.LogRow{Time: MustParse("2018-01-01T23:59:59.200Z"), Log: "day 1, event 2"},
		logstore.LogRow{Time: MustParse("2018-01-02T00:00:30.000Z"), Log: "day 2, event 1"},
		logstore.LogRow{Time: MustParse("2018-01-02T00:00:45.000Z"), Log: "day 2, event 2"},
	}
	assert.Truef(t, reflect.DeepEqual(expectedRows, results.LogRows),
		"unexpected result set: expected: %#v, was: %#v", expectedRows, results.LogRows)

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// On Driver.Query() error, LogStore should return a QueryError.
func TestLogStoreQueryOnError(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	queryStart := MustParse("2018-01-01T12:00:00.000Z")
	queryEnd := MustParse("2018-01-01T14:00:00.000Z")
	query := &api.Query{Namespace: "ns", PodName: "pod", ContainerName: "container",
		StartTime: queryStart, EndTime: queryEnd}
	//
	// set up mock expectations
	//

	// Driver should respond with an error
	driverErr := fmt.Errorf("connection refused")
	queryDate := query.StartTime.Format("2006-01-02")
	expectedPlaceholders := []interface{}{
		query.Namespace, query.PodName, query.ContainerName, queryDate, query.StartTime, query.EndTime,
	}
	mockCQLDriver.On("Query", logStore.logQueryStatement(), expectedPlaceholders).Return(nil, driverErr)

	//
	// make call
	//
	results, err := logStore.Query(query)
	assert.NotNilf(t, err, "expected error return")
	assert.Nilf(t, results, "expected nil result")
	expectedErr := QueryError{"query execution", driverErr}
	assert.Equalf(t, expectedErr, err, "unexpected err: expected: %s, was: %s", expectedErr, err)
	assert.Equalf(t, "query failed: query execution: connection refused", err.Error(), "unexpected err message")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

func logEntry(timestamp time.Time, message string) logstore.LogEntry {
	return logstore.LogEntry{
		Date: float64(timestamp.UnixNano() / 1.0e9),
		Kubernetes: api.KubernetesMetadata{
			DockerID: "e4b0b3eb8c25a73351c5cfeb37a9d64736584c640f21010443fe2e7e5b9c085b",
			Labels: map[string]string{
				"pod-template-generation": "1",
				"app": "nginx",
			},
			Host:          "worker0",
			PodName:       "nginx-deployment-abcde",
			ContainerName: "nginx",
			PodID:         "1021f36b-4e9e-11e8-8b6b-02425d6e035a",
			Namespace:     "default",
		},
		Log:    message,
		Stream: "stdout",
		Time:   timestamp,
	}
}

// Verify that the LogStore produces expected INSERT statements for log entries.
func TestInsertStatement(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	assert.Equalf(t,
		fmt.Sprintf("INSERT INTO %s.%s "+
			"(namespace, pod_name, container_name, date, time, message, stream, pod_id, docker_id, host, labels) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", options().Keyspace, options().LogTableName),
		logStore.insertStatement(),
		"unexepected insert statement",
	)
}

// Verify that LogStore.Write() sends expected insert statements to the backend.
func TestLogStoreWrite(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	logEntries := []logstore.LogEntry{
		logEntry(MustParse("2018-01-01T12:00:00.000Z"), "event 1"),
		logEntry(MustParse("2018-01-01T12:01:00.000Z"), "event 2"),
		logEntry(MustParse("2018-01-01T12:02:00.000Z"), "event 3"),
	}

	//
	// set up mock expectations
	//

	// one insert should be executed per log entry
	for _, logEntry := range logEntries {
		// LogStore should create keyspace if it doesn't exist already
		mockCQLDriver.On("Execute", logStore.insertStatement(),
			[]interface{}{
				logEntry.Kubernetes.Namespace,
				logEntry.Kubernetes.PodName,
				logEntry.Kubernetes.ContainerName,
				logEntry.Time.Format("2006-01-02"),
				logEntry.Time,
				logEntry.Log,
				logEntry.Stream,
				logEntry.Kubernetes.PodID,
				logEntry.Kubernetes.DockerID,
				logEntry.Kubernetes.Host,
				logEntry.Kubernetes.Labels,
			}).Return(nil)
	}

	//
	// make call
	//
	err := logStore.Write(logEntries)
	assert.Nilf(t, err, "unexpected error return: %s", err)

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}

// It should be possible to write an empty batch of log records. This should be
// a no-op, which never touches Cassandra.
func TestLogStoreWriteEmptyLogBatch(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	// an empty log batch
	logEntries := []logstore.LogEntry{}

	//
	// set up mock expectations
	//

	// NO inserts should be made against driver

	//
	// make call
	//
	err := logStore.Write(logEntries)
	assert.Nilf(t, err, "unexpected error return")

	// verify that expected calls were (not) made
	mockCQLDriver.AssertExpectations(t)
}

// Verify that that LogStore.Write(..) reacts apropriately on errors
// Verify that LogStore.Write() sends expected insert statements to the backend.
func TestLogStoreWriteOnError(t *testing.T) {
	mockCQLDriver := new(MockedCQLDriver)
	logStore := NewLogStore(mockCQLDriver, options())

	logEntries := []logstore.LogEntry{
		logEntry(MustParse("2018-01-01T12:00:00.000Z"), "event 1"),
	}

	//
	// set up mock expectations
	//

	// driver should fail with error
	driverErr := fmt.Errorf("connection refused")
	mockCQLDriver.On("Execute", logStore.insertStatement(),
		[]interface{}{
			logEntries[0].Kubernetes.Namespace,
			logEntries[0].Kubernetes.PodName,
			logEntries[0].Kubernetes.ContainerName,
			logEntries[0].Time.Format("2006-01-02"),
			logEntries[0].Time,
			logEntries[0].Log,
			logEntries[0].Stream,
			logEntries[0].Kubernetes.PodID,
			logEntries[0].Kubernetes.DockerID,
			logEntries[0].Kubernetes.Host,
			logEntries[0].Kubernetes.Labels,
		}).Return(driverErr)

	//
	// make call
	//
	err := logStore.Write(logEntries)
	expectedErr := InsertError{driverErr}
	assert.Equalf(t, expectedErr, err, "expected write to fail")
	assert.Equalf(t, "insert failed: connection refused", err.Error(),
		"unexpected error message")

	// verify that expected calls were made
	mockCQLDriver.AssertExpectations(t)
}
