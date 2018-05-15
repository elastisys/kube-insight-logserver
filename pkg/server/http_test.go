package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/logstore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockedLogStore is a mocked object that implements LogStore.
type MockedLogStore struct {
	mock.Mock
}

func (m *MockedLogStore) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedLogStore) Disconnect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockedLogStore) Ready() (bool, error) {
	args := m.Called()
	return args.Bool(0), args.Error(1)
}

func (m *MockedLogStore) Write(entries []logstore.LogEntry) error {
	args := m.Called(entries)
	return args.Error(0)
}

func (m *MockedLogStore) Query(query *logstore.Query) (*logstore.QueryResult, error) {
	args := m.Called(query)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*logstore.QueryResult), args.Error(1)
}

// newTestServer creates a HTTPServer associated with a given LogStore.
// The HTTPServer is intended to be used with a httptest Server
func newTestServer(logStore logstore.LogStore) *HTTPServer {
	// note: address doesn't matter since we will use httptest server
	server := NewHTTP(&Config{BindAddress: "127.0.0.1:8080"}, logStore)
	return server
}

func readBody(t *testing.T, resp *http.Response) string {
	bytes, err := ioutil.ReadAll(resp.Body)
	require.NoErrorf(t, err, "failed to read response body")
	return string(bytes)
}

func MustParse(isoTime string) time.Time {
	t, _ := time.Parse(time.RFC3339, isoTime)
	return t
}

// return a LogEntry missing Kubernetes metadata
func invalidLogEntry() logstore.LogEntry {
	timestamp := time.Now()
	return logstore.LogEntry{
		Date:   float64(timestamp.UnixNano() / 1.0e9),
		Log:    "event 1",
		Stream: "stdout",
		Time:   timestamp,
	}
}

func logEntry(timestamp time.Time, message string) logstore.LogEntry {
	return logstore.LogEntry{
		Date: float64(timestamp.UnixNano() / 1.0e9),
		Kubernetes: logstore.KubernetesMetadata{
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

// GET /write should call LogStore.Ready() as a health probe and should return
// 200 on success.
func TestGetWrite(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	//
	// set up mock expectations
	//

	// LogStore.Ready() should succeed.
	mockLogStore.On("Ready").Return(true, nil)

	//
	// make call
	//
	resp, _ := client.Get(testServer.URL + "/write")
	// should return 200
	assert.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, []string{"application/json"}, resp.Header["Content-Type"], "unexpected Content-Type")
	assert.Equalf(t, `{"healthy":true,"detail":""}`, readBody(t, resp), "unexpected json response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// GET /write should call LogStore.Ready() as a health probe and should return
// 503 (Service Unavailable) on probe failure.
func TestGetWriteOnError(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	//
	// set up mock expectations
	//

	// LogStore.Ready() should fail
	mockLogStore.On("Ready").Return(false, fmt.Errorf("connection refused"))

	//
	// make call
	//
	resp, _ := client.Get(testServer.URL + "/write")
	// should return 503 (Service Unavailable)
	assert.Equalf(t, http.StatusServiceUnavailable, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, []string{"application/json"}, resp.Header["Content-Type"], "unexpected Content-Type")
	assert.Equalf(t, `{"healthy":false,"detail":"connection refused"}`, readBody(t, resp), "unexpected json response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// POST /write should call through to LogStore.Write()
func TestPostWrite(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	logsToWrite := []logstore.LogEntry{
		logEntry(MustParse("2018-01-01T12:00:00.000Z"), "event 1"),
		logEntry(MustParse("2018-01-01T12:01:00.000Z"), "event 2"),
		logEntry(MustParse("2018-01-01T12:03:00.000Z"), "event 3"),
	}

	//
	// set up mock expectations
	//

	mockLogStore.On("Ready").Return(true, nil)
	mockLogStore.On("Write", logsToWrite).Return(nil)

	//
	// make call
	//
	jsonBytes, _ := json.Marshal(logsToWrite)
	body := strings.NewReader(string(jsonBytes))
	resp, _ := client.Post(testServer.URL+"/write", "application/json", body)
	// should return 503 (Service Unavailable)
	assert.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, ``, readBody(t, resp), "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// POST /write should respond with 503 (Service Unavailable) if not
// LogStore.Ready()
func TestPostWriteWhenLogStoreNotReady(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	logsToWrite := []logstore.LogEntry{
		logEntry(MustParse("2018-01-01T12:00:00.000Z"), "event 1"),
	}

	//
	// set up mock expectations
	//

	mockLogStore.On("Ready").Return(false, fmt.Errorf("connection refused"))

	//
	// make call
	//
	jsonBytes, _ := json.Marshal(logsToWrite)
	body := strings.NewReader(string(jsonBytes))
	resp, _ := client.Post(testServer.URL+"/write", "application/json", body)
	// should return 503 (Service Unavailable)
	assert.Equalf(t, http.StatusServiceUnavailable, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, `{"message":"data store is not ready","detail":"connection refused"}`, readBody(t, resp), "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

//  POST /write should respond with 500 on LogStore.Write() error
func TestPostWriteWhenLogStoreWriteFails(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	logsToWrite := []logstore.LogEntry{
		logEntry(MustParse("2018-01-01T12:00:00.000Z"), "event 1"),
	}

	//
	// set up mock expectations
	//

	mockLogStore.On("Ready").Return(true, nil)
	mockLogStore.On("Write", logsToWrite).Return(fmt.Errorf("internal error"))

	//
	// make call
	//
	jsonBytes, _ := json.Marshal(logsToWrite)
	body := strings.NewReader(string(jsonBytes))
	resp, _ := client.Post(testServer.URL+"/write", "application/json", body)
	// should return 500 (Internal Server Error)
	assert.Equalf(t, http.StatusInternalServerError, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, `{"message":"failed to store entries","detail":"internal error"}`, readBody(t, resp), "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// POST /write should respond with 400 (Bad Request) on non-json request
func TestPostWriteOnNonJSONRequest(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	//
	// set up mock expectations
	//

	//
	// make call
	//
	nonJSONBody := "illegal POST body"
	body := strings.NewReader(nonJSONBody)
	resp, _ := client.Post(testServer.URL+"/write", "application/json", body)
	// should return 400 (Internal Server Error)
	assert.Equalf(t, http.StatusBadRequest, resp.StatusCode, "unexpected response code")
	assert.Contains(t, readBody(t, resp), `"message":"failed to parse request"`, "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// POST /write should respond with 400 (Bad Request) when log entries fail
// validation
func TestPostWriteOnInvalidLogEntry(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	logsToWrite := []logstore.LogEntry{invalidLogEntry()}

	//
	// set up mock expectations
	//

	//
	// make call
	//
	jsonBytes, _ := json.Marshal(logsToWrite)
	body := strings.NewReader(string(jsonBytes))
	resp, _ := client.Post(testServer.URL+"/write", "application/json", body)
	// should return 400 (Bad Request)
	assert.Equalf(t, http.StatusBadRequest, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, `{"message":"invalid log entry","detail":"log entry missing namespace field"}`, readBody(t, resp), "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// GET /query should call through to LogStore.Query()
func TestGetQuery(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	// query to ask
	startTime := MustParse("2018-01-01T12:00:00.000Z")
	endTime := MustParse("2018-01-01T13:00:00.000Z")
	query := logstore.Query{
		Namespace:     "default",
		PodName:       "nginx-deployment-abcde",
		ContainerName: "nginx",
		StartTime:     startTime,
		EndTime:       endTime,
	}

	//
	// set up mock expectations
	//

	// will respond with this query result
	logStoreResult := logstore.QueryResult{
		LogRows: []logstore.LogRow{
			{
				Time: startTime,
				Log:  "event 1",
			},
		},
	}

	mockLogStore.On("Ready").Return(true, nil)
	mockLogStore.On("Query", &query).Return(&logStoreResult, nil)

	//
	// make call
	//
	queryURL, _ := url.Parse(testServer.URL + "/query")
	queryParams := queryURL.Query()
	queryParams.Set("namespace", query.Namespace)
	queryParams.Set("pod_name", query.PodName)
	queryParams.Set("container_name", query.ContainerName)
	queryParams.Set("start_time", "2018-01-01T12:00:00.000Z")
	queryParams.Set("end_time", "2018-01-01T13:00:00.000Z")
	queryURL.RawQuery = queryParams.Encode()

	resp, _ := client.Get(queryURL.String())
	// should return 200
	assert.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, []string{"application/json"}, resp.Header["Content-Type"], "unexpected Content-Type")
	var clientResult logstore.QueryResult
	json.Unmarshal([]byte(readBody(t, resp)), &clientResult)
	assert.Equalf(t, logStoreResult, clientResult, "unexpected query response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// addQueryParams adds a given map of parameters to a Values object.
func addQueryParams(values *url.Values, parameters map[string]string) {
	for key, value := range parameters {
		values.Set(key, value)
	}
}

// GET /query on missing query parameters should respond with 400 (Bad Request)
func TestGetQueryOnMissingQueryParams(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	//
	// set up mock expectations
	//

	//
	// make calls
	//
	tests := []struct {
		query                 map[string]string
		expectedValidationErr string
	}{
		// missing namespace
		{
			query: map[string]string{
				"pod_name":       "nginx-deployment-abcde",
				"container_name": "nginx",
				"start_time":     "2018-01-01T12:00:00.000Z",
				"end_time":       "2018-01-01T14:00:00.000Z",
			},
			expectedValidationErr: "missing query parameter: namespace",
		},
		// missing pod_name
		{
			query: map[string]string{
				"namespace":      "default",
				"container_name": "nginx",
				"start_time":     "2018-01-01T12:00:00.000Z",
				"end_time":       "2018-01-01T14:00:00.000Z",
			},
			expectedValidationErr: "missing query parameter: pod_name",
		},
		// missing container_name
		{
			query: map[string]string{
				"namespace":  "default",
				"pod_name":   "nginx-deployment-abcde",
				"start_time": "2018-01-01T12:00:00.000Z",
				"end_time":   "2018-01-01T14:00:00.000Z",
			},
			expectedValidationErr: "missing query parameter: container_name",
		},
		// missing start_time
		{
			query: map[string]string{
				"namespace":      "default",
				"pod_name":       "nginx-deployment-abcde",
				"container_name": "nginx",
			},
			expectedValidationErr: "missing query parameter: start_time",
		},
		// invalid start_time
		{
			query: map[string]string{
				"namespace":      "default",
				"pod_name":       "nginx-deployment-abcde",
				"container_name": "nginx",
				"start_time":     "2018/01/01T12:00:00.000Z",
				"end_time":       "2018-01-01T14:00:00.000Z",
			},
			expectedValidationErr: "failed to parse start_time",
		},
		// invalid end_time
		{
			query: map[string]string{
				"namespace":      "default",
				"pod_name":       "nginx-deployment-abcde",
				"container_name": "nginx",
				"start_time":     "2018-01-01T12:00:00.000Z",
				"end_time":       "2018/01/01T14:00:00.000Z",
			},
			expectedValidationErr: "failed to parse end_time",
		},
		// start_time after end_time
		{
			query: map[string]string{
				"namespace":      "default",
				"pod_name":       "nginx-deployment-abcde",
				"container_name": "nginx",
				"start_time":     "2018-01-01T12:00:00.000Z",
				"end_time":       "2018-01-01T10:00:00.000Z",
			},
			expectedValidationErr: "query time-interval: start_time must be earlier than end_time",
		},
	}

	for _, test := range tests {
		queryURL, _ := url.Parse(testServer.URL + "/query")
		queryParams := queryURL.Query()
		addQueryParams(&queryParams, test.query)
		queryURL.RawQuery = queryParams.Encode()
		resp, _ := client.Get(queryURL.String())
		// should return 400 (Bad Request)
		assert.Equalf(t, http.StatusBadRequest, resp.StatusCode, "unexpected response code")
		expectedErr := fmt.Sprintf(`{"message":"invalid query","detail":"%s"}`, test.expectedValidationErr)
		assert.Equalf(t, expectedErr, readBody(t, resp), "unexpected response")

	}

}

// GET /query when log store is not ready should respond with 503 (Service Unavailable)
func TestGetQueryWhenLogStoreNotReady(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	//
	// set up mock expectations
	//

	mockLogStore.On("Ready").Return(false, fmt.Errorf("connection refused"))

	//
	// make call
	//
	queryURL, _ := url.Parse(testServer.URL + "/query")
	queryParams := queryURL.Query()
	queryParams.Set("namespace", "default")
	queryParams.Set("pod_name", "nginx-deploymeny-abcde")
	queryParams.Set("container_name", "nginx")
	queryParams.Set("start_time", "2018-01-01T12:00:00.000Z")
	queryParams.Set("end_time", "2018-01-01T13:00:00.000Z")
	queryURL.RawQuery = queryParams.Encode()

	resp, _ := client.Get(queryURL.String())
	// should return 503 (Service Unavailable)
	assert.Equalf(t, http.StatusServiceUnavailable, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, `{"message":"data store is not ready","detail":"connection refused"}`, readBody(t, resp), "unexpected response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// GET /query on LogStore.Query() error should respond with 500 (Internal Error)
func TestGetQueryOnLogStoreError(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	// query to ask
	startTime := MustParse("2018-01-01T12:00:00.000Z")
	endTime := MustParse("2018-01-01T13:00:00.000Z")
	query := logstore.Query{
		Namespace:     "default",
		PodName:       "nginx-deployment-abcde",
		ContainerName: "nginx",
		StartTime:     startTime,
		EndTime:       endTime,
	}

	//
	// set up mock expectations
	//

	mockLogStore.On("Ready").Return(true, nil)
	// will respond with error
	mockLogStore.On("Query", &query).Return(nil, fmt.Errorf("connection refused"))

	//
	// make call
	//
	queryURL, _ := url.Parse(testServer.URL + "/query")
	queryParams := queryURL.Query()
	queryParams.Set("namespace", query.Namespace)
	queryParams.Set("pod_name", query.PodName)
	queryParams.Set("container_name", query.ContainerName)
	queryParams.Set("start_time", "2018-01-01T12:00:00.000Z")
	queryParams.Set("end_time", "2018-01-01T13:00:00.000Z")
	queryURL.RawQuery = queryParams.Encode()

	resp, _ := client.Get(queryURL.String())
	// should return 500 (Internal Server Error)
	assert.Equalf(t, http.StatusInternalServerError, resp.StatusCode, "unexpected response code")
	assert.Equalf(t, []string{"application/json"}, resp.Header["Content-Type"], "unexpected Content-Type")
	assert.Equalf(t, `{"message":"query execution error","detail":"connection refused"}`, readBody(t, resp), "unexpected json response")

	// verify that expected calls were made
	mockLogStore.AssertExpectations(t)
}

// When run with EnableProfiling=true, it should be possible to get profiling
// (e.g. via go tool pprof <binary> localhost:8080/debug/pprof/*)
func TestWithProfilingEnabled(t *testing.T) {
	mockLogStore := new(MockedLogStore)
	server := NewHTTP(&Config{BindAddress: "127.0.0.1:8080", EnableProfiling: true}, mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	resp, _ := client.Get(testServer.URL + "/debug/pprof/heap")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected status code")
}

// GET /metrics should return Prometheus-compatible metrics about the server.
func TestGetMetrics(t *testing.T) {
	// set up test server and mocked LogStore
	mockLogStore := new(MockedLogStore)
	server := newTestServer(mockLogStore)
	testServer := httptest.NewServer(server.server.Handler)
	defer testServer.Close()
	client := testServer.Client()

	// Make a first call to a resource to make metrics middleware record some
	// stats. Prior to that, no stats will have been saved.
	resp, _ := client.Get(testServer.URL + "/metrics")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected status code")
	require.Equalf(t, "", readBody(t, resp), "expected first /metrics call to be empty")

	resp, _ = client.Get(testServer.URL + "/metrics")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "unexpected status code")
	require.Containsf(t, readBody(t, resp), "total_requests{method=GET,path=/metrics,statusCode=200} 1", "missing expected metric")
}
