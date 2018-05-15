package logstore

import (
	"fmt"
	"time"
)

// LogEntry represents a single log row captured from a container in a
// Kubernetes pod that is to be ingested into a LogStore.
// Its JSON structure follows that of the fluentbit Kubernetes metadata
// filter (https://fluentbit.io/documentation/current/installation/kubernetes.html)
// and may look something like:
//
//    {
//       "date": 1525349097.094408,
//       "kubernetes": {
//         "docker_id": "e8b89cc4e292827b2f521c4c7d7b8807cf72023565b9ac5f89f8186420325d74",
//         "labels": {
//           "pod-template-generation": "1",
//           "name": "weave-net",
//           "controller-revision-hash": "2689456918"
//         },
//         "host": "master1",
//         "pod_name": "weave-net-5mfwh",
//         "container_name": "weave",
//         "pod_id": "f5225d5f-4e9d-11e8-8b6b-02425d6e035a",
//         "namespace_name": "kube-system"
//       },
//       "log": "INFO: 2018/05/03 12:04:57.094154 Discovered remote MAC 36:96:7c:78:d0:22 at 4a:26:23:65:5b:88(worker1)",
//       "stream": "stderr",
//       "time": "2018-05-03T12:04:57.094408152Z"
//    }
type LogEntry struct {
	Date       float64            `json:"date"`
	Kubernetes KubernetesMetadata `json:"kubernetes"`
	Log        string             `json:"log"`
	Stream     string             `json:"stream"`
	Time       time.Time          `json:"time"`
}

// KubernetesMetadata carries metadata about a LogEntry.
type KubernetesMetadata struct {
	DockerID      string            `json:"docker_id"`
	Labels        map[string]string `json:"labels"`
	Host          string            `json:"host"`
	PodName       string            `json:"pod_name"`
	ContainerName string            `json:"container_name"`
	PodID         string            `json:"pod_id"`
	Namespace     string            `json:"namespace_name"`
}

// Validate ensures that a LogEntry contains all fields necessary for creating a
// row in Cassandra.
func (l *LogEntry) Validate() error {
	if l.Kubernetes.Namespace == "" {
		return fmt.Errorf("log entry missing namespace field")
	}
	if l.Kubernetes.PodName == "" {
		return fmt.Errorf("log entry missing pod_name field")
	}
	if l.Kubernetes.ContainerName == "" {
		return fmt.Errorf("log entry missing container_name field")
	}
	if l.Time.IsZero() {
		return fmt.Errorf("log entry missing time field")
	}

	return nil
}

// QueryResult contains a list of LogRows that matched a given query.
type QueryResult struct {
	LogRows []LogRow `json:"log_rows"`
}

// LogRow represents a single log entry in a QueryResult.
type LogRow struct {
	Time time.Time `json:"time"`
	Log  string    `json:"log"`
}

func (l *LogRow) String() string {
	return fmt.Sprintf("%s: %s", l.Time, l.Log)
}

// APIError represents an error that can be returned by the REST API.
type APIError struct {
	// Message is a human-readable message intended for presentation.
	Message string `json:"message"`
	// Detail holds error details (could be a stack-trace or more
	// specific error information)
	Detail string `json:"detail"`
}

// APIStatus represents a JSON status message on `GET /write`
type APIStatus struct {
	Healthy bool `json:"healthy"`
	// Detail contains an error message in case the status is unhealthy.
	Detail string `json:"detail"`
}

// LogStore is capable of writing Kubernetes pod log entries to a backing
// datastore and querying historical log entries.
type LogStore interface {
	LogWriter
	LogQueryer
	// Connect runs the code necessary (if any) to set up a connection
	// to the backing data store.
	Connect() error
	// Disconnect runs the code necessary (if any) to disconnect (in a
	// graceful manner) from the backing data store.
	Disconnect() error
	// Ready is used as a health check to see if the LogStore is ready
	// to accept writes/queries. If the LogStore is healthy, true is
	// returned. If the LogStore is unhealthy, false is returned
	// together with an error describing why the LogStore is unhealthy.
	Ready() (bool, error)
}

// LogWriter writes Kubernetes pod log entries to a backing datastore.
type LogWriter interface {
	// Write writes a collection of log entries to a backing store.
	Write(entries []LogEntry) error
}

// QueryError is used as an error return on invalid Query instances.
type QueryError string

func (e QueryError) Error() string {
	return string(e)
}

// Query represents a query for historical Kubernetes pod log entries.
type Query struct {
	Namespace     string    `json:"namespace"`
	PodName       string    `json:"pod_name"`
	ContainerName string    `json:"container_name"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
}

// Validate checks the validity of a Query.
func (q *Query) Validate() error {
	if q.Namespace == "" {
		return QueryError("missing query parameter: namespace")
	}
	if q.PodName == "" {
		return QueryError("missing query parameter: pod_name")
	}
	if q.ContainerName == "" {
		return QueryError("missing query parameter: container_name")
	}
	if q.StartTime.IsZero() {
		return QueryError("missing query parameter: start_time")
	}
	if q.EndTime.IsZero() {
		return QueryError("missing query parameter: end_time")
	}

	if !q.StartTime.Before(q.EndTime) {
		return QueryError("query time-interval: start_time must be earlier than end_time")
	}
	return nil
}

func (q *Query) String() string {
	return fmt.Sprintf(`{"Namespace": "%s", "PodName": "%s", "Container": "%s", "StartTime": "%s", "EndTime": "%s"}`,
		q.Namespace, q.PodName, q.ContainerName, q.StartTime.Format(time.RFC3339Nano), q.EndTime.Format(time.RFC3339Nano))
}

// LogQueryer queries a backing datastore for historical Kubernetes pod log entries.
type LogQueryer interface {
	// Query runs a for historical log entries.
	Query(query *Query) (*QueryResult, error)
}
