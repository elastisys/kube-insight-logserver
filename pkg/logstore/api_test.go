package logstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogEntryValidation(t *testing.T) {
	now := time.Now()

	tests := []struct {
		logEntry              *LogEntry
		valid                 bool
		expectedValidationErr string
	}{
		// a valid log entry
		{
			logEntry: &LogEntry{
				Kubernetes: KubernetesMetadata{
					PodName:       "nginx-deployment-abcde",
					ContainerName: "nginx",
					Namespace:     "default",
				},
				Log:    "event 1",
				Stream: "stdout",
				Time:   now,
			},
			valid: true,
			expectedValidationErr: "",
		},
		// invalid: missing pod_name
		{
			logEntry: &LogEntry{
				Kubernetes: KubernetesMetadata{
					ContainerName: "nginx",
					Namespace:     "default",
				},
				Log:    "event 1",
				Stream: "stdout",
				Time:   now,
			},
			valid: false,
			expectedValidationErr: "log entry missing pod_name field",
		},
		// invalid: missing container_name
		{
			logEntry: &LogEntry{
				Kubernetes: KubernetesMetadata{
					PodName:   "nginx-deployment-abcde",
					Namespace: "default",
				},
				Log:    "event 1",
				Stream: "stdout",
				Time:   now,
			},
			valid: false,
			expectedValidationErr: "log entry missing container_name field",
		},
		// invalid: missing namespace
		{
			logEntry: &LogEntry{
				Kubernetes: KubernetesMetadata{
					PodName:       "nginx-deployment-abcde",
					ContainerName: "nginx",
				},
				Log:    "event 1",
				Stream: "stdout",
				Time:   now,
			},
			valid: false,
			expectedValidationErr: "log entry missing namespace field",
		},
		// invalid: missing time
		{
			logEntry: &LogEntry{
				Kubernetes: KubernetesMetadata{
					PodName:       "nginx-deployment-abcde",
					ContainerName: "nginx",
					Namespace:     "default",
				},
				Log:    "event 1",
				Stream: "stdout",
			},
			valid: false,
			expectedValidationErr: "log entry missing time field",
		},
	}

	for _, test := range tests {
		err := test.logEntry.Validate()
		if test.valid {
			assert.Nilf(t, err, "expected validation to succeed, but failed with: %s", err)
		} else {
			assert.NotNilf(t, err, "expected validation to fail")
			assert.Equalf(t, test.expectedValidationErr, err.Error(), "unexpected validation error")
		}
	}
}

func TestQueryValidation(t *testing.T) {
	tests := []struct {
		query                 *Query
		expectedValidationErr string
	}{
		{
			query: &Query{
				Namespace:     "",
				PodName:       "nginx-deployment-abcde",
				ContainerName: "nginx",
				StartTime:     time.Now(),
				EndTime:       time.Now().Add(1 * time.Minute),
			},
			expectedValidationErr: "missing query parameter: namespace",
		},
		{
			query: &Query{
				Namespace:     "default",
				PodName:       "",
				ContainerName: "nginx",
				StartTime:     time.Now(),
				EndTime:       time.Now().Add(1 * time.Minute),
			},
			expectedValidationErr: "missing query parameter: pod_name",
		},
		{
			query: &Query{
				Namespace:     "default",
				PodName:       "nginx-deployment-abcde",
				ContainerName: "",
				StartTime:     time.Now(),
				EndTime:       time.Now().Add(1 * time.Minute),
			},
			expectedValidationErr: "missing query parameter: container_name",
		},
		{
			query: &Query{
				Namespace:     "default",
				PodName:       "nginx-deployment-abcde",
				ContainerName: "nginx",
				StartTime:     time.Time{},
				EndTime:       time.Now().Add(1 * time.Minute),
			},
			expectedValidationErr: "missing query parameter: start_time",
		},
		{
			query: &Query{
				Namespace:     "default",
				PodName:       "nginx-deployment-abcde",
				ContainerName: "nginx",
				StartTime:     time.Now(),
				EndTime:       time.Time{},
			},
			expectedValidationErr: "missing query parameter: end_time",
		},
		{
			query: &Query{
				Namespace:     "default",
				PodName:       "nginx-deployment-abcde",
				ContainerName: "nginx",
				StartTime:     time.Now(),
				EndTime:       time.Now().Add(-1 * time.Second),
			},
			expectedValidationErr: "query time-interval: start_time must be earlier than end_time",
		},
	}

	for _, test := range tests {
		err := test.query.Validate()
		require.NotNilf(t, err, "expected query validation to fail for: %s", test.query)
		assert.Equalf(t, test.expectedValidationErr, err.Error(), "unexpected validation error")
	}

	// validate a valid query
	validQuery := &Query{
		Namespace:     "default",
		PodName:       "nginx-deployment-abcde",
		ContainerName: "nginx",
		StartTime:     time.Now(),
		EndTime:       time.Now().Add(1 * time.Second),
	}
	assert.Nilf(t, validQuery.Validate(), "expected query validation to succeed")
}
