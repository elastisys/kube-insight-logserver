package cassandra

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Verify the behavior of Options.Validate()
func TestOptionValidation(t *testing.T) {

	tests := []struct {
		options                 Options
		isValid                 bool
		expectedValidationError string
	}{
		{
			// no hosts
			options: Options{
				Hosts:               []string{},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: at least one cassandra host must be given",
		},
		{
			// illegal CQL port
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             0,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: CQL port must be in range [1,65535]",
		},
		{
			// missing keyspace
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: no keyspace given",
		},
		{
			// missing log table name
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: no log table name given",
		},
		{
			// unknown replication strategy
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: "UnknownStrategy",
				ReplicationFactors: map[string]int{
					"cluster": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: invalid replication strategy: must be one of [SimpleStrategy NetworkTopologyStrategy]",
		},
		{
			// missing a 'cluster' replication factor for SimpleStrategy
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"dc1": 1,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: for SimpleStrategy, a replication factor with key 'cluster' is required",
		},
		{
			// too many replication factors for SimpleStrategy
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 3,
					"dc2":     3,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: for SimpleStrategy, one single replication factor must be given",
		},
		{
			// too few replication factors for NetworkTopologyStrategy
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: NetworkTopologyStrategy,
				ReplicationFactors:  map[string]int{},
				WriteConcurrency:    1,
				WriteBufferSize:     1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: for NetworkTopologyStrategy, one replication factor must be given for each datacenter",
		},
		{
			// non-positive write concurrency
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: NetworkTopologyStrategy,
				ReplicationFactors: map[string]int{
					"dc1": 3,
					"dc2": 3,
				},
				WriteConcurrency: 0,
				WriteBufferSize:  1024,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: WriteConcurrency must be a positive value",
		},
		{
			// invalid WriteBufferSize
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: NetworkTopologyStrategy,
				ReplicationFactors: map[string]int{
					"dc1": 3,
					"dc2": 3,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  0,
			},
			isValid:                 false,
			expectedValidationError: "invalid cassandra options: WriteBufferSize must be a positive value",
		},

		{
			// valid
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: NetworkTopologyStrategy,
				ReplicationFactors: map[string]int{
					"dc1": 3,
					"dc2": 3,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 true,
			expectedValidationError: "",
		},
		{
			// valid
			options: Options{
				Hosts:               []string{"localhost"},
				CQLPort:             9042,
				Keyspace:            "ks",
				LogTableName:        "log",
				ReplicationStrategy: SimpleStrategy,
				ReplicationFactors: map[string]int{
					"cluster": 3,
				},
				WriteConcurrency: 1,
				WriteBufferSize:  1024,
			},
			isValid:                 true,
			expectedValidationError: "",
		},
	}

	for _, test := range tests {
		opts := &test.options
		err := opts.Validate()
		if test.isValid {
			assert.Nilf(t, err, "Options expected to be valid: got error: %s", err)
		} else {

			assert.NotNilf(t, err, "Options expected to be invalid, but passed validation")

			assert.Equalf(t, test.expectedValidationError, err.Error(),
				"unexpected validation error: expected: %s, was: %s",
				test.expectedValidationError, err.Error())
		}
	}
}

// Tests the NewReplicationFactorMap function.
func TestParseReplicationFactorsFromJson(t *testing.T) {
	tests := []struct {
		json        string
		expectedMap ReplicationFactorMap
	}{
		{
			json:        `{}`,
			expectedMap: ReplicationFactorMap(map[string]int{}),
		},
		{
			json:        `{"cluster": 3}`,
			expectedMap: ReplicationFactorMap(map[string]int{"cluster": 3}),
		},
		{
			json:        `{"datacenter1": 3, "datacenter2": 2}`,
			expectedMap: ReplicationFactorMap(map[string]int{"datacenter1": 3, "datacenter2": 2}),
		},
	}

	for _, test := range tests {
		parsedMap, _ := NewReplicationFactorMap(test.json)
		if !reflect.DeepEqual(parsedMap, test.expectedMap) {
			t.Errorf("unexpected result of parsing reflection map JSON: expected: %s, was: %s", test.expectedMap, parsedMap)
		}
	}
}

// Tests the NewReplicationFactorMap function when an illegal value is given.
func TestReplicationFactorsFromJsonOnError(t *testing.T) {
	_, err := NewReplicationFactorMap(`{"cluster": "three"}`)
	expectedErr := "failed to parse replication factor map: json: " +
		"cannot unmarshal string into Go value of type int"
	if (err == nil) || (err.Error() != expectedErr) {
		t.Errorf("unexpected error: expected: %s, was: %s", expectedErr, err)
	}
}
