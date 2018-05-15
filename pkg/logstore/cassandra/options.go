package cassandra

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// ReplicationStrategy represents a replication strategy, which is
// used when a new Cassandra keyspace needs to be created.
type ReplicationStrategy string

// Valid replication strategies
const (
	SimpleStrategy          ReplicationStrategy = "SimpleStrategy"
	NetworkTopologyStrategy ReplicationStrategy = "NetworkTopologyStrategy"
)

func (r ReplicationStrategy) String() string {
	return fmt.Sprintf("%s", string(r))
}

// Validate ensures that the given ReplicationStrategy is recognized.
func (r ReplicationStrategy) Validate() error {
	switch r {
	case SimpleStrategy, NetworkTopologyStrategy:
		return nil
	default:
		return fmt.Errorf("invalid replication strategy: must be one of %s",
			[]ReplicationStrategy{SimpleStrategy, NetworkTopologyStrategy})
	}
}

// ReplicationFactorMap represents the replication factors to use for
// each cluster datacenter for a keyspace with NetworkTopologyStrategy
// replication strategy.
type ReplicationFactorMap map[string]int

// NewReplicationFactorMap parses a ReplicationFactorMap from a JSON string.
// An example replication factor map is
//    {"dc1":3,"dc2":2}
//
func NewReplicationFactorMap(asJSON string) (ReplicationFactorMap, error) {
	m := make(map[string]int)
	err := json.Unmarshal([]byte(asJSON), &m)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replication factor map: %s", err)
	}
	return m, nil
}

func (r ReplicationFactorMap) ToJSON() string {
	bytes, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Errorf("failed to convert ReplicationFactorMap to JSON: %s", err))
	}
	return string(bytes)
}

// String returns the ReplicationFactorMap as a string of form
//     'datacenter1': 2, 'datacenter2': 3, 'datacenter3': 4
func (r ReplicationFactorMap) String() string {
	// sort keys for deterministic output order
	keys := make([]string, 0)
	for key := range r {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	buf := &bytes.Buffer{}
	for i, key := range keys {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("'" + key + "': " + strconv.Itoa(r[key]))
	}
	return buf.String()
}

// JSON returns the ReplicationFactorMap as a JSON encoded string.
func (r ReplicationFactorMap) JSON() string {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// OptionError is returned when an invalid set of Cassandra Options are supplied.
type OptionError struct {
	Message string
}

func (e *OptionError) Error() string {
	return fmt.Sprintf("invalid cassandra options: %s", e.Message)
}

// Options describes Cassandra driver options.
type Options struct {
	// Hosts are one or more cluster nodes to connect to.
	Hosts   []string
	CQLPort int
	// Keyspace is the keyspace that contains the log table. This
	// keyspace will be created if it does not exist.
	Keyspace string
	// LogTableName is the name to use for the log table. This
	// keyspace will be created if it does not exist.
	LogTableName string
	// ReplicationStrategy is the replication strategy to use
	// if the keyspace does not exist and needs to be created.
	ReplicationStrategy ReplicationStrategy
	// ReplicationFactors is the map of replication factors (one per datacenter)
	// to use when a NetworkTopologyStrategy is specified for ReplicationStrategy.
	// When a SimpleStrategy is used, this map must only contain one entry
	// with key `cluster`.
	ReplicationFactors ReplicationFactorMap

	// WriteConcurrency specifies the number of goroutines to use to process
	// Cassandra insert statements (to increase write throughput).
	WriteConcurrency int
	// WriteBufferSize controls the maxiumum number of inserts that can be
	// queued up before additional writes will block.
	WriteBufferSize int
}

// Validate ensures that the given Options are valid.
func (opts *Options) Validate() error {
	if len(opts.Hosts) < 1 {
		return &OptionError{"at least one cassandra host must be given"}
	}
	if opts.CQLPort <= 0 || opts.CQLPort > 65535 {
		return &OptionError{"CQL port must be in range [1,65535]"}
	}
	if opts.Keyspace == "" {
		return &OptionError{"no keyspace given"}
	}
	if opts.LogTableName == "" {
		return &OptionError{"no log table name given"}
	}
	if err := opts.ReplicationStrategy.Validate(); err != nil {
		return &OptionError{err.Error()}
	}
	switch opts.ReplicationStrategy {
	case SimpleStrategy:
		if len(opts.ReplicationFactors) != 1 {
			return &OptionError{"for SimpleStrategy, one single replication factor must be given"}
		}
		if _, ok := opts.ReplicationFactors["cluster"]; !ok {
			return &OptionError{"for SimpleStrategy, a replication factor with key 'cluster' is required"}
		}
	case NetworkTopologyStrategy:
		if len(opts.ReplicationFactors) < 1 {
			return &OptionError{"for NetworkTopologyStrategy, one replication factor must be given for each datacenter"}
		}
	}
	if opts.WriteConcurrency <= 0 {
		return &OptionError{"WriteConcurrency must be a positive value"}
	}
	if opts.WriteBufferSize <= 0 {
		return &OptionError{"WriteBufferSize must be a positive value"}
	}

	return nil
}

func (opts *Options) String() string {
	return fmt.Sprintf("%+v", *opts)
}
