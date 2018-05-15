package cassandra

import (
	"fmt"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
	"github.com/gocql/gocql"
)

// CQLRows represents a slice of CQL query result rows, each in the form of a
// map of column key-value pairs.
type CQLRows []map[string]interface{}

// Driver is a simplified Cassandra driver interface intended to be used
// by the Cassandra LogStore.
type Driver interface {
	// Connect connects the driver to the Cassandra node(s) it has been
	// configured to use (in an implementation-specific manner).
	Connect() error

	// Close disconnects the driver from its Cassandra node(s) and releases any
	// allocated system resources. The Driver should NOT be used after this
	// method has been called.
	Close() error

	// Reachable returns true if the Cassandra cluster can be connect to. If
	// not, false is returned together with an error message.
	Reachable() (bool, error)

	// Execute runs a data modification (CREATE/INSERT) statement against
	// cassandra. Note: if Connect() hasn't been successfully called, this call
	// will fail.
	Execute(statement string, placeholders ...interface{}) error

	// Query runs a SELECT query statement against cassandra. The caller is
	// responsible for closing the returned iterator.
	// Note: if Connect() hasn't been successfully called, this call will fail.
	Query(query string, placeholders ...interface{}) (CQLRows, error)
}

// CQLDriver is capable of connecting to Cassandra and running queries/DML
// statements.
type CQLDriver struct {
	// cluster holds the cassandra cluster settings
	cluster *gocql.ClusterConfig
	// session: will be nil before Connect() is called.
	session *gocql.Session
}

// NewCQLDriver creates a new disconnected CQLDriver. Before use, call
// Connect().
func NewCQLDriver(clusterConfig *gocql.ClusterConfig) *CQLDriver {
	return &CQLDriver{cluster: clusterConfig, session: nil}
}

// Connect connects the driver to the Cassandra node(s) it has been
// configured to use (in an implementation-specific manner).
func (d *CQLDriver) Connect() (err error) {
	// connect to the cluster
	d.session, err = d.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("unable to create cassandra session: %s", err)
	}
	log.Debugf("connected.")
	return nil
}

// Reachable returns true if the Cassandra cluster can be connect to. If
// not, false is returned together with an error message.
func (d *CQLDriver) Reachable() (bool, error) {
	session, err := d.cluster.CreateSession()
	if err != nil {
		return false, fmt.Errorf("failed to connect to cluster: %s", err)
	}
	defer session.Close()
	return true, nil
}

// Close disconnects the driver from its Cassandra node(s) and releases any
// allocated system resources. The Driver should NOT be used after this
// method has been called.
func (d *CQLDriver) Close() error {
	if d.session != nil {
		d.session.Close()
	}
	return nil
}

// Execute runs a data modification (CREATE/INSERT) statement against
// cassandra. Note: if Connect() hasn't been successfully called, this call
// will fail.
func (d *CQLDriver) Execute(statement string, placeholders ...interface{}) error {
	if d.session == nil {
		return fmt.Errorf("cannot execute statement: not connected to cassandra")
	}

	if log.Level() >= log.TraceLevel {
		log.Tracef("executing statement: %s\nwith placeholders: %#v",
			statement, placeholders)
	}

	stmt := d.session.Query(statement, placeholders...)
	if err := stmt.Exec(); err != nil {
		return err
	}
	return nil
}

// Query runs a SELECT query statement against cassandra. Note: if
// Connect() hasn't been successfully called, this call will fail.
func (d *CQLDriver) Query(query string, placeholders ...interface{}) (CQLRows, error) {
	if d.session == nil {
		return nil, fmt.Errorf("cannot execute query: not connected to cassandra")
	}

	if log.Level() >= log.TraceLevel {
		log.Tracef("executing query: %s\nwith placeholders: %#v",
			query, placeholders)

	}
	iter := d.session.Query(query, placeholders...).Iter()
	rows, err := iter.SliceMap()
	if err != nil {
		return nil, fmt.Errorf("failed to get result rows: %s", err)
	}

	// close the underlying iterator. this will also return an error if any
	// problems were encountered during query execution.
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("query execution failed: %s", err)
	}

	return CQLRows(rows), nil
}
