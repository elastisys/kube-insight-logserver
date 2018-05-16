package cassandra

import (
	"fmt"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
	"github.com/elastisys/kube-insight-logserver/pkg/logstore"
)

// InsertError is returned on problems to insert log records.
type InsertError struct {
	cause error
}

func (e InsertError) Error() string {
	return fmt.Sprintf("insert failed: %s", e.cause.Error())
}

// QueryError is returned on problems to query Cassandra for log records.
type QueryError struct {
	message string
	cause   error
}

func (e QueryError) Error() string {
	return fmt.Sprintf("query failed: %s: %s", e.message, e.cause.Error())
}

// SchemaError indicates a problem to create the Cassandra schema.
type SchemaError struct {
	message string
	cause   error
}

func (e SchemaError) Error() string {
	return fmt.Sprintf("schema creation failed: %s: %s", e.message, e.cause.Error())
}

// LogStore is a Cassandra implementation of the LogStore API.
type LogStore struct {
	driver     Driver
	options    *Options
	writerPool *writerPool
}

// NewLogStore creates a new Cassandra LogStore using the specified Driver and
// Options.
func NewLogStore(driver Driver, options *Options) *LogStore {
	return &LogStore{
		driver:     driver,
		options:    options,
		writerPool: newWriterPool(driver, options.WriteConcurrency, options.WriteBufferSize),
	}
}

// Connect connects the LogStore to the Cassandra cluster.
func (c *LogStore) Connect() error {
	log.Infof("connecting to cassandra ...")
	err := c.driver.Connect()
	if err != nil {
		return err
	}

	return c.createSchemaIfNotExists()
}

// Disconnect disconnects the LogStore from the Cassandra cluster.
func (c *LogStore) Disconnect() error {
	c.writerPool.stop()
	log.Infof("disconnecting from cassandra ...")
	return c.driver.Close()
}

// Ready returns true if the Cassandra cluster appears reachable.
func (c *LogStore) Ready() (bool, error) {
	return c.driver.Reachable()
}

func (c *LogStore) Write(entries []logstore.LogEntry) error {

	// add log entry inserts to writer pool queue (executed asynchronously)
	resultChannels := make([]writeResultChan, len(entries))
	for i, logEntry := range entries {
		resultChannels[i] = c.insert(&logEntry)
	}

	// await completion of all inserts
	for _, resultChannel := range resultChannels {
		err := <-resultChannel
		if err != nil {
			return InsertError{err}
		}
	}

	return nil
}

// Query performs a query for historical log records against Cassandra.
func (c *LogStore) Query(query *logstore.Query) (*logstore.QueryResult, error) {
	// break into sub-queries if query interval spans date border(s)
	splitter := &querySplitter{query}
	subQueries := splitter.Split()

	logRows := make([]logstore.LogRow, 0)
	for i, subQuery := range subQueries {
		if log.Level() >= log.TraceLevel {
			log.Tracef("running subquery %d out of %d: %s", (i + 1), len(subQueries), subQuery)
		}
		rows, err := c.executeQuery(subQuery)
		if err != nil {
			return nil, QueryError{"query execution", err}
		}
		logRows = append(logRows, rows...)
	}

	return &logstore.QueryResult{LogRows: logRows}, nil
}

func (c *LogStore) executeQuery(query *logstore.Query) ([]logstore.LogRow, error) {
	date := query.StartTime.Format("2006-01-02")
	results, err := c.driver.Query(c.logQueryStatement(),
		query.Namespace, query.PodName, query.ContainerName, date, query.StartTime, query.EndTime)
	if err != nil {
		return nil, err
	}

	logRows := make([]logstore.LogRow, 0)
	for _, logRow := range results {
		var time = logRow["time"].(time.Time)
		var log = logRow["message"].(string)
		logRows = append(logRows, logstore.LogRow{Time: time, Log: log})
	}

	return logRows, nil
}

func (c *LogStore) createSchemaIfNotExists() error {
	if err := c.createKeyspaceIfNotExists(); err != nil {
		return SchemaError{message: "failed to create keyspace", cause: err}
	}

	if err := c.createTableIfNotExists(); err != nil {
		return SchemaError{message: "failed to create log table", cause: err}
	}

	return nil
}

func (c *LogStore) createKeyspaceIfNotExists() error {
	return c.driver.Execute(c.keyspaceDeclaration())
}

func (c *LogStore) createTableIfNotExists() error {
	return c.driver.Execute(c.tableDeclaration())
}

func (c *LogStore) keyspaceDeclaration() string {
	replicationSpec := ""
	if c.options.ReplicationStrategy == NetworkTopologyStrategy {
		replFactors := c.options.ReplicationFactors.String()
		replicationSpec = fmt.Sprintf("{ 'class': 'NetworkTopologyStrategy', %s }", replFactors)
	} else {
		replicationSpec = fmt.Sprintf("{ 'class': 'SimpleStrategy', 'replication_factor': %d }",
			c.options.ReplicationFactors["cluster"])
	}

	return fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s",
		c.options.Keyspace, replicationSpec)
}

func (c *LogStore) tableDeclaration() string {
	const LogTableTemplate string = `CREATE TABLE IF NOT EXISTS %s.%s (
	namespace text,
	pod_name text,
	container_name text,
	date date,
	time timestamp,
	message text,
	stream text,
	pod_id text,
	docker_id text,
	host text,	
	labels map<text,text>,
	PRIMARY KEY ((namespace, pod_name, container_name, date), time) )
WITH CLUSTERING ORDER BY (time DESC)`

	return fmt.Sprintf(LogTableTemplate, c.options.Keyspace, c.options.LogTableName)
}

func (c *LogStore) logQueryStatement() string {
	return "SELECT time, message " +
		"FROM " + c.options.Keyspace + "." + c.options.LogTableName + " WHERE" +
		"(namespace=?) AND " +
		"(pod_name=?) AND " +
		"(container_name=?) AND " +
		"(date=?) AND " +
		"(time >= ?) AND " +
		"(time <= ?) " +
		"ORDER BY time ASC"
}

func (c *LogStore) insertStatement() string {
	return "INSERT INTO " + c.options.Keyspace + "." + c.options.LogTableName + " " +
		"(namespace, pod_name, container_name, date, time, message, stream, pod_id, docker_id, host, labels) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

func (c *LogStore) insert(logEntry *logstore.LogEntry) writeResultChan {
	podMeta := logEntry.Kubernetes
	date := logEntry.Time.Format("2006-01-02")

	return c.writerPool.write(c.insertStatement(),
		podMeta.Namespace, podMeta.PodName, podMeta.ContainerName, date, logEntry.Time,
		logEntry.Log, logEntry.Stream, podMeta.PodID, podMeta.DockerID, podMeta.Host, podMeta.Labels)
}
