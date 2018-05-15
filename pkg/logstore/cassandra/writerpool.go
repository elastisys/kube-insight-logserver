package cassandra

import (
	"fmt"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
)

// cqlInsert represents a single CQL cqlInsert statement with placeholders.
type cqlInsert struct {
	insertStatement string
	placeholders    []interface{}
}

// writeResultChan is a return value channel that a writer uses to
// asynchronously return the insert return value to the caller.
type writeResultChan chan error

// insertOperation is a single CQL insert statement (bundled with a return value
// channel) that is read from the work channel by writers.
type insertOperation struct {
	insert     *cqlInsert
	resultChan writeResultChan
}

// writer reads insertOperations off of a work channel (work queue), and
// executes them against Cassandra.
type writer struct {
	workChan        chan insertOperation
	stopChan        chan struct{}
	cassandraDriver Driver
}

// newWriter creates a new writer associated with a given work channel.
func newWriter(cassandraDriver Driver, workChan chan insertOperation) *writer {
	w := writer{
		workChan:        workChan,
		stopChan:        make(chan struct{}),
		cassandraDriver: cassandraDriver,
	}
	return &w
}

// start starts reading insert operations from the work channel and execute them
// against Cassandra. It continues until its stop channel is closed.
func (w *writer) start() {
	for {
		select {
		case op := <-w.workChan:
			// execute insert and send result back to caller on result channel
			op.resultChan <- w.cassandraDriver.Execute(
				op.insert.insertStatement, op.insert.placeholders...)
		case <-w.stopChan:
			// told to stop, so exit
			return
		}
	}
}

// stop will stop the writer from processing any more insert operations.
func (w *writer) stop() {
	close(w.stopChan)
}

// writerPool represents a pool of writer goroutines that accept Cassandra
// insert statements and execute them. The use of multiple writers can speed up
// large insert batches quite considerably.
type writerPool struct {
	// cassandraDriver is a Cassandra Driver assumed to be in a connected state.
	cassandraDriver Driver
	// workChan is the channel where insert statements are buffered until a
	// writer is ready to handle it.
	workChan chan insertOperation
	// writers is a collection of writer goroutines that process inserts off of
	// the workChan.
	writers []*writer
	// started is true if the writers have been started.
	started bool
}

// newWriterPool creates a new writerPool with a given number of writer
// goroutines, connected to a given cassandra cluster (via a driver). The caller
// is responsible for making sure that the Driver is in a connected state before
// calling write(). The writerPool keeps a work queue where inserts are buffered
// until a writer grabs it. The capacity of the write buffer can be controlled
// via `bufferSize`. Once the size of the insert queue grows beyond
// `bufferSize`, additional `write()` calls will block until the queue has been
// processed down to `bufferSize` again.
func newWriterPool(cassandraDriver Driver, numWriters, bufferSize int) *writerPool {
	workChannel := make(chan insertOperation, bufferSize)

	writers := make([]*writer, numWriters)
	for i := 0; i < numWriters; i++ {
		writers[i] = newWriter(cassandraDriver, workChannel)
	}

	pool := writerPool{
		cassandraDriver: cassandraDriver,
		workChan:        workChannel,
		writers:         writers,
	}

	log.Debugf("starting %d cassandra writers ...", len(pool.writers))
	for _, writer := range pool.writers {
		go writer.start()
	}
	pool.started = true

	return &pool
}

// stop stops all writer goroutines started by a prior call to start().
func (pool *writerPool) stop() {
	log.Debugf("stopping %d cassandra writers ...", len(pool.writers))
	for _, writer := range pool.writers {
		writer.stop()
	}
	pool.started = false
}

// write executes an insert statement against cassandra in an asynchronous
// manner. The method will not block but will return immediately when the
// request has been queued. The returned channel can be used by the caller to
// check for completion (and to check the result -- an error is returned if the
// write failed).
func (pool *writerPool) write(insertStatement string, placeholders ...interface{}) writeResultChan {
	resultChan := make(writeResultChan, 1)
	if !pool.started {
		resultChan <- fmt.Errorf("write rejected: writerPool has been stopped")
		return resultChan
	}

	insertRequest := insertOperation{
		insert: &cqlInsert{
			insertStatement: insertStatement,
			placeholders:    placeholders,
		},
		resultChan: resultChan,
	}
	pool.workChan <- insertRequest
	return insertRequest.resultChan
}
