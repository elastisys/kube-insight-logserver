package main

import (
	"flag"
	"fmt"
	"runtime"

	"os"
	"os/signal"
	"strconv"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
	"github.com/elastisys/kube-insight-logserver/pkg/logstore/cassandra"
	"github.com/elastisys/kube-insight-logserver/pkg/server"
	"github.com/gocql/gocql"
)

// version is the release version of the program. This is intended to be set by
// the linker at build-time. E.g. `-ldflags "-X main.version=1.0.0"`.
var version string

// command-line defaults
var (
	defaultServerIP   = "0.0.0.0"
	defaultServerPort = 8080
	// Cassandra keyspace
	cassandraDefaults = cassandra.Options{
		Hosts:               []string{"127.0.0.1"},
		CQLPort:             9042,
		Keyspace:            "insight_logs",
		ReplicationStrategy: cassandra.SimpleStrategy,
		ReplicationFactors:  cassandra.ReplicationFactorMap{"cluster": 1},
		LogTableName:        "logs",
		WriteConcurrency:    runtime.GOMAXPROCS(-1) * 4,
		WriteBufferSize:     1024,
	}
	defaultEnableProfiling = false
)

// command-line options
var (
	serverBindAddr               string
	serverPort                   int
	cassandraPort                int
	cassandraKeyspace            string
	cassandraReplicationStrategy string
	cassandraReplicationFactor   string
	cassandraWriteConcurrency    int
	cassandraWriteBufferSize     int

	enableProfiling bool

	showVersion bool
)

func envOrDefaultStr(envVar string, defaultValue string) string {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return defaultValue
	}

	return envVal
}

func envOrDefaultInt(envVar string, defaultValue int) int {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return defaultValue
	}
	intVal, err := strconv.Atoi(envVal)
	if err != nil {
		log.Fatalf("environment variable %s: not an int value: %s", envVar, envVal)
	}
	return intVal
}

func envOrDefaultBool(envVar string, defaultValue bool) bool {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		return defaultValue
	}
	boolVal, err := strconv.ParseBool(envVal)
	if err != nil {
		log.Fatalf("environment variable %s: not a bool value: %s", envVar, envVal)
	}
	return boolVal
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stdout, "usage: %s [OPTIONS] [<cassandra-node> ...]\n\n",
			os.Args[0])

		fmt.Fprintf(os.Stdout, "Connects to a (set of) Cassandra node(s) and "+
			"starts a HTTP server with a REST API through which Kubernetes "+
			"pod logs can be ingested into the Cassandra cluster and against "+
			"which queries can be posed to fetch historical log entries. If no "+
			"Cassandra nodes are given, 127.0.0.1 is assumed.\n\n")

		fmt.Fprintf(os.Stdout, "Options:\n")
		flag.PrintDefaults()
	}

	flag.StringVar(&serverBindAddr, "bind-address",
		envOrDefaultStr("IP", defaultServerIP),
		fmt.Sprintf("The IP address to bind the HTTP server to "+
			"(default value: %s, environment variable: IP)", defaultServerIP))

	flag.IntVar(&serverPort, "port",
		envOrDefaultInt("PORT", defaultServerPort),
		fmt.Sprintf("The server port to listen on (default value: %d, environment "+
			"variable: PORT)", defaultServerPort))

	flag.StringVar(&cassandraKeyspace, "cassandra-keyspace",
		envOrDefaultStr("CASSANDRA_KEYSPACE", cassandraDefaults.Keyspace),
		fmt.Sprintf("The keyspace to use/create. "+
			"(default value: %s, environment variable: CASSANDRA_KEYSPACE)",
			cassandraDefaults.Keyspace))

	flag.IntVar(&cassandraPort, "cassandra-port",
		envOrDefaultInt("CASSANDRA_PORT", cassandraDefaults.CQLPort),
		fmt.Sprintf("Cassandra cluster CQL port (default value: %d, environment"+
			" variable: CASSANDRA_PORT)", cassandraDefaults.CQLPort))

	flag.StringVar(&cassandraReplicationStrategy, "cassandra-replication-strategy",
		envOrDefaultStr("CASSANDRA_REPLICATION_STRATEGY", cassandraDefaults.ReplicationStrategy.String()),
		fmt.Sprintf("Replication strategy to use when keyspace needs to be created (default value: %s, environment"+
			" variable: CASSANDRA_REPLICATION_STRATEGY). One of 'SimpleStrategy' and 'NetworkTopologyStrategy'.",
			cassandraDefaults.ReplicationStrategy))

	flag.StringVar(&cassandraReplicationFactor, "cassandra-replication-factors",
		envOrDefaultStr("CASSANDRA_REPLICATION_FACTORS", cassandraDefaults.ReplicationFactors.JSON()),
		fmt.Sprintf("Replication factor(s) to use when a keyspace needs to be created (default value: %s, environment"+
			" variable: CASSANDRA_REPLICATION_FACTORS). The value is a map of datacenter replication factors. "+
			"For example, '{\"dc1\": 3, \"dc2\": 3}'. When SimpleStrategy is specified, the map is expected to hold "+
			"a single value (with datacenter name 'cluster').", cassandraDefaults.ReplicationFactors.JSON()))

	flag.IntVar(&cassandraWriteConcurrency, "cassandra-write-concurrency",
		envOrDefaultInt("CASSANDRA_WRITE_CONCURRENCY", cassandraDefaults.WriteConcurrency),
		fmt.Sprintf("The number of goroutines to use to write a received log entry batch. "+
			"A value greater than one can (to a certain limit) increase write throughput for large batches. "+
			"Default value: %d, environment variable: CASSANDRA_WRITE_CONCURRENCY.", cassandraDefaults.WriteConcurrency))
	flag.IntVar(&cassandraWriteBufferSize, "cassandra-write-buffer-size",
		envOrDefaultInt("CASSANDRA_WRITE_BUFFER_SIZE", cassandraDefaults.WriteBufferSize),
		fmt.Sprintf("The maxiumum number of inserts that can be queued up "+
			"before additional writes will block. "+
			"Default value: %d, environment variable: CASSANDRA_WRITE_BUFFER_SIZE.", cassandraDefaults.WriteBufferSize))

	flag.BoolVar(&enableProfiling, "enable-profiling",
		envOrDefaultBool("ENABLE_PROFILING", defaultEnableProfiling),
		fmt.Sprintf("Enable CPU/memory profiling endpoint at /debug/pprof. "+
			"Default: %v, environment variable: ENABLE_PROFILING.",
			defaultEnableProfiling))

	flag.BoolVar(&showVersion, "version", false, fmt.Sprintf("Show version information."))
}

func main() {
	flag.Parse()

	if showVersion {
		fmt.Printf("version: %s\n", version)
		os.Exit(0)
	}

	cqlHosts := cassandraDefaults.Hosts
	if len(flag.Args()) > 0 {
		cqlHosts = flag.Args()
	}

	replStrategy := cassandra.ReplicationStrategy(cassandraReplicationStrategy)
	if err := replStrategy.Validate(); err != nil {
		log.Fatalf(err.Error())
	}
	replFactorMap, err := cassandra.NewReplicationFactorMap(cassandraReplicationFactor)
	if err != nil {
		log.Fatalf(err.Error())
	}
	cassandraOptions := &cassandra.Options{
		Hosts:               cqlHosts,
		CQLPort:             cassandraPort,
		Keyspace:            cassandraKeyspace,
		ReplicationStrategy: replStrategy,
		ReplicationFactors:  replFactorMap,
		LogTableName:        cassandraDefaults.LogTableName,
		WriteConcurrency:    cassandraWriteConcurrency,
		WriteBufferSize:     cassandraWriteBufferSize,
	}
	if err := cassandraOptions.Validate(); err != nil {
		log.Fatalf(err.Error())
	}

	// connect to cassandra
	log.Infof("using cassandra options: %s", cassandraOptions)
	cluster := gocql.NewCluster(cassandraOptions.Hosts...)
	cluster.Port = cassandraOptions.CQLPort
	cluster.Consistency = gocql.One
	cqlDriver := cassandra.NewCQLDriver(cluster)
	logStore := cassandra.NewLogStore(cqlDriver, cassandraOptions)
	err = logStore.Connect()
	if err != nil {
		log.Fatalf("failed to connect to cassandra: %s", err)
	}

	// start REST API server
	serverConfig := server.Config{
		BindAddress:     fmt.Sprintf("%s:%d", serverBindAddr, serverPort),
		EnableProfiling: enableProfiling,
	}
	server := server.NewHTTP(&serverConfig, logStore)
	go func() {
		err := server.Start()
		if err != nil {
			log.Fatalf("failed to start server: %s", err)
		}
	}()

	log.Infof("pid: %d", os.Getpid())

	// wait for process to be terminated (by SIGINT) and make sure we clean up
	// gracefully (shutdown http server and logstore connections)
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, os.Interrupt)
	// wait for a signal
	signal := <-sigChannel
	log.Infof("interrupted by signal: %s", signal)
	logStore.Disconnect()
	server.Stop()
}
