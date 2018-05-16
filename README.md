[![Go Report Card](https://goreportcard.com/badge/github.com/elastisys/kube-insight-logserver)](https://goreportcard.com/report/github.com/elastisys/kube-insight-logserver)
[![Build Status](https://travis-ci.org/elastisys/kube-insight-logserver.svg?branch=master)](https://travis-ci.org/elastisys/kube-insight-logserver)
[![Coverage](https://codecov.io/gh/elastisys/kube-insight-logserver/branch/master/graph/badge.svg)](https://codecov.io/gh/elastisys/kube-insight-logserver)

## kube-insight-logserver
`kube-insight-logserver` is a logging data service that stores Kubernetes pod
logs in Cassandra for later querying. It exposes a HTTP REST API, which can be
used both to ingest Kubernetes pod log entries into Cassandra and to query
historical log entries.

The log ingest part of the API has been written to be compatible with
[fluentbit](https://fluentbit.io/) and its [HTTP output
plugin](https://fluentbit.io/documentation/current/output/http.html).

Fluentbit can be installed as a Kubernetes daemon set to capture logs from
pod containers running in a cluster. For an example of manifests, have a look in
the [kube-insight-manifests
repo](https://github.com/elastisys/kube-insight-manifests/tree/master/logging/).



## REST API
The REST API includes the endpoints outlined in the below sections.


### POST /write

The `/write` resource is used to ingest Kubernetes pod logs into Cassandra in a
format that  is compatible with fluentbit's [Kubernetes metadata
filter](https://fluentbit.io/documentation/current/filter/kubernetes.html).

It accepts a JSON array of log entries of the following form:

    [{
        "date": 1525349097.094408,
        "kubernetes": {
            "docker_id": "e8b89cc4e292827b2f521c4c7d7b8807cf72023565b9ac5f89f8186420325d74",
            "labels": {
                "pod-template-generation": "1",
                "name": "weave-net",
                "controller-revision-hash": "2689456918"
            },
            "host": "master1",
            "pod_name": "weave-net-5mfwh",
            "container_name": "weave",
            "pod_id": "f5225d5f-4e9d-11e8-8b6b-02425d6e035a",
            "namespace_name": "kube-system"
        },
        "log": "INFO: 2018/05/03 12:04:57.094154 Discovered remote MAC 36:96:7c:78:d0:22 at 4a:26:23:65:5b:88(worker1)",
        "stream": "stderr",
        "time": "2018-05-03T12:04:57.094408152Z"
    }, ... ]

A simple Python ingest client can be found under
[scripts/insert.py](scripts/insert.py).



### GET /write
Used as a health probe. When called, it will attempt to connect to Cassandra.
If the response code is different from `200`, the service is to be considered
(temporarily) unavailable and writes/queries will fail.

    $ curl -X GET http://localhost:8080/write
    {"healthy":true,"detail":""}




### GET /query
A query endpoint for querying historical log records for a certain pod container
(in a given namespace). It can be queried via:

    /query?namespace=<namespace>&pod_name=<name>&container_name=<name>&start_time=<timestamp>&end_time=<timestamp>

For example, using `curl`:

    curl -G  "http:/localhost:8080/query" \
      --data-urlencode "namespace=default" \
      --data-urlencode "pod_name=nginx-deployment-abcde" \
      --data-urlencode "container_name=nginx" \
      --data-urlencode "start_time=2018-05-07T00:00:00.000Z" \
      --data-urlencode "end_time=2018-05-07T00:01:00.000Z"
    {
      "log_rows": [
        {
          "time": "2018-05-07T00:00:00Z",
          "log": "10.46.0.0 - - [2018-05-07T00:00:00.000000Z] 'GET /index.html HTTP/1.1' 200 647 '-' 'kube-probe/1.10' '-'"
        },
        {
          "time": "2018-05-07T00:00:00.1Z",
          "log": "10.46.0.0 - - [2018-05-07T00:00:00.100000Z] 'GET /index.html HTTP/1.1' 200 647 '-' 'kube-probe/1.10' '-'"
        },
        ...
      ]
    }

A simple Python query client can be found under
[scripts/query.py](scripts/query.py).



### GET /metrics
The `/metrics` endpoint provides server performance metrics in a
Prometheus-compatible format. It tracks metrics categorized along the following
dimensions:

- `method`: request method (e.g. `GET`)
- `path`: request path (e.g. `/query`)
- `statusCode`: response status code (e.g. `200`)

An example invocation:


    $ curl X GET  http://localhost:8080/metrics
    total_requests{method=GET,path=/write,statusCode=200} 1
    total_requests{method=POST,path=/write,statusCode=200} 96
    total_requests{method=GET,path=/metrics,statusCode=200} 1
    total_requests{method=GET,path=/query,statusCode=200} 1
    sum_response_time{method=GET,path=/write,statusCode=200} 0.002639
    sum_response_time{method=POST,path=/write,statusCode=200} 0.886447
    sum_response_time{method=GET,path=/metrics,statusCode=200} 0.000120
    sum_response_time{method=GET,path=/query,statusCode=200} 0.012495
    avg_response_time{method=GET,path=/write,statusCode=200} 0.002639
    avg_response_time{method=POST,path=/write,statusCode=200} 0.009234
    avg_response_time{method=GET,path=/metrics,statusCode=200} 0.000120
    avg_response_time{method=GET,path=/query,statusCode=200} 0.012495



### GET /debug/pprof/...
If the server is started with profiling (via the `ENABLE_PROFILING=true`
environment variable or the `--enable-profiling` command-line option),
[pprof](https://golang.org/pkg/net/http/pprof/) endpoints will be published
which can be used together with the `go tool pprof` tool.

For example, to profile the application's heap usage:

    go tool pprof ./kube-insight-logserver http://127.0.0.1:8080/debug/pprof/heap



### Build
[dep](https://github.com/golang/dep) is used for dependency management.
Make sure it is [installed](https://github.com/golang/dep/releases).

    make build


### Test
There is a build target for testing:

    make test

However, if you only want to run tests for a certain package, simply do:

    go test ./pkg/logstore/cassandra

To see detailed logs (for a certain log-level), run something like:

    go test -v ./pkg/logstore/cassandra -args -log-level=4

To only run a particular test function, use something like:

    go test  -v ./pkg/logstore/cassandra/ -run TestLogStoreQueryThatCrossesDateBorder -args -log-level 4



### Run
To start the server with default parameters, run:

    ./bin/kube-insight-logserver

To view the set of environment variables and options (take precendence)
run with the `--help` flag.

The `-enable-profiling` flag configures HTTP endpoints for the Go
[pprof package](https://golang.org/pkg/net/http/pprof/). With this option
enabled one can, for instance, look at memory allocation using

     go tool pprof ./kube-insight-logserver http://127.0.0.1:8080/debug/pprof/heap

or CPU profiling using

     go tool pprof ./kube-insight-logserver http://127.0.0.1:8080/debug/pprof/profile



### Build docker image
To build an Alpine-based docker image, run:

    make docker-image
    # optionally push to registry
    make docker-push


### Run docker image
The docker image (`elastisys/kube-insight-logserver:<version>`)
can be run via:

    docker run elastisys/kube-insight-logserver:<version>

The container's entrypoint is the `kube-insight-logserver` binary, meaning that
any options to it can be passed directly to `docker run`. For example,

    docker run elastisys/kube-insight-logserver:<version> --help

As described in the help text, the behavior of the server can also be controlled
via environment variables.



## Developer notes

### Dependencies
To introduce a new dependency, add it to `Gopkg.toml` (and the `vendor`
folder) using something like

     dep ensure -add go.uber.org/zap

and then make sure you make use of (import) it in the code before the next time
you run `dep ensure` (if not used, this will prune it).
