package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
	"github.com/elastisys/kube-insight-logserver/pkg/logstore"
	"github.com/gorilla/mux"
)

// Config describes a configuration for a HTTPServer.
type Config struct {
	// BindAddress describes the local IP address and port to bind the server
	// listen socket to. For example, "0.0.0.0:8080".
	BindAddress string
	// EnableProfiling can be used to enable Go profiling and set up HTTP
	// endpoints that, for example, can be queried with
	//    go tool pprof <binary> http://<host>:<port>/debug/pprof/heap
	EnableProfiling bool
}

// HTTPServer represents a HTTP/REST API server for a particular LogStore.
type HTTPServer struct {
	server            *http.Server
	logStore          logstore.LogStore
	metricsMiddleware *MetricsMiddleware
}

// NewHTTP creates a new HTTP (REST API) server with a given configuration and
// backing LogStore. The LogStore is assumed to already be in a connected state.
func NewHTTP(serverConfig *Config, logStore logstore.LogStore) *HTTPServer {
	// register handlers
	r := mux.NewRouter()
	s := HTTPServer{
		server:            &http.Server{Addr: serverConfig.BindAddress, Handler: r},
		logStore:          logStore,
		metricsMiddleware: NewMetricsMiddleware(),
	}

	r.Use(s.metricsMiddleware.Intercept)
	r.HandleFunc("/write", s.writeGetHandler).Methods("GET")
	r.HandleFunc("/write", s.writePostHandler).Methods("POST")
	r.HandleFunc("/query", s.queryGetHandler).Methods("GET")
	r.HandleFunc("/metrics", s.metricsGetHandler).Methods("GET")

	if serverConfig.EnableProfiling {
		log.Infof("enabling profiling under /debug/pprof")
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
		r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		r.Handle("/debug/pprof/block", pprof.Handler("block"))
	}

	return &s
}

// Start starts the HTTP server. If successful, this method will block until the
// server is stopped.
func (s *HTTPServer) Start() error {
	log.Infof("starting server on address %s ...", s.server.Addr)
	return s.server.ListenAndServe()
}

// Stop shuts down the HTTP server.
func (s *HTTPServer) Stop() error {
	log.Infof("stopping server ...")
	return s.server.Shutdown(context.Background())
}

// writeGetHandler reponds to GET /write (which is a health probe)
func (s *HTTPServer) writeGetHandler(w http.ResponseWriter, r *http.Request) {
	healthy, err := s.logStore.Ready()
	status := logstore.APIStatus{Healthy: healthy}

	responseCode := http.StatusOK
	if err != nil {
		responseCode = http.StatusServiceUnavailable
		status.Detail = err.Error()
	}

	bytes, err := json.Marshal(status)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("failed to marshal api status: %s", err)))
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(responseCode)
	w.Write(bytes)
}

// writePostHandler reponds to POST /write
func (s *HTTPServer) writePostHandler(w http.ResponseWriter, r *http.Request) {
	logEntries := make([]logstore.LogEntry, 0)
	if err := json.NewDecoder(r.Body).Decode(&logEntries); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			logstore.APIError{Message: "failed to parse request", Detail: err.Error()})
		return
	}

	// ensure log entries are valid (and can be inserted into data store)
	for _, logEntry := range logEntries {
		lp := &logEntry
		if err := lp.Validate(); err != nil {
			s.errorResponse(w, http.StatusBadRequest,
				logstore.APIError{Message: "invalid log entry", Detail: err.Error()})
			return
		}
	}

	log.Debugf("received %d log entries", len(logEntries))

	_, err := s.logStore.Ready()
	if err != nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			logstore.APIError{Message: "data store is not ready", Detail: err.Error()})
		return
	}

	// write to backend
	if err := s.logStore.Write(logEntries); err != nil {
		log.Errorf("failed to store log entries: %s", err)
		s.errorResponse(w, http.StatusInternalServerError,
			logstore.APIError{Message: "failed to store entries", Detail: err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
}

// queryGetHandler reponds to GET /query
func (s *HTTPServer) queryGetHandler(w http.ResponseWriter, r *http.Request) {
	query, err := queryFromRequest(r)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			logstore.APIError{Message: "invalid query", Detail: err.Error()})
		return
	}
	if err := query.Validate(); err != nil {
		s.errorResponse(w, http.StatusBadRequest,
			logstore.APIError{Message: "invalid query", Detail: err.Error()})
		return
	}

	_, err = s.logStore.Ready()
	if err != nil {
		s.errorResponse(w, http.StatusServiceUnavailable,
			logstore.APIError{Message: "data store is not ready", Detail: err.Error()})
		return
	}

	log.Debugf("received query: %s", query)
	rows, err := s.logStore.Query(query)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError,
			logstore.APIError{Message: "query execution error", Detail: err.Error()})
		return
	}
	bytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError,
			logstore.APIError{Message: "failed to serialize response", Detail: err.Error()})
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// metricsGetHandler reponds to GET /metrics
func (s *HTTPServer) metricsGetHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	s.metricsMiddleware.Metrics().WriteTo(w)
}

func queryFromRequest(r *http.Request) (*logstore.Query, error) {
	namespace, err := getQueryParam("namespace", r)
	if err != nil {
		return nil, err
	}
	podName, err := getQueryParam("pod_name", r)
	if err != nil {
		return nil, err
	}
	containerName, err := getQueryParam("container_name", r)
	if err != nil {
		return nil, err
	}
	startTimeStr, err := getQueryParam("start_time", r)
	if err != nil {
		return nil, err
	}
	startTime, err := time.Parse(time.RFC3339Nano, startTimeStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse start_time")
	}
	// end_time is optional (defaults to current time)
	endTime := time.Now().UTC()
	endTimeStr, err := getQueryParam("end_time", r)
	if err == nil {
		endTime, err = time.Parse(time.RFC3339Nano, endTimeStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse end_time")
		}
	}

	query := logstore.Query{
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
		StartTime:     startTime,
		EndTime:       endTime,
	}
	return &query, nil
}

func getQueryParam(paramName string, r *http.Request) (string, error) {
	var paramValues []string
	paramValues, exist := r.URL.Query()[paramName]
	if !exist {
		return "", fmt.Errorf("missing query parameter: %s", paramName)
	}
	if len(paramValues) != 1 {
		return "", fmt.Errorf("query parameter %s has wrong number of values: was: %d, expected: %d",
			paramName, len(paramValues), 1)
	}
	return paramValues[0], nil
}

func (s *HTTPServer) errorResponse(w http.ResponseWriter, statusCode int, errorMsg logstore.APIError) {
	bytes, err := json.Marshal(errorMsg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("failed to marshal error: %s", err)))
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(bytes)
}
