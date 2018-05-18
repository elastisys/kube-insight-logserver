package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/elastisys/kube-insight-logserver/pkg/log"
)

// MetricDimensions represent the dimensions over which request
// metrics are categorized. Each data point for a given metric
// (for example, total_requests) will be categorized into these
// dimensions, making it a data point in a time-series (a metric
// and a particular set of metric dimension values).
//
//   total_requests{method=POST,path=/write,statusCode=200} 6
//   total_requests{method=GET,path=/metrics,statusCode=200} 5
//
type MetricDimensions struct {
	// Method is the HTTP method used: GET/POST/...
	Method string
	// Path is the requested path e.g., /write
	Path string
	// StatusCode is the response code, e.g.: 200
	StatusCode int
}

// MetricsMiddleware is a "middleware" intended to be added as an interceptor
// handler that is invoked prior and after a request is dispatched to its
// handler. It collects metrics about the request handling.
type MetricsMiddleware struct {
	TotalRequests   map[MetricDimensions]int64
	SumResponseTime map[MetricDimensions]float64
	AvgResponseTime map[MetricDimensions]float64
	// TODO: response time (95th percentile)

	updateMutex sync.Mutex
}

// NewMetricsMiddleware creates a new metricsMiddleware.
func NewMetricsMiddleware() *MetricsMiddleware {
	return &MetricsMiddleware{
		TotalRequests:   make(map[MetricDimensions]int64, 0),
		SumResponseTime: make(map[MetricDimensions]float64, 0),
		AvgResponseTime: make(map[MetricDimensions]float64, 0),
		updateMutex:     sync.Mutex{},
	}
}

// wrappedResponseWriter is used to wrap a regular http.ResponsWriter to
// allow the statusCode set by the handler function to be captured.
type wrappedResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newWrappedResponseWriter(w http.ResponseWriter) *wrappedResponseWriter {
	return &wrappedResponseWriter{w, -1}
}

// WriteHeader overrides the method in the wrapped http.ResponseWriter
// to capture the status code set by the request handler function.
func (w *wrappedResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

// Intercept is called by gorilla mux prior to passing the request through to
// the handling function `nextHandler`. Here, we time the request handling,
// log the request, and update the metric counters.
func (mw *MetricsMiddleware) Intercept(nextHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := newWrappedResponseWriter(w)

		start := time.Now()
		nextHandler.ServeHTTP(ww, r)
		elapsed := time.Since(start).Seconds()

		url, err := url.Parse(r.RequestURI)
		if err != nil {
			log.Errorf("failed to parse request URI: %s", err)
		}
		metricDim := MetricDimensions{Method: r.Method, Path: url.Path, StatusCode: ww.statusCode}
		log.Infof("%s => %s %s: %d [%fs]", r.RemoteAddr, r.Method, r.RequestURI, ww.statusCode, elapsed)

		mw.updateMutex.Lock()
		defer mw.updateMutex.Unlock()

		// update request count for the given status code
		_, ok := mw.TotalRequests[metricDim]
		if !ok {
			mw.TotalRequests[metricDim] = 0
		}
		mw.TotalRequests[metricDim]++

		// update sum of response times for the given status code
		_, ok = mw.SumResponseTime[metricDim]
		if !ok {
			mw.SumResponseTime[metricDim] = 0
		}
		mw.SumResponseTime[metricDim] += elapsed

		// update average response time for the given status code
		mw.AvgResponseTime[metricDim] =
			mw.SumResponseTime[metricDim] / float64(mw.TotalRequests[metricDim])

	})
}

// Metrics returns a byte buffer containing a snapshot of the collected
// metrics thus far.
func (mw *MetricsMiddleware) Metrics() *bytes.Buffer {
	var buffer bytes.Buffer

	mw.updateMutex.Lock()
	defer mw.updateMutex.Unlock()

	for dim, val := range mw.TotalRequests {
		buffer.WriteString(fmt.Sprintf("total_requests{method=\"%s\",path=\"%s\",statusCode=\"%d\"} %d\n",
			dim.Method, dim.Path, dim.StatusCode, val))
	}
	for dim, val := range mw.SumResponseTime {
		buffer.WriteString(fmt.Sprintf("sum_response_time{method=\"%s\",path=\"%s\",statusCode=\"%d\"} %f\n",
			dim.Method, dim.Path, dim.StatusCode, val))
	}
	for dim, val := range mw.AvgResponseTime {
		buffer.WriteString(fmt.Sprintf("avg_response_time{method=\"%s\",path=\"%s\",statusCode=\"%d\"} %f\n",
			dim.Method, dim.Path, dim.StatusCode, val))
	}

	return &buffer
}
