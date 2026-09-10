// Package observability wires Prometheus metrics and health/liveness
// endpoints for the Go binaries in this module (gatewayd, consumer). Both
// binaries are single-purpose processes -- there is no shared HTTP surface
// otherwise -- so a small dedicated server on its own port keeps metrics
// scraping independent of gRPC traffic and independent of whether gRPC
// itself is healthy.
package observability

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// StartServer exposes Prometheus metrics on /metrics and a liveness probe
// on /healthz at addr (e.g. ":9101"), and returns the *http.Server so the
// caller can Shutdown it on process exit. Listen failures are logged, not
// fatal -- a metrics outage should never take down request serving.
func StartServer(addr, serviceName string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("%s: metrics/health server listening on %s (/metrics, /healthz)", serviceName, addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("%s: metrics server stopped: %v", serviceName, err)
		}
	}()
	return srv
}

// Shutdown gives the metrics server up to 5s to stop cleanly. Intended for
// use in a deferred call from main().
func Shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// GRPCServerMetrics holds the per-RPC instrumentation recorded by
// UnaryServerInterceptor. Each binary that serves gRPC constructs one of
// these (via NewGRPCServerMetrics) and installs the interceptor at server
// creation time.
type GRPCServerMetrics struct {
	requestsTotal   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	inFlight        *prometheus.GaugeVec
}

// NewGRPCServerMetrics registers gRPC server metrics under the given
// namespace (e.g. "vsgw_gatewayd"). Call once per process.
func NewGRPCServerMetrics(namespace string) *GRPCServerMetrics {
	m := &GRPCServerMetrics{
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "requests_total",
			Help:      "Total unary gRPC requests handled, by method and status code.",
		}, []string{"method", "code"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "request_duration_seconds",
			Help:      "Unary gRPC request latency in seconds, by method.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "requests_in_flight",
			Help:      "Unary gRPC requests currently being handled, by method.",
		}, []string{"method"}),
	}
	prometheus.MustRegister(m.requestsTotal, m.requestDuration, m.inFlight)
	return m
}

// UnaryInterceptor records request count, latency, and in-flight gauge for
// every unary RPC this server handles. It never alters the response or
// error -- purely observational.
func (m *GRPCServerMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		m.inFlight.WithLabelValues(info.FullMethod).Inc()
		defer m.inFlight.WithLabelValues(info.FullMethod).Dec()

		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start).Seconds()

		code := status.Code(err)
		m.requestsTotal.WithLabelValues(info.FullMethod, code.String()).Inc()
		m.requestDuration.WithLabelValues(info.FullMethod).Observe(duration)

		return resp, err
	}
}
