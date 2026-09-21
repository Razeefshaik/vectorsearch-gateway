import os
import sys
import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import grpc
from concurrent import futures
from sentence_transformers import SentenceTransformer

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

import embed_pb2
import embed_pb2_grpc
from embedding_cache import EmbeddingCache

import torch
torch.set_num_threads(1)

# Metric names are namespaced "vsgw_embed_*" to match the vsgw_gatewayd_* /
# vsgw_consumer_* convention used by the Go services, so the same metric
# family reads consistently across languages in Grafana. The two running
# instances (embed-search, embed-ingest) are told apart by Prometheus job
# name in prometheus.yml, not by a label here.
EMBED_REQUESTS_TOTAL = Counter(
    "vsgw_embed_requests_total",
    "Embed RPCs handled, by result.",
    ["result"],
)
EMBED_REQUEST_DURATION = Histogram(
    "vsgw_embed_request_duration_seconds",
    "Embed RPC latency in seconds.",
)
EMBED_IN_FLIGHT = Gauge(
    "vsgw_embed_requests_in_flight",
    "Embed RPCs currently being handled.",
)
EMBED_MODEL_LOAD_SECONDS = Gauge(
    "vsgw_embed_model_load_seconds",
    "Time taken to load the sentence-transformers model at startup.",
)
EMBED_CACHE_REQUESTS_TOTAL = Counter(
    "vsgw_embed_cache_requests_total",
    "Embedding cache lookups, by result.",
    ["result"],
)

MODEL_NAME = os.environ.get(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)
EXPECTED_DIMENSION = int(os.environ.get("EMBEDDING_DIMENSION", "384"))
CACHE_MAX_ENTRIES = int(os.environ.get("EMBED_CACHE_MAX_ENTRIES", "0"))



class EmbedServicer(embed_pb2_grpc.EmbedServiceServicer):
    def __init__(self):
        start = time.monotonic()
        self.model = SentenceTransformer(MODEL_NAME)
        self.cache = EmbeddingCache(CACHE_MAX_ENTRIES)
        EMBED_MODEL_LOAD_SECONDS.set(time.monotonic() - start)
        actual_dimension = self.model.get_embedding_dimension()

        if actual_dimension != EXPECTED_DIMENSION:
            raise RuntimeError(
                "Embedding dimension mismatch: "
                f"model={MODEL_NAME}, actual={actual_dimension}, "
                f"expected={EXPECTED_DIMENSION}"
            )
        print(
            "Embedding model ready: "
            f"model={MODEL_NAME}, dimension={actual_dimension}"
        )
    
    
    def Embed(self, request, context):
        EMBED_IN_FLIGHT.inc()
        start = time.monotonic()
        try:
            if not request.text or not request.text.strip():
                EMBED_REQUESTS_TOTAL.labels(result="invalid_argument").inc()
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("text must not be empty")
                return embed_pb2.EmbedResponse()

            vector = self.cache.get(MODEL_NAME, request.text)
            if vector is None:
                EMBED_CACHE_REQUESTS_TOTAL.labels(result="miss").inc()
                vector = self.model.encode(request.text).tolist()
                self.cache.put(MODEL_NAME, request.text, vector)
            else:
                EMBED_CACHE_REQUESTS_TOTAL.labels(result="hit").inc()
            EMBED_REQUESTS_TOTAL.labels(result="success").inc()
            return embed_pb2.EmbedResponse(vector=vector)
        finally:
            EMBED_REQUEST_DURATION.observe(time.monotonic() - start)
            EMBED_IN_FLIGHT.dec()


class MetricsHandler(BaseHTTPRequestHandler):
    """Serves /metrics (Prometheus exposition) and /healthz (liveness) on
    a port separate from the gRPC port, so scraping never competes with or
    depends on gRPC traffic."""

    def do_GET(self):
        if self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)
        elif self.path == "/healthz":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # don't let periodic scrapes/health checks spam stdout


def start_metrics_server(port):
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="metrics-http")
    thread.start()
    return server


def serve():
    port = sys.argv[1] if len(sys.argv) > 1 else "50051"
    # Metrics port comes from env, not argv, so the Dockerfile HEALTHCHECK
    # (which only has env vars, not the compose `command:` list) and this
    # server agree on the same port without duplicating it in two places.
    metrics_port = int(os.environ.get("METRICS_PORT", "9100"))

    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    embed_pb2_grpc.add_EmbedServiceServicer_to_server(EmbedServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    start_metrics_server(metrics_port)
    print(f"embed service listening on :{port} (metrics on :{metrics_port})")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
