# vectorsearch-gateway — Monitoring, Metrics & Deployment

This is the operations reference: every metric this system emits, the
dashboards and alerts built from them, health checks, the production
container build, and the single command that brings the whole stack up. For
what the system *is* and how a request flows through it, see
[docs/ARCHITECTURE.md](./ARCHITECTURE.md) — this document assumes that one.

---

> **BugBrother feature branch behavior:**
> `feature/search-client-id-isolation` does not emit gateway rate-limit denials
> because gatewayd does not use the token-bucket limiter on this branch.
> Watch Kafka publish failures, consumer lag, embedding latency, and vector
> insertion failures to understand indexing capacity.

## 1. Why one combined monitoring stack

The question this project raised explicitly: should gatewayd, the embed
services, Kafka, and the coordinator/shards (vector-search engine — a
separate repo, [VectorSearchEngine](../../VectorSearchEngine)) each get
their own metrics/monitoring stack, or share one?

**Decision: one combined Prometheus + Grafana + Alertmanager stack for the
whole system, including the coordinator and all 4 shards.** Reasoning:

- **A single request crosses every service.** A slow `Search` call is one
  timeline that passes through gatewayd → embed-search → coordinator →
  every shard (scatter-gather, see ARCHITECTURE.md §4). A dashboard that can
  only show one hop at a time can't answer "why is Search slow right now" —
  you'd be tab-switching between separate Grafana instances trying to line
  up timestamps by eye. One Prometheus with one time axis makes that a
  single panel with N lines.
- **The ingest pipeline is inherently cross-service.** Kafka consumer lag
  (from `kafka-exporter`) only means something next to the consumer's own
  processing-outcome metrics and the coordinator's insert error rate — they
  are views of one pipeline, not unrelated systems.
- **Operational overhead scales with the number of stacks, not the number of
  services.** A second Prometheus/Grafana pair is another set of
  credentials, another alerting config to keep in sync, another dashboard
  URL to remember during an incident — for a system this size, that cost
  buys nothing a `job` label doesn't already give you for free.
- **A separate source repo doesn't mean a separate ops boundary.**
  VectorSearchEngine is developed independently (its own git history, its
  own `DEPLOYMENT.md`/`ARCHITECTURE.md`), but it's instrumented with the
  exact same `vsgw_*` metric-naming convention (§2) and wired into this
  same `docker-compose.yml`, Prometheus, and Grafana — see §6 for how.

If the system grows to the point where different teams own different
services and need independent alerting ownership, that's the point to
reconsider — not before.

---

## 2. Metric catalog

All application metrics are namespaced `vsgw_<service>_...` (`vsgw` =
vectorsearch-gateway) so every metric in Grafana/Prometheus is
unambiguous about which process emitted it, and so Go and Python metrics
read as one consistent family. Standard runtime metrics (`process_*`,
`go_*` for Go via `client_golang`'s default collectors, `python_gc_*` for
Python via `prometheus_client`'s defaults) are also exposed on every
`/metrics` endpoint automatically — not listed below, but available for
CPU/memory/GC troubleshooting.

### gatewayd (`go/main.go`, `go/gateway/*.go`)

Endpoint: `http://gatewayd:9101/metrics` (env `GATEWAYD_METRICS_PORT`).
Instrumented in `go/internal/observability/observability.go`
(gRPC interceptor), `go/gateway/server.go` (rate-limit denials), and
`go/gateway/producer.go` (Kafka publish).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vsgw_gatewayd_grpc_requests_total` | counter | `method`, `code` | Every unary RPC gatewayd handled, by full method name and final gRPC status code. |
| `vsgw_gatewayd_grpc_request_duration_seconds` | histogram | `method` | Handler latency, start to response, per RPC. |
| `vsgw_gatewayd_grpc_requests_in_flight` | gauge | `method` | RPCs currently being handled — spikes here precede latency spikes. |
| `vsgw_gatewayd_ratelimiter_denied_total` | counter | `method` | Requests rejected by `Limiter.Allow` before reaching business logic (`Search` or `Insert`). |
| `vsgw_gatewayd_kafka_publish_total` | counter | `result` (`success`\|`error`\|`marshal_error`) | Every attempt to publish an `IngestEvent` to Kafka from `Insert`. |
| `vsgw_gatewayd_kafka_publish_duration_seconds` | histogram | — | Time spent inside `kafka.Writer.WriteMessages`. |

### consumer (`go/gateway/consumer/main.go`)

Endpoint: `http://consumer:9102/metrics` (env `CONSUMER_METRICS_PORT`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vsgw_consumer_messages_consumed_total` | counter | — | Kafka messages fetched (`FetchMessage`), regardless of outcome. |
| `vsgw_consumer_messages_processed_total` | counter | `outcome` (`success`\|`duplicate`\|`permanent_error`\|`transient_error`) | Final disposition of `processEvent`, matching the commit/skip decision documented in ARCHITECTURE.md §5. |
| `vsgw_consumer_processing_duration_seconds` | histogram | — | End-to-end time for one `processEvent` call (embed + coordinator insert). |
| `vsgw_consumer_embed_client_requests_total` | counter | `result` (`success`\|`invalid_argument`\|`error`) | Consumer's own `Embed` calls against embed-ingest. |
| `vsgw_consumer_coordinator_client_insert_requests_total` | counter | `result` (`success`\|`already_exists`\|`permanent_error`\|`transient_error`) | Consumer's `Insert` calls against the coordinator. |
| `vsgw_consumer_commit_failures_total` | counter | — | Kafka `CommitMessages` calls that themselves failed (separate from processing failures — this means the *offset write* failed, which risks reprocessing a message that already succeeded). |

### embed-search / embed-ingest (`python/embed_service/server.py`)

Endpoint: `http://embed-search:9100/metrics` and
`http://embed-ingest:9100/metrics` — same internal port, different
container/hostname; distinguished in Prometheus by `job` label
(`embed-search` vs `embed-ingest`), not by a metric label, since both run
identical code.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vsgw_embed_requests_total` | counter | `result` (`success`\|`invalid_argument`) | Every `Embed` RPC handled. |
| `vsgw_embed_request_duration_seconds` | histogram | — | `Embed` handler latency, including model inference. |
| `vsgw_embed_requests_in_flight` | gauge | — | Concurrent `Embed` calls — the server runs a 4-worker thread pool, so this saturating at 4 is a real capacity signal. |
| `vsgw_embed_model_load_seconds` | gauge | — | One-time cost of `SentenceTransformer(...)` at process startup. Set once; useful for comparing cold-start time across deploys. |

### coordinator (`VectorSearchEngine/go/cmd/coordinatord`, `go/coordinator/*.go`)

Endpoint: `http://coordinator:9105/metrics` (flag `-metrics-listen`, env-wired
via `COORDINATOR_METRICS_PORT`). Instrumented in
`go/observability/observability.go` (gRPC interceptor, same package design
as this repo's) and `go/coordinator/metrics.go` (per-shard fan-out calls).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vsgw_coordinator_grpc_requests_total` | counter | `method`, `code` | Every unary RPC the coordinator handled (`Insert`, `Delete`, `Search`). |
| `vsgw_coordinator_grpc_request_duration_seconds` | histogram | `method` | Handler latency. For `Search` this includes the full scatter-gather across every shard. |
| `vsgw_coordinator_grpc_requests_in_flight` | gauge | `method` | Requests currently being handled. |
| `vsgw_coordinator_shard_client_requests_total` | counter | `shard`, `method`, `result` | Calls the coordinator made *to* a shard — the thing that tells you which specific shard is the problem when `Search`/`Insert` errors are up. |
| `vsgw_coordinator_shard_client_request_duration_seconds` | histogram | `shard`, `method` | Latency of one coordinator→shard call. |
| `vsgw_coordinator_search_shards_queried_total` | counter | — | Sum of `shards_queried` across every `Search` — total shard-queries issued. |
| `vsgw_coordinator_search_shards_failed_total` | counter | — | Sum of `shards_failed` across every `Search`. `shards_failed_total / shards_queried_total` is the system's real degraded-coverage rate (see `SearchDegradedCoverage` alert, §5). |

### shard0-3 (`VectorSearchEngine/go/cmd/shardd`, `go/shard/*.go`)

Endpoint: `http://shard<N>:9106/metrics` (flag `-metrics-listen`, env-wired
via `SHARD_METRICS_PORT`) — all 4 shards run identical code on the same
internal port; the `instance` label Prometheus attaches
(`shard0:9106`, `shard1:9106`, ...) is what tells them apart. Instrumented in
`go/shard/metrics.go`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vsgw_shard_grpc_requests_total` | counter | `method`, `code` | Every unary RPC this shard handled (`Insert`, `Delete`, `Undelete`, `Search`, `Snapshot`, `Stats`). |
| `vsgw_shard_grpc_request_duration_seconds` | histogram | `method` | Handler latency. |
| `vsgw_shard_grpc_requests_in_flight` | gauge | `method` | Requests currently being handled. |
| `vsgw_shard_index_vectors_total` | gauge | — | Total vectors in the index, including tombstoned (deleted-but-not-compacted) entries. |
| `vsgw_shard_index_active_vectors` | gauge | — | Vectors not tombstoned — what `Search` actually considers. |
| `vsgw_shard_index_capacity` | gauge | — | Fixed `-max-elements` this shard was created with. Capacity is set at creation and does not grow. |
| `vsgw_shard_index_memory_bytes` | gauge | — | Approximate memory held by the C++ HNSW index arena. |
| `vsgw_shard_snapshot_total` | counter | `result` (`success`\|`error`) | Every `Index.Snapshot()` call, scheduled or on-demand via the `Snapshot` RPC. |
| `vsgw_shard_snapshot_duration_seconds` | histogram | — | Snapshot duration — this **blocks all writes** on that shard for its duration (see `durable.Index.mu`'s doc comment), so a growing p95 here is a direct latency risk, not just a background-job metric. |

### Kafka (via `kafka-exporter`)

Endpoint: `http://kafka-exporter:9308/metrics`. This is
[`danielqsj/kafka-exporter`](https://github.com/danielqsj/kafka_exporter),
not code in this repo — listed here because the dashboards and alerts
depend on it directly. Key metrics used: `kafka_consumergroup_lag`
(per consumer-group/topic/partition), `kafka_topic_partitions`,
`kafka_consumergroup_current_offset`. Full metric list is in that
project's README.

---

## 3. Health check endpoints

Every Go/Python service in this repo exposes `/healthz` on its metrics
port, returning `200 ok` once the process is listening. This is a
**liveness** check, not a deep readiness check — it confirms the process is
up and serving, not that its downstream dependencies (Kafka, the
coordinator) are reachable. That's intentional: mixing liveness with
downstream health means a downstream outage can cause Docker/Kubernetes to
kill and restart an otherwise-healthy process in a loop, making the outage
worse. Downstream health is what the Grafana dashboard and alerts (§5) are
for.

The coordinator and shards (VectorSearchEngine) predate this pass and
already had their own liveness mechanism — the gRPC Health Checking
Protocol (`google.golang.org/grpc/health`), registered in both
`cmd/coordinatord` and `cmd/shardd` — plus a `docker-compose.yml` healthcheck
that does a raw TCP probe (`nc -z localhost <port>`) rather than HTTP. Both
of those were left as-is; the `/metrics` + `/healthz` HTTP server added in
this pass (`VectorSearchEngine/go/observability/observability.go`) is purely
additive alongside them, not a replacement.

| Service | Endpoint | Checked by |
|---|---|---|
| gatewayd | `http://localhost:9101/healthz` | `go/Dockerfile.gatewayd` `HEALTHCHECK` |
| consumer | `http://localhost:9102/healthz` | `go/Dockerfile.consumer` `HEALTHCHECK` |
| embed-search / embed-ingest | `http://localhost:9100/healthz` (in-container) | `python/embed_service/Dockerfile` `HEALTHCHECK` |
| coordinator | gRPC Health Checking Protocol; TCP probe on `:8000` | `docker-compose.yml`'s `nc -z` healthcheck (VectorSearchEngine convention) |
| shard0-3 | gRPC Health Checking Protocol; TCP probe on `:7001` | same `nc -z` pattern, per shard |
| kafka | broker API versions probe | compose-level `healthcheck:` on the `kafka` service |
| prometheus | `http://localhost:9090/-/healthy` | compose-level `healthcheck:` |

`docker-compose.yml`'s `depends_on: { condition: service_healthy }` chains
on these so `gatewayd`/`consumer` don't start accepting traffic (or, more
precisely, don't start at all) until Kafka and both embed instances report
healthy. `./deploy.ps1` / `./deploy.sh` additionally use
`docker compose up --wait`, which blocks the deploy command itself until
every healthcheck passes (or times out) — see §7.

---

## 4. Grafana dashboard

Auto-provisioned on Grafana startup — no manual dashboard creation. Files:

- `monitoring/grafana/provisioning/datasources/datasource.yml` — registers
  Prometheus (`http://prometheus:9090`) as the default datasource.
- `monitoring/grafana/provisioning/dashboards/dashboards.yml` — tells
  Grafana to load any dashboard JSON found under
  `monitoring/grafana/provisioning/dashboards/json/`.
- `monitoring/grafana/provisioning/dashboards/json/gateway-overview.json` —
  the dashboard itself, titled **"vectorsearch-gateway — Overview"**.

Panel groups (top to bottom):

1. **Service health** — an `up{}` stat panel across every scrape target, so
   "is anything down" is answerable at a glance before drilling into
   anything else.
2. **gatewayd — request path** — request rate and error rate by RPC method,
   p50/p95/p99 latency, in-flight requests, rate-limit rejections, and
   Kafka publish rate/result.
3. **Ingest pipeline — Kafka + consumer** — consumer group lag, messages
   consumed vs. processed (by outcome), processing latency percentiles,
   commit failures, and the consumer's own coordinator-insert result
   breakdown.
4. **Embed services** — request rate and p95 latency per instance
   (search vs. ingest side by side), in-flight requests, and model load
   time.
5. **Coordinator & shards** — coordinator request/error rate and latency
   percentiles, coordinator→shard call results broken out by shard index
   (so a single bad shard is visible at a glance), search shard-coverage
   (queried vs. failed), per-shard gRPC latency, per-shard capacity
   utilization, vector counts (total vs. active), and snapshot
   duration/failures.

Access at `http://localhost:${GRAFANA_PORT}` (default `3000`), login
`admin` / `${GRAFANA_ADMIN_PASSWORD}` (default `admin` — **change this in
`.env` before deploying anywhere reachable by anyone else**).

---

## 5. Alerting

Rules live in `monitoring/prometheus/alerts.yml`, loaded by Prometheus via
`rule_files` in `prometheus.yml`. Routing/receivers live in
`monitoring/alertmanager/alertmanager.yml`.

| Alert | Fires when | Severity |
|---|---|---|
| `ServiceDown` | Any scrape target has been unreachable for 1m. | critical |
| `GatewaydHighErrorRate` | >5% of a method's requests are non-OK, sustained 5m. | warning |
| `GatewaydHighLatencyP99` | p99 latency for a method exceeds 2s, sustained 5m. | warning |
| `GatewaydRateLimitSpike` | Rate-limit rejections exceed 5/s, sustained 5m. | info |
| `GatewaydKafkaPublishErrors` | Any non-success Kafka publish in the last 5m. | critical |
| `ConsumerStalled` | No messages fetched in 10m while consumer lag > 0. | critical |
| `ConsumerHighTransientErrorRate` | >10% of processed events are `transient_error`, sustained 5m. | warning |
| `ConsumerGroupLagGrowing` | Total consumer-group lag exceeds 1000 messages, sustained 10m. | warning |
| `EmbedHighLatencyP95` | An embed instance's p95 latency exceeds 1s, sustained 5m. | warning |
| `EmbedHighErrorRate` | An embed instance's error rate exceeds 10%, sustained 5m. | warning |
| `CoordinatorHighErrorRate` | >5% of a coordinator method's requests are non-OK, sustained 5m. | warning |
| `CoordinatorHighLatencyP99` | Coordinator p99 latency for a method exceeds 1s, sustained 5m. | warning |
| `ShardCallErrors` | Coordinator sees sustained errors calling a specific shard, sustained 5m. | warning |
| `SearchDegradedCoverage` | Over 5% of shard-queries across all Searches fail, sustained 5m (see `shards_failed`/`shards_queried`). | warning |
| `ShardNearCapacity` | A shard's active vectors exceed 90% of its fixed `-max-elements` capacity, sustained 15m. | warning |
| `ShardSnapshotFailing` | A shard's snapshot failed in the last 30m. | critical |

**Thresholds are starting points**, not tuned SLOs — there's no real traffic
history to calibrate against yet. Adjust the `for:` durations and
comparison values in `alerts.yml` once you have a baseline.

**No real notification channel is wired up.** `alertmanager.yml`'s default
receiver is `null` (alerts are evaluated, grouped, and visible in the
Alertmanager UI at `http://localhost:${ALERTMANAGER_PORT}`, but nothing gets
paged) because this repo has no Slack webhook, PagerDuty key, or SMTP
credentials to put there. Commented examples for Slack, a generic webhook,
and email are in that file — uncomment and fill in one, then point
`route.receiver` at it.

---

## 6. How the coordinator/shards were wired in

The coordinator (vector-search engine) turned out to already exist, in a
sibling repo — [VectorSearchEngine](../../VectorSearchEngine): a stateless
Go `coordinatord` doing scatter-gather routing over 4 durable `shardd`
processes, each a WAL-backed HNSW index (cgo binding to a C++ core). This
section documents how that repo was instrumented and connected, as a record
of what was actually done — not a future contract for someone else to
implement.

**In VectorSearchEngine** (separate git history — not this repo):

1. Added `go/observability/observability.go` — the same
   `/metrics` + `/healthz` HTTP server and gRPC interceptor pattern as this
   repo's `go/internal/observability`, adapted to that module (`hnswdb`).
2. Added `go/coordinator/metrics.go` (per-shard call metrics,
   shards-queried/failed counters — wired into `server.go` and `gather.go`
   at the actual `pool.Shard(s).Insert/Delete/Search` call sites) and
   `go/shard/metrics.go` (index-size gauges, updated after every
   `Insert`/`Delete`/`Undelete`; snapshot duration/result, wired into the
   background snapshot ticker, `Close()`'s final snapshot, and the
   `Snapshot` RPC).
3. Added a `-metrics-listen` flag to both `cmd/coordinatord` (default
   `:9105`) and `cmd/shardd` (default `:9106`), and installed the gRPC
   interceptor via `grpc.UnaryInterceptor(...)` on both servers' existing
   `grpc.NewServer(...)` calls.
4. Updated `docker-compose.yml`, `Dockerfile` (`EXPOSE`), and
   `scripts/start_local_cluster.ps1` (per-shard `-metrics-listen` ports,
   printed in the startup summary) so both the Docker and local hands-on
   testing flows (see that repo's `DEPLOYMENT.md`) carry metrics.

**In this repo:**

1. `monitoring/prometheus/prometheus.yml` — added `coordinator` and `shard`
   scrape jobs (`coordinator:9105`, `shard0..3:9106`).
2. `monitoring/prometheus/alerts.yml` — added the `coordinator-and-shards`
   rule group (§5).
3. `monitoring/grafana/provisioning/dashboards/json/gateway-overview.json` —
   added the "Coordinator & shards" panel row (§4.5).
4. `docker-compose.yml` — added `shard0`-`shard3` and `coordinator` services,
   building from `${VSE_PATH}` (default `../VectorSearchEngine` — override in
   `.env` if that repo is checked out somewhere else); `gatewayd` and
   `consumer`'s `COORDINATOR_ADDR` now point at `coordinator:8000` instead of
   the earlier `host.docker.internal:50052` placeholder. Topology (4 shards,
   `dim=128`) mirrors VectorSearchEngine's own `docker-compose.yml` exactly —
   see that file's comments for why those specific values.
5. `.env` — added `VSE_PATH`, `COORDINATOR_METRICS_PORT`, `SHARD_METRICS_PORT`;
   fixed `COORDINATOR_ADDR`'s default from a guessed `localhost:50052` to
   the real `localhost:8000`.

This is a second, explicit copy of VectorSearchEngine's shard/coordinator
service definitions (not a Compose `include:` of that repo's own
`docker-compose.yml`) — see the comment above the `x-shard` anchor in
`docker-compose.yml` for why: it guarantees every service resolves on the
same `vsgw` network without relying on `include`'s cross-project network
merging behaving as hoped.

---

## 7. Single-command deploy

```bash
# Windows (PowerShell)
./deploy.ps1

# macOS/Linux
./deploy.sh
```

This is genuinely one command for the full production-shaped stack:
`kafka`, `embed-search`, `embed-ingest`, `gatewayd`, `consumer`, `coordinator`,
`shard0`-`shard3`, `prometheus`, `grafana`, `alertmanager`, `kafka-exporter`
— fourteen containers across two repos, built and started in dependency
order, with the script blocking until every healthchecked service actually
reports healthy (via `docker compose up -d --wait`), not just "container
started."

What it does, in order:

1. Checks `docker` is on `PATH`, `.env` exists, and `VSE_PATH` (the sibling
   VectorSearchEngine checkout the coordinator/shards build from) actually
   exists on disk — refuses to guess or silently skip any of these; a clear
   error beats an opaque Docker build failure partway through.
2. `docker compose build` — builds `gatewayd`, `consumer`, both embed
   images, and the coordinator/shard image (compiling the C++ HNSW core via
   CMake/Ninja, then the Go binaries via cgo — see VectorSearchEngine's own
   `Dockerfile`) from their Dockerfiles.
3. `docker compose up -d --wait --wait-timeout 300` — starts everything;
   Compose enforces the `depends_on: condition: service_healthy` chains
   from §3 (shards before the coordinator, Kafka/embed/coordinator before
   gatewayd/consumer). Default timeout is 300s because the embed image
   build (PyTorch + pre-downloading the model, see §8) and the C++/cgo
   build can genuinely take a few minutes on a cold Docker cache.
4. On success, prints every service's URL/port and the Grafana admin
   password. On failure/timeout, prints the `docker compose ps` / `docker
   compose logs <service>` commands to diagnose it — the script doesn't
   guess what went wrong.

Tear down: `./deploy.ps1 -Down` / `./deploy.sh down` (containers only,
volumes kept — Prometheus/Grafana data survives a restart). Add
`-Volumes` / `down -v` to also drop `prometheus-data`, `alertmanager-data`,
`grafana-data` (asks for confirmation first).

`docker compose up -d --build --wait` on its own is equally valid if you'd
rather not use the wrapper script — the scripts add `.env` validation, a
clear failure message, and a printed summary, nothing the Compose file
doesn't already enforce on its own.

---

## 8. Production build notes

- **Multi-stage Go builds.** `go/Dockerfile.gatewayd` and
  `go/Dockerfile.consumer` build on `golang:1.26-alpine`, then copy only the
  compiled, stripped (`-ldflags="-s -w"`) binary into an `alpine:3.20`
  runtime image running as a non-root user (uid 10001). Alpine (not
  `scratch`) specifically so the `HEALTHCHECK` can use `wget`, which ships
  with Alpine's busybox, without adding a separate healthcheck binary.
- **Model baked into the embed image at build time.**
  `python/embed_service/Dockerfile` runs
  `SentenceTransformer('all-MiniLM-L6-v2')` once during `docker build` (not
  at container startup), caching the weights into the image at
  `/app/.cache/huggingface`. This means embed-search/embed-ingest
  containers never depend on network access to Hugging Face at startup or
  restart — a real production concern, since a Hugging Face outage
  shouldn't be able to prevent your own containers from restarting.
- **Non-root everywhere.** All four custom images (`gatewayd`, `consumer`,
  and the embed image used twice) run as a non-root user.
- **Resource limits.** `docker-compose.yml` sets `deploy.resources.limits`
  per service (Compose applies these outside Swarm mode too, as of the
  Compose version this was built against). Current values are generous
  starting points, not tuned to any real load test — the embed services in
  particular (`2 CPU / 3G` each) may need adjusting up or down once you know
  real request volume and concurrency.
- **Restart policy.** Every long-running service has `restart:
  unless-stopped`, so a crashed container comes back without manual
  intervention, but `docker compose down` (or a deliberate stop) is
  respected. The coordinator/shard images use `restart: "no"` instead,
  matching VectorSearchEngine's own `docker-compose.yml` — an
  auto-restarted shard would skip the crash-recovery-via-WAL-replay path
  its tests actually exercise, which is a deliberate choice in that repo,
  not an oversight here.
- **Coordinator/shards build a C++ core via cgo.** VectorSearchEngine's
  `Dockerfile` is a 3-stage build: CMake/Ninja compiles the HNSW core
  (`libhnsw.so`), a `golang:1.25-bookworm` stage cross-links the Go binaries
  against it via cgo, and the runtime stage is `ubuntu:24.04` with the
  shared library installed via `ldconfig` (not the build-time rpath, which
  is a relocatable-but-fragile absolute path — see that Dockerfile's
  comments). Each shard gets its own named volume (`shard0-data` ..
  `shard3-data`) for its snapshot + WAL.

### Known scaling caveat: consumer replicas

`docker compose up -d --scale consumer=3` will start multiple consumer
containers (Kafka will rebalance `INGEST_TOPIC` partitions across them
under the shared `CONSUMER_GROUP` — that part works as-is), **but two things
need to change first**:

1. `consumer`'s `ports: ["${CONSUMER_METRICS_PORT}:${CONSUMER_METRICS_PORT}"]`
   binds a fixed host port — the second and third replica will fail to
   start because that host port is already taken. Remove the host-side
   number (`- "${CONSUMER_METRICS_PORT}"` with no `:`) to let Docker assign
   a random host port per replica, or drop host publishing entirely since
   Prometheus scrapes over the container network, not the host mapping.
2. Prometheus's `consumer` scrape job in `prometheus.yml` is a
   `static_configs` target of `consumer:9102` — Docker's embedded DNS
   round-robins a single hostname across replicas, so a static target only
   ever lands on one replica per scrape, not all of them. Multi-replica
   scraping needs `dns_sd_configs` or `docker_sd_configs` (the latter needs
   the Prometheus container to read the Docker socket) instead of a static
   target.

Neither is done here — single-replica consumer is the tested, supported
configuration in this pass. The path to scale it is documented rather than
silently left broken.

---

## 9. Environment variable reference

All in `.env` at the repo root, read by both Go binaries via
`godotenv.Load(".env")` and by Docker Compose automatically.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap address (local/hybrid dev value; Compose overrides to `kafka:9092` per-service). |
| `INGEST_TOPIC` | `ingest-events` | Kafka topic for ingest events. |
| `CONSUMER_GROUP` | `ingest-consumers` | Kafka consumer group id. |
| `EMBED_SEARCH_PORT` | `50051` | embed-search gRPC port. |
| `EMBED_INGEST_PORT` | `50054` | embed-ingest gRPC port. |
| `COORDINATOR_ADDR` | `localhost:8000` | Coordinator address (local/hybrid dev; Compose overrides to `coordinator:8000`). |
| `GATEWAYD_PORT` | `50053` | gatewayd gRPC port. |
| `GATEWAYD_METRICS_PORT` | `9101` | gatewayd `/metrics` + `/healthz` port. |
| `CONSUMER_METRICS_PORT` | `9102` | consumer `/metrics` + `/healthz` port. |
| `EMBED_SEARCH_METRICS_PORT` | `9103` | Host-side port mapped to embed-search's internal metrics port `9100`. |
| `EMBED_INGEST_METRICS_PORT` | `9104` | Host-side port mapped to embed-ingest's internal metrics port `9100`. |
| `VSE_PATH` | `../VectorSearchEngine` | Build context for the `coordinator`/`shard0-3` services — path to the sibling VectorSearchEngine checkout. |
| `COORDINATOR_METRICS_PORT` | `9105` | coordinator `/metrics` + `/healthz` port (both host-mapped and the value passed to `coordinatord -metrics-listen`). |
| `SHARD_METRICS_PORT` | `9106` | Internal metrics port all 4 shards listen on (not host-mapped — shards are internal-only, same as their gRPC port; Prometheus scrapes them over the `vsgw` network by container name). |
| `PROMETHEUS_PORT` | `9090` | Prometheus UI/API. |
| `GRAFANA_PORT` | `3000` | Grafana UI. |
| `ALERTMANAGER_PORT` | `9093` | Alertmanager UI/API. |
| `KAFKA_EXPORTER_PORT` | `9308` | kafka-exporter `/metrics`. |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | Grafana admin login — **change before deploying anywhere shared.** |

`EMBED_SEARCH_ADDR` / `EMBED_INGEST_ADDR` are intentionally **not** in
`.env` — they're container-only overrides set directly in
`docker-compose.yml`'s `environment:` blocks (`embed-search:${EMBED_SEARCH_PORT}`
etc.), so the root `.env` stays meaningful for local/hybrid dev without a
docker-vs-local split inside the same file. See ARCHITECTURE.md §8 for the
fallback logic.

---

## 10. Runbook — reading an incident off the dashboard

Quick reference tying the panels/alerts back to what to actually do:

- **`GatewaydHighErrorRate` fires, method = `Search`.** Check
  `vsgw_embed_requests_total{job="embed-search",result!="success"}` and
  `vsgw_coordinator_grpc_requests_total{method="/vectorsearch.coordinator.v1.VectorSearch/Search",code!="OK"}`
  — Search's errors are almost always inherited from one of its two
  downstream calls, not gatewayd itself.
- **`SearchDegradedCoverage` or `ShardCallErrors` fires.** Check
  `vsgw_coordinator_shard_client_requests_total{result="error"}` broken
  down by `shard` — this tells you immediately whether it's one shard (a
  single bad instance — check that shard's own `vsgw_shard_grpc_requests_total`
  and `docker compose logs shard<N>`) or all of them (something upstream of
  every shard, e.g. the `vsgw` network itself or a coordinator config
  problem).
- **`ShardNearCapacity` fires.** `-max-elements` is fixed at shard creation
  — there is no live resize. Either that shard needs re-provisioning with a
  larger `-max-elements` (new data dir, replay from a backfill), or traffic
  needs rebalancing — check `vsgw_coordinator_shard_client_requests_total`
  to confirm the router (`coordinator/router.go`'s FNV-1a hash) is actually
  distributing evenly and this shard isn't just getting hot-keyed.
- **`ShardSnapshotFailing` fires.** Check disk space on that shard's data
  volume first (`docker exec` in, or `docker system df -v`) — this is the
  most common cause. Not urgent in the sense of immediate data loss (the WAL
  still has everything), but `vsgw_shard_snapshot_duration_seconds` and WAL
  replay time will both keep growing until it's fixed.
- **`GatewaydKafkaPublishErrors` fires.** This means accepted `Insert`
  calls are failing to even reach Kafka — check `kafka`'s health/logs
  directly (`docker compose logs kafka`), not the consumer.
- **`ConsumerGroupLagGrowing` fires, consumer looks healthy.** Check
  `vsgw_consumer_processing_duration_seconds` p95 — if it's climbing, the
  bottleneck is embed-ingest or the coordinator, not the consumer loop
  itself; consider scaling embed-ingest (or the coordinator) before
  reaching for consumer replicas (§8's caveat).
- **`ConsumerStalled` fires.** Distinct from lag growing — this means the
  consumer has stopped fetching *at all*. Check `docker compose ps
  consumer` for a crash loop first, then `docker compose logs consumer`.
- **`EmbedHighLatencyP95` fires on one instance only.** Compare
  `vsgw_embed_requests_in_flight` for that job against the other — if it's
  pinned at 4 (the thread pool size in `server.py`), that instance is
  saturated; the fix is more replicas or a larger thread pool, not a code
  bug.

---

## 11. Files added/changed in this pass

**In this repo (vectorsearch-gateway):**

| File | Purpose |
|---|---|
| `go/internal/config/config.go` | Shared env-lookup + docker/local address fallback logic. |
| `go/internal/observability/observability.go` | `/metrics` + `/healthz` HTTP server and the gRPC server metrics interceptor. |
| `go/gateway/producer.go` | Kafka publish metrics. |
| `go/gateway/server.go` | Rate-limit-denied metric. |
| `go/main.go`, `go/gateway/consumer/main.go` | Wired in metrics server + interceptor; addressing now goes through `internal/config`. |
| `python/embed_service/server.py` | `/metrics` + `/healthz` HTTP server (stdlib, no extra framework) and embed-call instrumentation. |
| `python/embed_service/requirements.txt` | Added `prometheus_client`. |
| `go/Dockerfile.gatewayd`, `go/Dockerfile.consumer` | New — multi-stage builds for both Go binaries. |
| `python/embed_service/Dockerfile` | Non-root user, healthcheck, model baked in at build time. |
| `docker-compose.yml` | Every service (app + coordinator/shards + monitoring), health-gated `depends_on`, resource limits, container-network addressing. |
| `.env` | Added metrics/monitoring port variables, `VSE_PATH`, fixed `COORDINATOR_ADDR`. |
| `monitoring/prometheus/prometheus.yml`, `alerts.yml` | Scrape config (incl. coordinator/shards) and alert rules. |
| `monitoring/alertmanager/alertmanager.yml` | Routing + no-op receiver with commented real-receiver examples. |
| `monitoring/grafana/provisioning/**` | Datasource + dashboard auto-provisioning, one dashboard JSON. |
| `deploy.ps1`, `deploy.sh` | Single-command build/deploy/teardown wrapper. |
| `docs/MONITORING.md` | This document. |
| `docs/ARCHITECTURE.md` | Cross-linked to this doc; updated to reflect the above. |

**In the sibling VectorSearchEngine repo** (separate git history):

| File | Purpose |
|---|---|
| `go/observability/observability.go` | Same `/metrics` + `/healthz` server and gRPC interceptor pattern as this repo's, adapted to that module. |
| `go/coordinator/metrics.go` | Per-shard call metrics, shards-queried/failed counters. |
| `go/shard/metrics.go` | Index-size gauges, snapshot duration/result metrics. |
| `go/coordinator/server.go`, `go/coordinator/gather.go` | Wired `recordShardCall`/`shardsQueriedTotal`/`shardsFailedTotal` into the actual `Insert`/`Delete`/`Search` call sites. |
| `go/shard/server.go` | Wired `updateIndexGauges`/`recordSnapshot` into `Insert`/`Delete`/`Undelete`, the background snapshot ticker, `Close()`, and the `Snapshot` RPC. |
| `go/cmd/coordinatord/main.go`, `go/cmd/shardd/main.go` | Added `-metrics-listen` flag, started the metrics server, installed the gRPC interceptor. |
| `Dockerfile` | `EXPOSE`d the new metrics ports. |
| `docker-compose.yml` | Added `-metrics-listen` args to each service's `command:`. |
| `scripts/start_local_cluster.ps1` | Added per-shard `-metrics-listen` ports and printed them in the startup summary, for the non-Docker hands-on testing flow. |
