# Full-stack deploy & test guide — vectorsearch-gateway + VectorSearchEngine + BugBrother

This document lives identically in all three repos (`vectorsearch-gateway`,
`VectorSearchEngine`, `BugBrother`) so it's discoverable regardless of which
one you open first. It covers running and testing all three systems
together: the vector search engine, its gateway, and BugBrother (the RAG
code-debugging platform that consumes the gateway).

For single-repo detail, see each repo's own docs:
- `vectorsearch-gateway/docs/ARCHITECTURE.md`, `docs/MONITORING.md`
- `VectorSearchEngine/ARCHITECTURE.md`, `DEPLOYMENT.md`
- `BugBrother/README.md`

---

## 0. Branch requirement — read this first

BugBrother's RAG feature depends on `Search` actually being scoped per
repo — `VectorSearchService.searchRelated` sets
`client_id = fnv1a64(owner/repo)` and expects only that repo's files back.

**`master` in both `vectorsearch-gateway` and `VectorSearchEngine` does
*not* forward/filter by `client_id`** — that filtering is deliberately kept
on `feature/search-client-id-isolation` only. If you point BugBrother at
`master`, `Gateway.Insert`/`Search` will work with no errors, but `Search`
will silently return neighbours from *every* indexed repo, not just the
current one — wrong results, not a visible failure.

**On Windows (PowerShell):**

```powershell
cd vectorsearch-gateway; git checkout feature/search-client-id-isolation
cd ..\VectorSearchEngine; git checkout feature/search-client-id-isolation
```

**On Linux/macOS (Bash):**

```bash
cd vectorsearch-gateway && git checkout feature/search-client-id-isolation
cd ../VectorSearchEngine && git checkout feature/search-client-id-isolation
```

Note: `deploy.ps1`/`deploy.sh` exist on this branch in `vectorsearch-gateway`
(that's where they were originally built), but **not** in
`VectorSearchEngine` — those were added to `master` afterward and never
backported. On this branch, VSE only has raw `docker compose up --build`.

---

## 1. Port map (check for conflicts before starting)

| Port | Owner |
|---|---|
| 9092 | vectorsearch-gateway's Kafka |
| 50051 / 50054 | embed-search / embed-ingest |
| 50053 | gatewayd |
| 8000 | VectorSearchEngine coordinator |
| 9090 / 3000 / 9093 / 9308 | Prometheus / Grafana / Alertmanager / kafka-exporter |
| 8080 | BugBrother `fixhub-ingestion-service` |
| 8081 | BugBrother `fixhub-worker-service` |
| 5173 | BugBrother frontend (Vite dev) |
| 2181 | BugBrother Zookeeper |
| **9092** | **BugBrother's own Kafka — collides with vectorsearch-gateway's** |

**Fix: don't run BugBrother's `zookeeper`/`kafka` containers at all.** Point
BugBrother's services at vectorsearch-gateway's already-running Kafka broker
instead — one broker happily hosts both `ingest-events` and
`code-guardian-tasks` as unrelated topics. This also matches BugBrother's
own README, which documents running the two Java services via
`./gradlew bootRun`, not via its own `docker-compose.yml`.

---

## 2. Prerequisites you need ready

- **GitHub OAuth App** (for login + committing fixes): create one at
  github.com/settings/developers, callback URL
  `http://localhost:8080/login/oauth2/code/github`. You need its client
  ID/secret.
- **OpenAI-compatible API key** for the worker service's AI calls (or point
  `OPENAI_BASE_URL`/`OPENAI_MODEL` at a compatible provider).

If you just want to test the RAG plumbing (vector engine ↔ gatewayd ↔
BugBrother) without wiring these up yet, skip to §6 — that path needs
neither.

---

## 3. Startup, in order

**A. Vector engine + gateway** (one terminal, from `vectorsearch-gateway/`):

```powershell
./deploy.ps1
```

Brings up kafka, embed-search, embed-ingest, gatewayd (`:50053`),
coordinator (`:8000`), 4 shards, and the full monitoring stack. Waits for
real health, not just "started" — see that repo's `docs/MONITORING.md §7`.

**B. BugBrother backend** (two more terminals, from `BugBrother/`):

**On Windows (PowerShell):**

```powershell
# terminal 2
cd fixhub-ingestion-service
$env:GITHUB_CLIENT_ID = "<id>"
$env:GITHUB_CLIENT_SECRET = "<secret>"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
./gradlew bootRun

# terminal 3
cd fixhub-worker-service
$env:OPENAI_API_KEY = "<key>"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:VECTORSEARCH_GATEWAY_HOST = "localhost"
$env:VECTORSEARCH_GATEWAY_PORT = "50053"
./gradlew bootRun
```

**On Linux/macOS (Bash):**

```bash
# terminal 2
cd fixhub-ingestion-service
GITHUB_CLIENT_ID=<id> GITHUB_CLIENT_SECRET=<secret> KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ./gradlew bootRun

# terminal 3
cd fixhub-worker-service
OPENAI_API_KEY=<key> KAFKA_BOOTSTRAP_SERVERS=localhost:9092 VECTORSEARCH_GATEWAY_HOST=localhost VECTORSEARCH_GATEWAY_PORT=50053 ./gradlew bootRun
```

(`VECTORSEARCH_GATEWAY_*` defaults already match gatewayd's defaults — only
set them if you changed gatewayd's port.)

**C. BugBrother frontend** (terminal 4):

**On Windows (PowerShell):**

```powershell
cd frontend; npm run dev
```

**On Linux/macOS (Bash):**

```bash
cd frontend && npm run dev
```

Opens on `:5173`, proxies `/api`, `/debug`, `/oauth2`, `/login`, `/logout`
to `:8080` (see `frontend/vite.config.js`).

---

## 4. Checks — confirm each layer before testing end-to-end

**On Windows (PowerShell):**

```powershell
# Vector engine / gateway
docker compose ps                                            # everything "healthy"
grpcurl -plaintext localhost:50053 list                       # gatewayd reflection
grpcurl -plaintext localhost:8000 list                        # coordinator reflection
curl http://localhost:9090/-/healthy                          # Prometheus
(curl http://localhost:9101/metrics -UseBasicParsing).Content | Select-String "vsgw_gatewayd_grpc"

# BugBrother
curl http://localhost:8080/health                             # ingestion-service up
curl http://localhost:5173                                     # frontend serving
```

**On Linux/macOS (Bash):**

```bash
# Vector engine / gateway
docker compose ps                                            # everything "healthy"
grpcurl -plaintext localhost:50053 list                       # gatewayd reflection
grpcurl -plaintext localhost:8000 list                        # coordinator reflection
curl http://localhost:9090/-/healthy                          # Prometheus
curl http://localhost:9101/metrics | grep vsgw_gatewayd_grpc   # gatewayd metrics flowing

# BugBrother
curl http://localhost:8080/health                             # ingestion-service up
curl http://localhost:5173                                     # frontend serving
```

If the worker service logs `Vector search unavailable ... continuing
without RAG context` on startup or on first task, gatewayd isn't reachable —
recheck §3A and `VECTORSEARCH_GATEWAY_HOST`/`VECTORSEARCH_GATEWAY_PORT`.

---

## 5. End-to-end test (full flow, via the UI)

1. Open `http://localhost:5173` → **Login with GitHub**
   (`/oauth2/authorization/github`).
2. Submit a debug task (`POST /debug/{owner}/{repo}` under the hood) with a
   real repo you own, a `userQ`, and 1+ Java files.
3. Watch terminal 2: publishes `CodeGuardianTask` to Kafka topic
   `code-guardian-tasks`, returns `202 Accepted` immediately.
4. Watch terminal 3 (`code-guardian-group` consumer): picks up the task,
   calls `VectorSearchService.searchRelated` (gRPC `Gateway.Search`,
   `client_id = fnv1a64("owner/repo")`) for related-file context, calls the
   AI, parses fixes, commits to a new `ai-fix/{uuid}` branch via
   `CommitService`, then calls `indexRepoFiles` (gRPC `Gateway.Insert`, one
   call per file) to index the repo for next time.
5. Confirm on GitHub: new branch with the committed fixes.
6. Confirm in Grafana (`http://localhost:3000`, `admin`/`admin`):
   `vsgw_gatewayd_grpc_requests_total` shows `Search` and `Insert` hits from
   this run.

---

## 6. Testing just the RAG plumbing (no GitHub/OpenAI needed)

Bypasses BugBrother entirely — proves gatewayd↔coordinator↔shard isolation
works before wiring up the Java services. Run from `vectorsearch-gateway/`:

**On Windows (PowerShell):**

```powershell
# Generate CLIENT_ID (or use any uint64 value like 12345)
$CLIENT_ID = [uint64]::Parse($(python3 -c "print(hash('testowner/testrepo') & 0xFFFFFFFFFFFFFFFF)"))

# Insert two files under the same client_id
grpcurl -plaintext -import-path ./proto -proto gateway.proto `
  -d "{`"key`":{`"client_id`":$CLIENT_ID,`"label`":1},`"text`":`"class Foo { void bar() {} }`"}" `
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

grpcurl -plaintext -import-path ./proto -proto gateway.proto `
  -d "{`"key`":{`"client_id`":$CLIENT_ID,`"label`":2},`"text`":`"class Baz { void qux() {} }`"}" `
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

# Insert one file under a DIFFERENT client_id (simulating another repo)
grpcurl -plaintext -import-path ./proto -proto gateway.proto `
  -d '{`"key`":{`"client_id`":999,`"label`":1},`"text`":`"totally unrelated content`"}' `
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

# Search scoped to the first client_id -- should return ONLY the 2 files
# above, never client_id=999's content
grpcurl -plaintext -import-path ./proto -proto gateway.proto `
  -d "{`"text`":`"Foo bar`",`"k`":5,`"ef`":50,`"allow_partial`":true,`"client_id`":$CLIENT_ID}" `
  localhost:50053 vectorsearch.gateway.v1.Gateway/Search
```

**On Linux/macOS (Bash):**

```bash
CLIENT_ID=$(python3 -c "print(hash('testowner/testrepo') & 0xFFFFFFFFFFFFFFFF)")  # or any uint64

# Insert two files under the same client_id
grpcurl -plaintext -import-path ./proto -proto gateway.proto \
  -d "{\"key\":{\"client_id\":$CLIENT_ID,\"label\":1},\"text\":\"class Foo { void bar() {} }\"}" \
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

grpcurl -plaintext -import-path ./proto -proto gateway.proto \
  -d "{\"key\":{\"client_id\":$CLIENT_ID,\"label\":2},\"text\":\"class Baz { void qux() {} }\"}" \
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

# Insert one file under a DIFFERENT client_id (simulating another repo)
grpcurl -plaintext -import-path ./proto -proto gateway.proto \
  -d '{"key":{"client_id":999,"label":1},"text":"totally unrelated content"}' \
  localhost:50053 vectorsearch.gateway.v1.Gateway/Insert

# Search scoped to the first client_id -- should return ONLY the 2 files
# above, never client_id=999's content
grpcurl -plaintext -import-path ./proto -proto gateway.proto \
  -d "{\"text\":\"Foo bar\",\"k\":5,\"ef\":50,\"allow_partial\":true,\"client_id\":$CLIENT_ID}" \
  localhost:50053 vectorsearch.gateway.v1.Gateway/Search
```

If the response ever includes `client_id: 999`, isolation is broken (or
you're accidentally on `master` — see §0).

---

## 7. Shutdown

**On Windows (PowerShell):**

```powershell
# Ctrl+C in terminals 2, 3, 4
cd vectorsearch-gateway; ./deploy.ps1 -Down    # add -Volumes to also wipe monitoring data
```

**On Linux/macOS (Bash):**

```bash
# Ctrl+C in terminals 2, 3, 4
cd vectorsearch-gateway && ./deploy.sh down    # add -v to also wipe monitoring data
```

---

## 8. Troubleshooting quick-reference

- **Worker never gets RAG context**: check §4's gatewayd checks; also
  confirm you're on `feature/search-client-id-isolation`, not `master`
  (§0).
- **`GatewayInsertRequest`/`GatewaySearchRequest` gRPC errors from the
  worker**: BugBrother's `fixhub-worker-service/src/main/proto/gateway.proto`
  copy may be stale vs. `vectorsearch-gateway/proto/gateway.proto` — diff
  them if the proto has changed since BugBrother's client stubs were
  generated.
- **Kafka connection refused from `gradlew bootRun`**: confirm
  `docker compose ps` shows `kafka` healthy in the vector engine stack, and
  you passed `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` (not `29092`, which
  is the in-container listener name).
- **OAuth redirect loop**: the callback URL registered on GitHub must
  exactly match `http://localhost:8080/login/oauth2/code/github`.
