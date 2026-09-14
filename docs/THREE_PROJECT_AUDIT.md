**Three-project audit — 12 September 2026**

The architecture is coherent and much of the implementation is usable. The current documented deployments do not form a reliable end-to-end BugBrother workflow. The largest problems occur between services and around indexing, task completion, and repository versions. Rewriting the HNSW engine is not the first repair to make.

This report covers the checked-out branches and BugBrother's current uncommitted files. No application source, branch, commit, or repository data was changed. Java build verification used temporary copies. Remote-tracking refs were inspected locally; no fetch was performed, so “origin” status below describes the locally recorded refs.

**Repository state**

| Repository | Checked-out state | Consequence |
|---|---|---|
| vectorsearch-gateway | feature/search-client-id-isolation, 5ace330; clean before this report; one commit ahead of its recorded origin branch | Search forwards client_id. Deployment and monitoring work is bundled with the isolation branch. |
| VectorSearchEngine | feature/search-client-id-isolation, 198ae05; clean | Search filters by client_id through the C++ layer. The branch still tracks build products, executables, logs and runtime index files, and lacks cleanup/deploy files present on master. |
| BugBrother | master, e5481d1, plus substantial local modifications and untracked files | The current frontend, authentication endpoint, initial-index endpoint/tasks/worker and error-only debugging flow are not fully represented by committed history. A fresh clone of master will not reproduce this working directory. |

The isolation branches are functional changes, not merely documentation changes. Deploying master of either vector service can defeat the repository-scoped behavior BugBrother expects. Consolidate them deliberately and pin compatible revisions across the three projects.

**What the current system actually does**

1. GitHub OAuth authenticates a user; BugBrother obtains that user's access token.
2. POST /api/repos/{owner}/{repo}/index queues IndexRepoTask.
3. IndexWorkerService fetches every Java file recursively through authenticated GitHub API requests.
4. For every file it sends Gateway.Insert with client_id = FNV-1a64(owner/repo), label = FNV-1a64(path), and source text.
5. Gateway queues that text in Kafka. Its consumer embeds it and inserts the vector into the engine.
6. POST /debug/{owner}/{repo} queues a task containing userQ and the GitHub token.
7. DebugWorkerService searches for five related files, then downloads the repository's Java files again to resolve returned labels into paths and current content.
8. The worker prompts the LLM, parses replacement files, creates an ai-fix branch, and writes each file through GitHub's contents API.
9. It attempts to index the default branch again.

The vector index stores keys and vectors, not source text or paths. Source text does pass through gateway Kafka events. The current retrieval path fetches source again from GitHub.

**What is good**

- The service boundaries make sense: BugBrother owns GitHub and debugging workflows; the gateway owns text embedding and ingestion; the engine owns numeric search and persistence.
- Search and ingestion use separate embedding instances, allowing independent resource allocation.
- The current Java gateway proto matches the gateway wire schema after ignoring comments and language-specific options. There is no confirmed current Java/Go gateway schema mismatch.
- client_id is forwarded from Java through gateway and coordinator to shard SearchFiltered. The C++ search excludes foreign-client results while allowing traversal through graph nodes belonging to other clients. This is a better implementation than taking global top-k and discarding foreign results afterward. See [include/hnsw.hpp](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/include/hnsw.hpp:542>).
- Composite keys allow the same label under different clients. Existing engine tests cover isolation, duplicates, routing, shard failure, recovery and snapshot/write interaction.
- The coordinator asks every shard for full k and merges distances. It exposes failed-shard counts.
- WAL records have checksums, writes call Sync, torn tails are truncated on reopening, and snapshots coordinate with writers.
- Gateway/engine metrics, dashboards and alert rules exist. They give a useful base for diagnosing the pipeline.
- BugBrother's new explicit indexing action fixes the old first-use problem where retrieval depended on indexing after a previous successful fix.
- GitHub reads use the user's bearer token, and the current OAuth registration requests repo scope. This is the mechanism for private repository access.

**Critical deployment and workflow defects**

**1. Embedding dimension mismatch — confirmed configuration blocker.**

The Python service loads all-MiniLM-L6-v2, which produces 384-dimensional vectors. Both Compose topologies retain 128-dimensional shard configuration. The shard correctly rejects those vectors with InvalidArgument. The gateway consumer treats that error as permanent and commits the Kafka offset, dropping the insertion.

BugBrother sets allow_partial=true. If all four shards reject a 384-dimensional query, the coordinator can return an empty successful response with four failed shards. BugBrother ignores that count and reports no related files. Thus the UI can accept indexing and debugging while the actual numeric operations fail.

Evidence: [python/embed_service/server.py](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/python/embed_service/server.py:46>), [docker-compose.yml](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/docker-compose.yml:158>), [go/shard/server.go](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/go/shard/server.go:156>), [go/coordinator/gather.go](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/go/coordinator/gather.go>). Model specification: [official model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).

Fix: configure one embedding dimension/model version throughout, use fresh versioned indexes or a controlled rebuild, and validate dimension compatibility at startup. Merely changing a flag does not convert existing snapshots: durable.Open loads their stored configuration.

**2. Kafka addressing contradicts the host-run instructions — confirmed configuration defect.**

Gateway Kafka advertises kafka:9092, a Docker-network hostname. DEPLOY.md instructs host Java services to connect to localhost:9092. Bootstrap can work, but subsequent broker metadata directs the host client to kafka:9092, which is not normally resolvable on the host.

BugBrother's current Compose file instead has a separate broker published on 9094 with distinct host/container listeners. The deployment documentation still describes the old Zookeeper/9092 collision.

Fix: choose and document one topology. Either share a broker with correct internal/external listeners, or use the separate BugBrother broker consistently. For shared-container deployment, place participating services on a common network.

Evidence: [docker-compose.yml](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/docker-compose.yml:49>), [docker-compose.yml](<S:/BackendStuff/BugBrother/docker-compose.yml:14>).

**3. BugBrother's container worker cannot reach the gateway as configured.**

Its gateway host defaults to localhost. In the worker container that means the worker container itself. BugBrother Compose supplies neither VECTORSEARCH_GATEWAY_HOST nor a shared gateway network.

Fix: explicitly configure the gateway address and network for container runs, separately from host runs. See [docker-compose.yml](<S:/BackendStuff/BugBrother/docker-compose.yml:36>) and [fixhub-worker-service/src/main/java/com/razeef/bugbrother/config/VectorSearchConfig.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/config/VectorSearchConfig.java>).

**4. Both BugBrother Dockerfiles have an ambiguous JAR copy — confirmed by local builds.**

Both builds produce an executable .jar and a -plain.jar. Both Dockerfiles COPY /app/build/libs/*.jar app.jar. The wildcard therefore matches two sources but names a single-file destination.

Fix: build/copy only the executable bootJar, give it an explicit filename, or disable the plain JAR. Docker builds were not run because the daemon is unavailable; the two matching build artifacts were observed directly.

Evidence: [fixhub-ingestion-service/Dockerfile](<S:/BackendStuff/BugBrother/fixhub-ingestion-service/Dockerfile:13>), [fixhub-worker-service/Dockerfile](<S:/BackendStuff/BugBrother/fixhub-worker-service/Dockerfile:13>).

**5. Host gateway configuration still points at the wrong coordinator.**

Root .env says localhost:8000, while go/.env says localhost:50052. Running go run from go/ loads the latter because godotenv.Load uses the current directory.

Fix: remove duplicated configuration and resolve one explicit config path, with a startup log of non-secret effective addresses. See [go/.env](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/.env:5>).

**6. Re-indexing does not update existing embeddings — confirmed.**

File identity depends only on repository and path. Modified content gets the same key. The engine rejects duplicate keys; the consumer treats AlreadyExists as success. The first successfully stored embedding persists indefinitely for that path.

Deleted/renamed files also lack reconciliation, and Gateway.Delete is declared but not implemented. Soft-delete alone would not implement replacement: the engine still reserves the existing key.

Fix: introduce versioned index generations or real upsert semantics, plus a manifest of active files/chunks and deletion reconciliation.

Evidence: [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/VectorSearchService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/VectorSearchService.java:101>), [go/gateway/consumer/main.go](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/gateway/consumer/main.go:172>), [include/hnsw.hpp](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/include/hnsw.hpp:215>).

**7. Post-fix indexing targets the wrong revision.**

Fixes are written to ai-fix/{uuid}. GitHubService reads without ref, so the subsequent indexRepoFiles call fetches the default branch again. It cannot reflect the unmerged fix branch as claimed by the docs.

Fix: choose explicit semantics. Keep the default-branch index until merge, or create a separate index generation for the fix branch. Carry the repository's immutable commit SHA through fetch, retrieval, generation and commit.

Evidence: [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/DebugWorkerService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/DebugWorkerService.java:62>), [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/GitHubService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/GitHubService.java>).

**8. Commits assume the branch is named main.**

getMasterBranchSha is misleadingly named and hardcodes heads/main. A repository with master or another default branch fails. If default and main both exist but differ, fetched source and branch base can disagree.

Fix: resolve repository metadata, select a base branch, pin its SHA, and use that same revision throughout. See [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java:57>).

**9. Failures can be reported as successful fixes.**

createFixBranchAndCommitWithLogging catches per-file failures and GitHub API errors, then returns void. DebugWorkerService prints success and re-indexes regardless. A branch may be empty, partially updated, or never created. Metadata lookup also treats every error as “file doesn't exist,” including permission failures and outages.

Fix: return a structured commit result, only interpret 404 as absence, propagate failed writes, and record success only after all intended changes are confirmed. Prefer an atomic multi-file commit.

Evidence: [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java:96>), [fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/services/CommitService.java:151>).

**10. “Indexing finished” does not mean searchable.**

There are two asynchronous queues. IndexWorkerService can finish submitting Gateway.Insert calls before embeddings reach any shard. It also prints completion when indexRepoFiles swallowed fetch errors or skipped failed inserts. The UI has no index-readiness check.

Fix: track a job/index generation, expected and completed chunk counts, failures and final searchable status. Permit error submission against a known-ready generation or explicitly wait for it.

**Reliability and retrieval gaps**

**11. Gateway transient retries can lose messages.**

processEvent returns false on a transient failure, but the loop immediately FetchMessages the next message. A later successful CommitMessages on the same partition commits beyond the failed record. “Do not commit this record” does not arrange a retry while the reader keeps advancing. This follows the library's documented highest-offset semantics: [kafka-go](https://github.com/segmentio/kafka-go).

Fix: retry the current message with bounded backoff before advancing that partition, or use a durable retry/dead-letter path with explicit handling. Add a regression test with failure at offset N followed by success at N+1. See [go/gateway/consumer/main.go](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/gateway/consumer/main.go:115>).

**12. BugBrother lacks durable task outcomes and reliable acceptance.**

Controllers return 202 without observing the Kafka send future. DebugWorkerService catches failures and returns normally, so Kafka's listener error handling cannot retry them. There is no backend task ID/status/branch URL. The frontend's recent submissions are browser localStorage entries, not execution history.

Fix: broker-confirmed acceptance, durable task records, status/error endpoints, bounded retries, dead-letter handling and idempotency keys. A retry must not generate another random fix branch for the same completed task.

**13. Bulk indexing is best-effort and shares a restrictive search budget.**

Gateway uses one limiter for Insert and Search with capacity 10/refill 1 per second. Depending on publish latency, fast batches can exceed it. BugBrother skips rejected files instead of retrying. Interactive search also consumes the same bucket.

Fix: separate read/ingestion budgets, backoff on ResourceExhausted, and report per-file outcomes. The token bucket itself checks tokens > 0 instead of >= 1, allowing fractional-token requests; correct this as well.

Evidence: [go/main.go](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/main.go:60>), [go/ratelimiter/bucket.go](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/ratelimiter/bucket.go>).

**14. Retrieval loses code coverage and repeatedly downloads the repository.**

Only .java files are indexed. Whole files are sent to a short-text model, whose default truncation is 256 word pieces. Code near the end of a file can be absent from its embedding. Every nonempty search downloads all Java files to resolve a handful of labels.

Fix: chunk by methods/classes with path and line metadata; store label-to-path/SHA mappings; fetch only selected immutable blobs. Add exact filename/class/stack-trace matching alongside vector ranking, deduplicate chunks into files, and budget prompt size. The model truncation is documented in the [model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).

**15. Retrieval failure and incompleteness are collapsed into “nothing found.”**

The worker ignores shards_failed, distances and returned client_id. It can generate changes from incomplete or weak matches. Gateway outages now stop the error-only flow; comments saying “continue without RAG” describe the old architecture.

Fix: distinguish unavailable, partially searched, not indexed and no relevant results. Validate each result's client_id defensively. Require an explicit policy before generating fixes from partial coverage.

**16. External calls lack explicit workflow deadlines.**

Java gRPC stubs have no per-call deadline; GitHub WebClient calls block without explicit timeouts; gateway consumer calls use context.Background. A hung dependency can occupy processing indefinitely.

Fix: bounded per-hop timeouts and a task-level deadline, with distinct retryable and permanent error outcomes.

**17. AI output is accepted without a validation stage.**

Returned paths/content are passed to CommitService without an allowlist, diff constraints, syntax/compile checks or tests. The system prompt asks for broad improvements and a replacement for every selected file, including unchanged files. This encourages unrelated edits.

The prompt uses “==== File:” while the primary parser expects “==== File ”. Its fallback does recognize File: Java headers, so this discrepancy is maintenance fragility rather than a demonstrated total parsing failure.

Fix: one structured response schema, validate repository-relative paths and allowed changes, compare against the pinned base, avoid no-op commits, and run relevant checks in an isolated execution environment before declaring a fix successful.

**Private-repository handling**

There are two different IDs: the GitHub OAuth app client ID identifies the application during login; vector client_id is a numeric repository namespace. Neither the repo hash nor the vector filter proves authorization.

Authenticated GitHub reads are a useful boundary: a foreign search result does not directly return foreign source text to BugBrother, because it re-fetches the requested repository using the user's token. However, anyone who can reach the plaintext gateway can claim an arbitrary client_id and insert/query that namespace. Its coordinator port is also published.

Required work before shared use:

- Authenticate internal callers and bind permitted repository namespaces to trusted identity. Restrict gateway/coordinator exposure and use protected transport.
- Verify requested repository access before expensive task processing; use stable GitHub repository IDs rather than case-sensitive owner/name strings.
- Avoid long-lived raw OAuth tokens in Kafka task payloads; use a protected credential reference where practical.
- Restore CSRF protection for cookie-authenticated mutations. CORS configuration alone is not CSRF protection.
- Remove raw AI/source logging from FixedfileParser. Define retention and access for Kafka source-text events, logs and credentials.

Evidence: [fixhub-ingestion-service/src/main/java/com/razeef/bugbrother/config/Oauth.java](<S:/BackendStuff/BugBrother/fixhub-ingestion-service/src/main/java/com/razeef/bugbrother/config/Oauth.java:37>), [fixhub-worker-service/src/main/java/com/razeef/bugbrother/parsers/FixedfileParser.java](<S:/BackendStuff/BugBrother/fixhub-worker-service/src/main/java/com/razeef/bugbrother/parsers/FixedfileParser.java:23>), [go/main.go](<S:/StudyResource/TechBoooo/Backend/B projs/vectorsearch-gateway/go/main.go:71>).

**Engine correctness and operational limits**

Existing engine tests passed, but they do not establish every durability/concurrency guarantee claimed by the documentation. These additional findings come from source inspection, not fault-injection reproduction:

- durable.Add logs before checking whether the in-memory insertion will fail. An insertion rejected because the index is full remains in the WAL. A crash before the next successful snapshot causes replay to encounter the same full-index error and Open to fail. Validate/reserve mutation capacity before committing a replayable operation. See [go/durable/durable.go](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/go/durable/durable.go:140>).
- C++ duplicate detection and key registration occur in separate critical sections. Two simultaneous inserts of the same key can both pass the duplicate check. Reserve keys atomically; serialize conflicting operations. See [include/hnsw.hpp](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/include/hnsw.hpp:215>).
- Snapshot save uses ofstream without an explicit durable file sync; Go renames the snapshot and rotates away the WAL without syncing the snapshot/directory boundary. The documented power-loss guarantee is stronger than the implementation supports. Add platform-aware durable snapshot replacement and power-loss-oriented tests. See [include/hnsw.hpp](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/include/hnsw.hpp:381>) and [go/durable/durable.go](<S:/StudyResource/TechBoooo/Backend/B projs/VectorSearchEngine/go/durable/durable.go:202>).
- Snapshot rotation closes the active WAL before all failure-prone operations finish. A rename/reopen error can leave d.w closed. An abandoned .new file can block later wal.Create calls because creation is exclusive. Recovery from interrupted/failed rotation needs explicit handling.
- Concurrent delete/search reads and writes of deleted_ are not protected by the same lock or atomic access. Add C++ ThreadSanitizer coverage for deletes alongside searches.
- SearchFiltered can traverse much of the shared graph when the requested client has fewer than ef eligible vectors. Benchmark sparse tenants and absent clients; global ANN benchmarks do not measure this cost.
- Shard membership is static hash-modulo routing. Changing shard count/order requires migration; there is no replication or automatic rebalancing.
- Capacity is preallocated and tombstones do not reclaim slots. Moving from 128 to 384 dimensions also needs a memory-budget review rather than keeping the 1 GiB limit unquestioned.

**Deployment/documentation cleanup**

- Gateway.Delete returns Unimplemented despite existing in the public proto.
- Gateway does not register gRPC reflection, so the DEPLOY.md command that lists services without a proto is wrong.
- /healthz confirms liveness, not full readiness. Python starts its health HTTP server before model loading completes.
- Kafka has no explicit persistent data volume in either Compose file; container recreation is not a reliable queue-retention strategy.
- deploy.ps1 -Down -Volumes warns only about monitoring data, but down --volumes also deletes shard volumes. Correct the warning and document the full data impact.
- Engine documents say metrics are absent even though code implements them; older examples omit client_id and therefore search client 0 on the isolation branch.
- Gateway hybrid instructions run unrestricted docker compose up, which now starts gatewayd itself before suggesting a second host gatewayd on the same port.
- The three DEPLOY documents are not identical; the engine feature branch does not even contain DEPLOY.md.
- Alertmanager deliberately uses a null receiver, so alerts are visible but no notification is sent.
- Worker default AI base URL ends in /v1, while the cached Spring AI 1.0.0-M4 source appends /v1/chat/completions. This appears to form /v1/v1/chat/completions and needs an endpoint-construction/provider smoke test. The actual configured provider/model availability was not tested.
- BugBrother's root Gradle project does not include the two services. Root build success would not validate them; document separate builds or make a real multi-project build.
- Consolidate branch changes, stop tracking generated binaries/data, use explicit config examples, and add a cross-project compatibility check in CI.

**Verification performed**

| Check | Result |
|---|---|
| Gateway go test ./... | Passed; business handlers and consumer have no tests, limiter tests were cached. |
| Engine go test ./... | Passed after selecting CLion's installed 64-bit MinGW compiler and DLL path. Coordinator, shard and durability tests executed; WAL result was cached. The initially selected C:/MinGW compiler could not build 64-bit cgo. |
| BugBrother ingestion offline Gradle build | Passed from a temporary source copy; test NO-SOURCE. |
| BugBrother worker offline Gradle build, including proto generation | Passed from a temporary source copy; test NO-SOURCE. |
| Frontend tsc --noEmit | Passed; no browser or production bundling check performed. |
| Gateway vs Java gateway proto | Wire schema matches after removing comments/options. |
| Gateway/BugBrother Compose config validation | Passed; BugBrother emits an obsolete version-field warning. This does not validate service connectivity or Dockerfile builds. |
| Live Docker/GitHub/LLM workflow | Not run. Docker Desktop's Linux daemon pipe was absent. No private repository was sent to an LLM and no GitHub branch was created. |

**Recommended repair order and acceptance criteria**

1. Preserve and consolidate current branch/local work into reproducible revisions. Pin one compatible set of commits.
2. Fix dimensions, broker listeners, worker networking, host config and Docker JAR selection. Acceptance: clean deployment can ingest two synthetic repositories and return only each repository's own results with zero failed shards.
3. Add versioned indexing/upsert/deletion semantics and true readiness. Acceptance: editing a file changes its searchable embedding; deleted files disappear; debugging cannot race incomplete indexing.
4. Repair retry/ack handling and durable task status. Acceptance: dependency outages produce retries or visible failed states, never a false successful task.
5. Pin repository SHA and repair branch/commit outcomes. Acceptance: both main and master repositories work; all changes belong to the selected revision; the returned task identifies its actual branch and commit.
6. Add output validation and relevant code checks. Acceptance: unexpected paths, malformed output, no-op replacements and failing fixes are surfaced explicitly.
7. Enforce private-repository authorization across service boundaries and remove credential/source logging.
8. Add engine fault/concurrency tests, fix the durability issues, then tune retrieval quality and performance with representative code/error datasets.

The best next milestone is a small, repeatable end-to-end test with one public fixture and one authorized private fixture. It should cover first indexing, error retrieval, a changed file, a deleted file, an unavailable shard, an LLM failure and a rejected GitHub write. Production-readiness claims should wait until those behaviors are observable and reproducible.

