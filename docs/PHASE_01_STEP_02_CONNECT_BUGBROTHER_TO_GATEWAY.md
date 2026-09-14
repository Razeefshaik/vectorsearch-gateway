# Phase 1, Step 2: Connect BugBrother to the Vector Gateway

This step gives the BugBrother worker an explicit, testable route to the vector gateway when both projects run in Docker.

Do this work manually. BugBrother remains on `master`. The gateway remains on `feature/search-client-id-isolation`.

## What is wrong now

BugBrother and the vector gateway are separate Docker Compose projects. Each project normally receives its own private Docker network.

The worker currently defaults to:

```properties
vectorsearch.gateway.host=${VECTORSEARCH_GATEWAY_HOST:localhost}
vectorsearch.gateway.port=${VECTORSEARCH_GATEWAY_PORT:50053}
```

Inside the worker container, `localhost` means the worker container itself. It does not mean the host and it does not mean the gateway container.

The correct addresses are:

| Runtime | Gateway host | Gateway port |
|---|---:|---:|
| Worker running directly on Windows | `localhost` | `50053` |
| Worker running in Docker | `gatewayd` | `50053` |

The existing property defaults are correct for host development. Docker Compose must override them for container development.

## Required startup order

The vector gateway Compose project owns the external Docker network named `vsgw`.

Start the vector gateway first:

```powershell
cd "S:\StudyResource\TechBoooo\Backend\B projs\vectorsearch-gateway"
./deploy.ps1 -Timeout 600
```

Only after that succeeds should BugBrother start. If the gateway stack is stopped and removed, Docker may also remove `vsgw`; BugBrother cannot create an external network that it does not own.

## Change 1: Declare BugBrother's two networks

File:

`S:\BackendStuff\BugBrother\docker-compose.yml`

Remove the obsolete top-level line:

```yaml
version: '3.8'
```

At the top level, before `services:`, add:

```yaml
name: bugbrother

networks:
  bugbrother:
    driver: bridge
  vsgw:
    external: true
    name: vsgw
```

Network responsibilities:

- `bugbrother` carries BugBrother's Kafka, ingestion, worker, and later frontend/database traffic.
- `vsgw` carries only calls that must cross into the vector gateway project.
- Only the worker needs both networks.

## Change 2: Give BugBrother Kafka a real readiness check

In the existing `kafka` service, add:

```yaml
    restart: unless-stopped
    networks:
      - bugbrother
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092 || exit 1"]
      interval: 15s
      timeout: 15s
      retries: 10
      start_period: 120s
```

Use port `29092` in this health check because that is BugBrother Kafka's internal broker listener. Port `9094` is the Windows host mapping and is not the address used by containers.

## Change 3: Make ingestion wait for Kafka readiness

In the existing `ingestion-service`, replace:

```yaml
    depends_on:
      - kafka
```

with:

```yaml
    depends_on:
      kafka:
        condition: service_healthy
```

Add:

```yaml
    restart: unless-stopped
    networks:
      - bugbrother
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8080/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 60s
```

Keep its existing environment values. You may convert the list syntax to mapping syntax for clarity:

```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      GITHUB_CLIENT_ID: ${GITHUB_CLIENT_ID:-YOUR_GITHUB_CLIENT_ID}
      GITHUB_CLIENT_SECRET: ${GITHUB_CLIENT_SECRET:-YOUR_GITHUB_CLIENT_SECRET}
      FRONTEND_URL: http://localhost:5173
```

`FRONTEND_URL` is a browser-visible URL, so `localhost:5173` is correct here even though the ingestion service runs in Docker.

## Change 4: Add a worker health endpoint

The worker currently has no HTTP health endpoint. Create this file:

`S:\BackendStuff\BugBrother\fixhub-worker-service\src\main\java\com\razeef\bugbrother\controllers\WorkerHealthController.java`

Write:

```java
package com.razeef.bugbrother.controllers;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WorkerHealthController {

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("BugBrother worker is running");
    }
}
```

This endpoint proves the Spring process is serving requests. It does not claim that Kafka, GitHub, the LLM, or the vector gateway is healthy; those connections are checked separately.

## Change 5: Attach the worker to both networks

In the existing `worker-service`, replace:

```yaml
    depends_on:
      - kafka
```

with:

```yaml
    depends_on:
      kafka:
        condition: service_healthy
```

Add these settings:

```yaml
    restart: unless-stopped
    networks:
      - bugbrother
      - vsgw
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8081/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 60s
```

Replace its environment list with this mapping:

```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      OPENAI_API_KEY: ${OPENAI_API_KEY:-YOUR_OPENAI_API_KEY}
      OPENAI_BASE_URL: ${OPENAI_BASE_URL:-https://router.huggingface.co/v1}
      OPENAI_MODEL: ${OPENAI_MODEL:-meta-llama/Llama-3.1-8B-Instruct}
      VECTORSEARCH_GATEWAY_HOST: gatewayd
      VECTORSEARCH_GATEWAY_PORT: "50053"
```

`gatewayd` works because it is the gateway service name on the shared `vsgw` network. Do not put `localhost` or a Windows host IP in this Docker configuration.

## Expected Compose structure after these changes

Use this only as a structural reference. Preserve the existing Kafka listener configuration and port mappings.

```yaml
name: bugbrother

networks:
  bugbrother:
    driver: bridge
  vsgw:
    external: true
    name: vsgw

services:
  kafka:
    networks: [bugbrother]
    # existing image, ports, environment
    # readiness healthcheck from Change 2

  ingestion-service:
    networks: [bugbrother]
    # existing build and port
    # Kafka readiness dependency and healthcheck

  worker-service:
    networks: [bugbrother, vsgw]
    # existing build and port
    # Kafka readiness dependency and healthcheck
    # VECTORSEARCH_GATEWAY_HOST=gatewayd
```

## Verification 1: Validate the engine correction from the previous step

From VectorSearchEngine:

```powershell
docker compose config --quiet
```

It must return without an undefined-volume error.

## Verification 2: Start and validate the vector stack

From vectorsearch-gateway:

```powershell
./deploy.ps1 -Timeout 600
docker compose ps --all
```

Every service must be running. `gatewayd`, `consumer`, both embedding services, Kafka, the coordinator, and all four shards must be healthy where health checks are defined. Nothing should remain in `Created` state.

Check the gateway directly from Windows:

```powershell
grpcurl -plaintext -import-path ./proto -proto gateway.proto -emit-defaults `
  -d '{"text":"empty index topology check","k":5,"ef":50,"allow_partial":false,"client_id":"1001"}' `
  localhost:50053 vectorsearch.gateway.v1.Gateway/Search
```

Before any repository is indexed for client `1001`, expect:

```json
{
  "results": [],
  "shardsQueried": 4,
  "shardsFailed": 0
}
```

If `grpcurl` is unavailable, install it once using the command already documented in `docs/ARCHITECTURE.md`, or defer this one command until it is installed. Do not treat a TCP-only check as proof that a search succeeds.

## Verification 3: Validate resolved BugBrother configuration

From BugBrother:

```powershell
docker compose config --quiet
```

Then inspect only the important resolved values:

```powershell
docker compose config | Select-String "KAFKA_BOOTSTRAP_SERVERS|VECTORSEARCH_GATEWAY_HOST|VECTORSEARCH_GATEWAY_PORT|vsgw"
```

Confirm:

- both Spring services use `kafka:29092`;
- the worker uses `gatewayd` and `50053`;
- the worker joins `vsgw`;
- no variable-resolution warning appears.

## Verification 4: Start BugBrother backend services

Keep the vector stack running. From BugBrother, run:

```powershell
docker compose up -d --build --wait kafka ingestion-service worker-service
docker compose ps --all
```

The three services must be running and healthy.

If the command says external network `vsgw` was not found, start the vector gateway stack first. Do not redefine `vsgw` as another private network in BugBrother.

## Verification 5: Prove container DNS and TCP routing

From BugBrother, run:

```powershell
docker compose exec -T worker-service getent hosts gatewayd
```

It must return a container IP for `gatewayd`.

Then run:

```powershell
docker compose exec -T worker-service sh -c "nc -z gatewayd 50053"
```

Exit code zero proves that the worker container can open a TCP connection to the gateway container. The direct `grpcurl` search from Verification 2 proves that the service behind that port actually implements the expected API.

## Verification 6: Check logs for address mistakes

```powershell
docker compose logs --tail 100 ingestion-service worker-service
```

There must be no error containing:

```text
localhost:9092
localhost:50053
UnknownHostException: gatewayd
Connection refused
Bootstrap broker disconnected
```

The words `localhost:5173` may appear as the frontend browser URL; that is expected.

## Completion checklist

- [ ] VectorSearchEngine Compose validates after the volume correction.
- [ ] The vector gateway stack starts completely.
- [ ] An empty-index gateway search queries four shards and reports zero failures.
- [ ] BugBrother declares a private `bugbrother` network and external `vsgw` network.
- [ ] BugBrother Kafka and ingestion use only the private network.
- [ ] The worker uses both networks.
- [ ] The worker receives `gatewayd:50053` through environment variables.
- [ ] Both Spring services wait for healthy BugBrother Kafka.
- [ ] Both Spring services expose and pass their `/health` checks.
- [ ] The worker resolves `gatewayd` and opens port `50053` from inside its container.
- [ ] Logs contain no container-to-container `localhost` address mistake.

Stop when these checks pass. The next worksheet will add the frontend container, the PostgreSQL placeholder for durable task state, and exact executable-JAR handling in both Spring Dockerfiles.
