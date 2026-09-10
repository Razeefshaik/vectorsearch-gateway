#!/usr/bin/env bash
# Single-command build/deploy/teardown for vectorsearch-gateway.
# Bash equivalent of deploy.ps1 -- see that file's header comment for the
# full explanation of what this does and why. Usage:
#   ./deploy.sh              build + start + wait for health, print URLs
#   ./deploy.sh down         stop and remove all containers
#   ./deploy.sh down -v      also remove monitoring data volumes
#   ./deploy.sh --logs       after a successful deploy, stream logs
set -euo pipefail

TIMEOUT="${DEPLOY_TIMEOUT:-300}"
FOLLOW_LOGS=false

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: docker is not on PATH. Install Docker and ensure it's running." >&2
    exit 1
fi

if [[ ! -f .env ]]; then
    echo "ERROR: .env not found at repo root. Every service reads its config from it -- see docs/MONITORING.md and docs/ARCHITECTURE.md for the required keys." >&2
    exit 1
fi

# VSE_PATH (sibling VectorSearchEngine repo) is a build context for the
# coordinator/shard services -- check it exists now so the failure is a
# clear message instead of an opaque Docker build error partway through.
VSE_PATH_CHECK="$(grep -E '^\s*VSE_PATH\s*=' .env | tail -n1 | cut -d= -f2-)"
if [[ -n "$VSE_PATH_CHECK" && ! -d "$VSE_PATH_CHECK" ]]; then
    echo "ERROR: VSE_PATH ('$VSE_PATH_CHECK', from .env) does not exist. The coordinator/shard services build from that path -- check it out alongside this repo or fix VSE_PATH in .env." >&2
    exit 1
fi

if [[ "${1:-}" == "down" ]]; then
    if [[ "${2:-}" == "-v" ]]; then
        read -r -p "This will delete Prometheus/Grafana/Alertmanager data volumes. Continue? [y/N] " confirm
        [[ "$confirm" == "y" ]] || { echo "Aborted."; exit 0; }
        docker compose down --volumes
    else
        docker compose down
    fi
    exit $?
fi

for arg in "$@"; do
    [[ "$arg" == "--logs" ]] && FOLLOW_LOGS=true
done

echo "==> Building images..."
docker compose build

echo "==> Starting services and waiting for health checks (timeout: ${TIMEOUT}s)..."
if ! docker compose up -d --wait --wait-timeout "$TIMEOUT"; then
    echo
    echo "One or more services did not become healthy in time." >&2
    echo "Check status with:  docker compose ps" >&2
    echo "Check logs with:    docker compose logs <service>" >&2
    exit 1
fi

# shellcheck disable=SC1091
set -a; source .env; set +a

echo
echo "==> Stack is up and healthy."
echo
echo "  gatewayd (gRPC)     localhost:${GATEWAYD_PORT}"
echo "  gatewayd metrics    http://localhost:${GATEWAYD_METRICS_PORT}/metrics"
echo "  consumer metrics    http://localhost:${CONSUMER_METRICS_PORT}/metrics"
echo "  embed-search        localhost:${EMBED_SEARCH_PORT}  (metrics: ${EMBED_SEARCH_METRICS_PORT})"
echo "  embed-ingest        localhost:${EMBED_INGEST_PORT}  (metrics: ${EMBED_INGEST_METRICS_PORT})"
echo "  coordinator (gRPC)  localhost:8000  (metrics: ${COORDINATOR_METRICS_PORT})"
echo "  shards 0-3          internal-only (vsgw network) -- see docker compose logs shard0..shard3"
echo "  Prometheus          http://localhost:${PROMETHEUS_PORT}"
echo "  Grafana             http://localhost:${GRAFANA_PORT}  (admin / ${GRAFANA_ADMIN_PASSWORD})"
echo "  Alertmanager        http://localhost:${ALERTMANAGER_PORT}"
echo "  kafka-exporter      http://localhost:${KAFKA_EXPORTER_PORT}/metrics"
echo
echo "  Tear down:          ./deploy.sh down"
echo

$FOLLOW_LOGS && docker compose logs -f

exit 0
