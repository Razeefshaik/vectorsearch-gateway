<#
.SYNOPSIS
    Single-command build/deploy/teardown for vectorsearch-gateway.

.DESCRIPTION
    Wraps `docker compose` so there is exactly one command to bring the
    whole stack up (kafka, both embed instances, gatewayd, consumer, the
    coordinator + 4 shards from the sibling VectorSearchEngine repo,
    Prometheus, Grafana, Alertmanager, kafka-exporter) and confirm it's
    actually healthy before handing control back -- not just "containers
    started", but "healthchecks passed" (via `docker compose up --wait`,
    which blocks on each service's HEALTHCHECK where one is defined).

.PARAMETER Down
    Stop and remove all containers (add -Volumes to also drop Prometheus/
    Grafana/Alertmanager data volumes).

.PARAMETER Volumes
    Used with -Down: also remove named volumes (prometheus-data,
    alertmanager-data, grafana-data). Destructive -- confirms before running.

.PARAMETER Logs
    After a successful deploy, stream logs from every service.

.PARAMETER Timeout
    Seconds to wait for all services to report healthy. Default 300 --
    the embed services can take a while to build (PyTorch + model
    pre-download) and to load the model at first startup.

.EXAMPLE
    ./deploy.ps1
    Build and start the entire stack, wait for it to be healthy, print URLs.

.EXAMPLE
    ./deploy.ps1 -Down
    Tear everything down (containers only, volumes kept).

.EXAMPLE
    ./deploy.ps1 -Down -Volumes
    Tear everything down including monitoring data.
#>
param(
    [switch]$Down,
    [switch]$Volumes,
    [switch]$Logs,
    [int]$Timeout = 300
)

$ErrorActionPreference = "Stop"

function Assert-CommandExists($name) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        Write-Host "ERROR: '$name' is not on PATH. Install Docker Desktop and ensure it's running." -ForegroundColor Red
        exit 1
    }
}

Assert-CommandExists "docker"

if (-not (Test-Path ".env")) {
    Write-Host "ERROR: .env not found at repo root. Every service reads its config from it -- see docs/MONITORING.md and docs/ARCHITECTURE.md for the required keys." -ForegroundColor Red
    exit 1
}

# VSE_PATH (sibling VectorSearchEngine repo) is a build context for the
# coordinator/shard services -- check it exists now so the failure is a
# clear message instead of an opaque Docker build error partway through.
$vsePath = (Get-Content ".env" | Where-Object { $_ -match '^\s*VSE_PATH\s*=\s*(.*)\s*$' } | ForEach-Object { $matches[1] } | Select-Object -Last 1)
if ($vsePath -and -not (Test-Path $vsePath)) {
    Write-Host "ERROR: VSE_PATH ('$vsePath', from .env) does not exist. The coordinator/shard services build from that path -- check it out alongside this repo or fix VSE_PATH in .env." -ForegroundColor Red
    exit 1
}

if ($Down) {
    if ($Volumes) {
        Write-Host "This will delete Prometheus/Grafana/Alertmanager data volumes. Continue? [y/N]" -ForegroundColor Yellow
        $confirm = Read-Host
        if ($confirm -ne "y") { Write-Host "Aborted."; exit 0 }
        docker compose down --volumes
    } else {
        docker compose down
    }
    exit $LASTEXITCODE
}

Write-Host "==> Building images..." -ForegroundColor Cyan
docker compose build
if ($LASTEXITCODE -ne 0) { Write-Host "Build failed." -ForegroundColor Red; exit $LASTEXITCODE }

Write-Host "==> Starting services and waiting for health checks (timeout: ${Timeout}s)..." -ForegroundColor Cyan
docker compose up -d --wait --wait-timeout $Timeout
$upResult = $LASTEXITCODE

if ($upResult -ne 0) {
    Write-Host ""
    Write-Host "One or more services did not become healthy in time." -ForegroundColor Red
    Write-Host "Check status with:  docker compose ps" -ForegroundColor Yellow
    Write-Host "Check logs with:    docker compose logs <service>" -ForegroundColor Yellow
    exit $upResult
}

# Read .env for the port values to print (docker compose already resolved
# these for the containers; this just reflects them back to the operator).
$envVars = @{}
Get-Content ".env" | ForEach-Object {
    if ($_ -match '^\s*([A-Z_]+)\s*=\s*(.*)\s*$') {
        $envVars[$matches[1]] = $matches[2]
    }
}

Write-Host ""
Write-Host "==> Stack is up and healthy." -ForegroundColor Green
Write-Host ""
Write-Host "  gatewayd (gRPC)     localhost:$($envVars['GATEWAYD_PORT'])"
Write-Host "  gatewayd metrics    http://localhost:$($envVars['GATEWAYD_METRICS_PORT'])/metrics"
Write-Host "  consumer metrics    http://localhost:$($envVars['CONSUMER_METRICS_PORT'])/metrics"
Write-Host "  embed-search        localhost:$($envVars['EMBED_SEARCH_PORT'])  (metrics: $($envVars['EMBED_SEARCH_METRICS_PORT']))"
Write-Host "  embed-ingest        localhost:$($envVars['EMBED_INGEST_PORT'])  (metrics: $($envVars['EMBED_INGEST_METRICS_PORT']))"
Write-Host "  coordinator (gRPC)  localhost:8000  (metrics: $($envVars['COORDINATOR_METRICS_PORT']))"
Write-Host "  shards 0-3          internal-only (vsgw network) -- see docker compose logs shard0..shard3"
Write-Host "  Prometheus          http://localhost:$($envVars['PROMETHEUS_PORT'])"
Write-Host "  Grafana             http://localhost:$($envVars['GRAFANA_PORT'])  (admin / $($envVars['GRAFANA_ADMIN_PASSWORD']))"
Write-Host "  Alertmanager        http://localhost:$($envVars['ALERTMANAGER_PORT'])"
Write-Host "  kafka-exporter      http://localhost:$($envVars['KAFKA_EXPORTER_PORT'])/metrics"
Write-Host ""
Write-Host "  Tear down:          ./deploy.ps1 -Down"
Write-Host ""

if ($Logs) {
    docker compose logs -f
}
