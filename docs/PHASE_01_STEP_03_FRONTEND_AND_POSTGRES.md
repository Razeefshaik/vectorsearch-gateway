# Phase 1, Step 3: Frontend Container and PostgreSQL Foundation

This is the final infrastructure step in Phase 1. It makes the frontend part of the BugBrother Compose application and adds the PostgreSQL service that Phase 2 will use for durable task state.

BugBrother remains on `master`. The vector gateway and engine remain on their BugBrother integration branches.

## Target topology

```text
Browser
  |
  | http://localhost:5173
  v
frontend (Nginx)
  |-- static React files
  `-- /api, /debug, /oauth2, /login, /logout, /health
          |
          v
      ingestion-service:8080
          |
          v
      BugBrother Kafka:29092

worker-service
  |-- BugBrother Kafka:29092
  `-- gatewayd:50053 through external network vsgw

postgres:5432
  `-- persistent task/repository/index state beginning in Phase 2
```

The browser uses a single origin: `http://localhost:5173`. Nginx serves the React application and proxies backend-owned paths to ingestion. This preserves the OAuth session cookie without browser-side cross-origin configuration.

## Change 1: Add safe local environment files

File to create:

`S:\BackendStuff\BugBrother\.env.example`

```dotenv
# GitHub OAuth application
GITHUB_CLIENT_ID=replace-me
GITHUB_CLIENT_SECRET=replace-me

# Worker LLM provider
OPENAI_API_KEY=replace-me
OPENAI_BASE_URL=https://router.huggingface.co/v1
OPENAI_MODEL=meta-llama/Llama-3.1-8B-Instruct

# Local PostgreSQL container
POSTGRES_DB=bugbrother
POSTGRES_USER=bugbrother
POSTGRES_PASSWORD=bugbrother-local-only
POSTGRES_HOST_PORT=5433
```

Copy it to `.env` and replace the credentials in `.env` only. Do not put real credentials in `.env.example`.

Add these entries to the repository's root `.gitignore`:

```gitignore
.env
frontend/node_modules/
frontend/dist/
frontend/*.tsbuildinfo
frontend/vite.config.js
frontend/vite.config.d.ts
```

The committed `vite.config.ts` is the source file. The `.js`, `.d.ts`, and `.tsbuildinfo` files are generated build output and should not be treated as source.

## Change 2: Create the frontend Docker build

File to create:

`S:\BackendStuff\BugBrother\frontend\Dockerfile`

```dockerfile
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=10 \
    CMD wget -qO- http://localhost/ >/dev/null || exit 1
```

File to create:

`S:\BackendStuff\BugBrother\frontend\.dockerignore`

```dockerignore
node_modules
dist
*.tsbuildinfo
vite.config.js
vite.config.d.ts
.git
```

This prevents host build output and `node_modules` from replacing the clean dependencies installed inside the image.

## Change 3: Add the Nginx same-origin proxy

File to create:

`S:\BackendStuff\BugBrother\frontend\nginx.conf`

```nginx
events {}

http {
    include /etc/nginx/mime.types;

    server {
        listen 80;
        server_name _;

        root /usr/share/nginx/html;
        index index.html;

        location ~ ^/(api|debug|home|health|oauth2|login|logout)(/|$) {
            proxy_pass http://ingestion-service:8080;
            proxy_http_version 1.1;

            proxy_set_header Host $http_host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $http_host;
        }

        location / {
            try_files $uri $uri/ /index.html;
        }
    }
}
```

The first location sends backend paths to ingestion. The second serves React routes and falls back to `index.html` for client-side navigation.

Do not proxy any path directly to the worker. The worker consumes Kafka commands and is not a browser-facing API.

## Change 4: Trust proxy forwarding in Spring Boot

File:

`S:\BackendStuff\BugBrother\fixhub-ingestion-service\src\main\resources\application.properties`

Add near the server configuration:

```properties
server.forward-headers-strategy=framework
```

Nginx forwards the browser-visible host and protocol. This property lets Spring use that information when it creates the GitHub OAuth callback URL.

Keep:

```properties
frontend.url=${FRONTEND_URL:http://localhost:5173}
```

The frontend URL is still the post-login and post-logout destination.

## Change 5: Add PostgreSQL to BugBrother Compose

File:

`S:\BackendStuff\BugBrother\docker-compose.yml`

Add a top-level volume between `networks` and `services`:

```yaml
volumes:
  postgres-data:
```

Add this service under `services:`:

```yaml
  postgres:
    image: postgres:17-alpine
    restart: unless-stopped
    ports:
      - "${POSTGRES_HOST_PORT:-5433}:5432"
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-bugbrother}
      POSTGRES_USER: ${POSTGRES_USER:-bugbrother}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-bugbrother-local-only}
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      - bugbrother
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s
```

The doubled dollar signs are required. `$${POSTGRES_USER}` tells Compose to leave the variable for the container shell instead of resolving it on the Windows host.

Do not add a datasource dependency to ingestion yet. Phase 2 will add Spring Data/JDBC, migrations, task tables, and the datasource connection as one coherent change.

## Change 6: Add the frontend service to BugBrother Compose

Add this service under `services:`:

```yaml
  frontend:
    build:
      context: ./frontend
    restart: unless-stopped
    ports:
      - "5173:80"
    networks:
      - bugbrother
    depends_on:
      ingestion-service:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost/ >/dev/null || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 20s
```

The frontend joins only the private BugBrother network. It has no reason to access `vsgw` or PostgreSQL.

## Change 7: Preserve host-development behavior

Do not change `frontend/vite.config.ts` during this step. Its current default is correct for running the frontend directly on Windows:

```typescript
const BACKEND_TARGET = process.env.BACKEND_URL ?? 'http://localhost:8080';
```

The two modes are now explicit:

| Mode | Frontend server | Backend route |
|---|---|---|
| Host development | Vite on `localhost:5173` | Vite proxies to `localhost:8080` |
| Docker development | Nginx published on `localhost:5173` | Nginx proxies to `ingestion-service:8080` |

Do not run the host Vite server and Docker frontend simultaneously because both use host port 5173.

## Change 8: Configure the GitHub OAuth application

In the GitHub OAuth application used for BugBrother, use these local-development values:

```text
Homepage URL:
http://localhost:5173

Authorization callback URL:
http://localhost:5173/login/oauth2/code/github
```

The callback reaches Nginx first and is then proxied to Spring Security. A callback pointing directly at `localhost:8080` creates a different browser origin and defeats the single-origin design.

## Verification 1: Validate configuration without starting anything

From BugBrother:

```powershell
docker compose config --quiet
```

It must return without output.

Inspect the service list:

```powershell
docker compose config --services
```

It must contain:

```text
kafka
postgres
ingestion-service
worker-service
frontend
```

## Verification 2: Build the React application directly

From `S:\BackendStuff\BugBrother\frontend`:

```powershell
npm ci
npm run build
```

This must finish without TypeScript or Vite errors and produce `dist`.

## Verification 3: Start the complete BugBrother stack

Keep the vector gateway stack running first. Then, from BugBrother:

```powershell
docker compose up -d --build --wait
docker compose ps --all
```

Kafka, PostgreSQL, ingestion, worker, and frontend must all be running and healthy. No service should remain in `Created`, `Starting`, or `Unhealthy` state.

## Verification 4: Check browser and proxied backend routes

From PowerShell:

```powershell
$home = Invoke-WebRequest -UseBasicParsing http://localhost:5173/ -TimeoutSec 10
$health = Invoke-WebRequest -UseBasicParsing http://localhost:5173/health -TimeoutSec 10
$me = Invoke-WebRequest -UseBasicParsing http://localhost:5173/api/me -TimeoutSec 10

Write-Output $home.StatusCode
Write-Output $health.Content
Write-Output $me.Content
```

Expected results:

- frontend root returns HTTP 200 and HTML;
- `/health` returns `Git AI Debug Ingestion Service is running`;
- `/api/me` returns JSON describing an unauthenticated user before login.

Also open this URL in a browser:

```text
http://localhost:5173
```

Refreshing a frontend route must return the SPA instead of an Nginx 404.

## Verification 5: Check the OAuth redirect URI

Run:

```powershell
curl.exe -s -D - -o NUL http://localhost:5173/oauth2/authorization/github
```

Find the `Location` response header. Its encoded `redirect_uri` must represent:

```text
http://localhost:5173/login/oauth2/code/github
```

If it contains `ingestion-service:8080`, `localhost:8080`, or container port 80, check the Nginx forwarded headers and `server.forward-headers-strategy`.

## Verification 6: Verify PostgreSQL health and persistence

```powershell
docker compose exec -T postgres pg_isready -U bugbrother -d bugbrother
```

It should report that port 5432 is accepting connections.

Restart only PostgreSQL:

```powershell
docker compose restart postgres
docker compose ps postgres
```

Wait until it is healthy again. The named `postgres-data` volume remains attached. Phase 2 will verify row persistence after migrations exist.

## Verification 7: Recheck the worker-to-gateway route

```powershell
docker compose exec -T worker-service getent hosts gatewayd
docker compose exec -T worker-service sh -c "nc -z gatewayd 50053"
```

Both commands must succeed after adding the new services.

## Verification 8: Inspect final logs

```powershell
docker compose logs --tail 100 frontend postgres ingestion-service worker-service
```

Confirm there are no recurring errors, restart loops, proxy connection failures, database initialization failures, incorrect Kafka addresses, or `UnknownHostException: gatewayd` messages.

## Completion checklist

- [ ] `.env.example` contains placeholders only and `.env` is ignored.
- [ ] Frontend Docker build uses `npm ci` and produces static files.
- [ ] Nginx serves the React SPA and proxies backend-owned paths.
- [ ] Spring honors forwarded host and protocol information.
- [ ] GitHub OAuth callback is `localhost:5173/login/oauth2/code/github`.
- [ ] PostgreSQL uses a persistent named volume and passes `pg_isready`.
- [ ] The complete BugBrother stack starts with one Compose command.
- [ ] Frontend, ingestion, worker, Kafka, and PostgreSQL are healthy.
- [ ] `/health` and `/api/me` work through frontend port 5173.
- [ ] Worker still resolves and connects to `gatewayd:50053`.
- [ ] Logs contain no recurring topology errors.

When every item passes, Phase 1 is complete. Phase 2 will add durable task status in PostgreSQL, backend-generated task IDs, Kafka publication acknowledgement, task status events, read endpoints, and frontend polling.
