# Phase 1, Step 1: Establish the Embedding Dimension Contract

This is the first implementation step in the BugBrother rebuild. Complete it manually before changing networking, ingestion, retrieval, or repair behavior.

## Goal

Use one explicit embedding contract throughout the BugBrother vector path:

| Setting | Required value |
|---|---|
| Embedding model | `sentence-transformers/all-MiniLM-L6-v2` |
| Embedding dimension | `384` |
| Vector shard dimension | `384` |

The current embedding service loads `all-MiniLM-L6-v2`, which produces 384 values, while the gateway-managed and standalone engine shards are started with `-dim 128`. Inserts cannot be reliable while those values disagree.

## Branch boundary

Make the changes in this worksheet only on these branches:

| Repository | Branch |
|---|---|
| BugBrother | `master` (no change required in this step) |
| vectorsearch-gateway | `feature/search-client-id-isolation` |
| VectorSearchEngine | `feature/search-client-id-isolation` |

Do not apply the BugBrother integration settings to the gateway or engine `master` branches. Their masters remain the generic vector retrieval ecosystem.

## Change 1: Define the contract in the gateway environment

File:

`vectorsearch-gateway/.env`

Add these values:

```dotenv
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
EMBEDDING_DIMENSION=384
```

These values make the selected model and its output size visible instead of leaving the model in Python and the shard dimension in Compose as unrelated constants.

## Change 2: Validate the model dimension when the embedding service starts

File:

`vectorsearch-gateway/python/embed_service/server.py`

Near the other module configuration, add:

```python
MODEL_NAME = os.environ.get(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2",
)
EXPECTED_DIMENSION = int(os.environ.get("EMBEDDING_DIMENSION", "384"))
```

Replace the hard-coded model initialization:

```python
model = SentenceTransformer("all-MiniLM-L6-v2")
```

with:

```python
model = SentenceTransformer(MODEL_NAME)
actual_dimension = model.get_sentence_embedding_dimension()

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
```

Keep the existing `import os`; it is already present. This startup check makes a future model change fail immediately with a useful error instead of producing bad vectors later in the Kafka consumer.

## Change 3: Pass the contract to both embedding-service containers

File:

`vectorsearch-gateway/docker-compose.yml`

In the `environment` section of both embedding services, keep `METRICS_PORT` and add:

```yaml
      EMBEDDING_MODEL: ${EMBEDDING_MODEL}
      EMBEDDING_DIMENSION: ${EMBEDDING_DIMENSION}
```

The resulting environment block for each embedding service should follow this shape:

```yaml
    environment:
      METRICS_PORT: <keep-the-existing-port-value>
      EMBEDDING_MODEL: ${EMBEDDING_MODEL}
      EMBEDDING_DIMENSION: ${EMBEDDING_DIMENSION}
```

Do not replace either existing metrics port with the placeholder shown above.

## Change 4: Start every gateway-managed shard with dimension 384

File:

`vectorsearch-gateway/docker-compose.yml`

For each of the four shard services, replace:

```yaml
"-dim", "128"
```

with:

```yaml
"-dim", "${EMBEDDING_DIMENSION}"
```

All four shards must resolve to the same value. Do not leave a mixture of 128 and 384-dimensional shards.

## Change 5: Isolate the new 384-dimensional shard data

File:

`vectorsearch-gateway/docker-compose.yml`

Existing shard volumes may contain indexes created with dimension 128. A 384-dimensional process must not reuse those snapshots.

Give the gateway's four BugBrother-compatible shard volumes new Compose volume keys. Use a consistent set such as:

```yaml
volumes:
  bugbrother-shard0-data:
  bugbrother-shard1-data:
  bugbrother-shard2-data:
  bugbrother-shard3-data:
```

Then update each shard service mount to reference its matching new key. Preserve the container-side mount path already used by that service. For example, if the current mount is:

```yaml
- shard0-data:<existing-container-path>
```

change only its volume key:

```yaml
- bugbrother-shard0-data:<existing-container-path>
```

Repeat for shards 1 through 3. Do not delete the old volumes; keeping them makes this change reversible and protects existing generic test data.

## Change 6: Align the engine feature branch's standalone Compose stack

File:

`VectorSearchEngine/docker-compose.yml`

For each of its four shard services, replace:

```yaml
"-dim", "128"
```

with:

```yaml
"-dim", "${EMBEDDING_DIMENSION:-384}"
```

Also rename its Compose volume keys and corresponding service mounts so this stack cannot open old 128-dimensional snapshots. A clear set is:

```yaml
volumes:
  bugbrother-engine-shard0-data:
  bugbrother-engine-shard1-data:
  bugbrother-engine-shard2-data:
  bugbrother-engine-shard3-data:
```

Keep every existing container-side mount path unchanged.

Do not change the generic `-dim 128` default in `go/cmd/shardd/main.go` during this step. The BugBrother integration should select its dimension through deployment configuration. This avoids silently changing the engine's behavior for other callers.

## Verification 1: Inspect the resolved gateway configuration

From the gateway repository, run:

```powershell
docker compose config | Select-String "EMBEDDING_MODEL|EMBEDDING_DIMENSION|-dim"
```

Confirm that:

- both embedding services receive the same model name;
- both embedding services receive dimension `384`;
- all four shard commands contain `-dim` followed by `384`.

If Compose reports an unset variable, stop and fix `.env` before starting containers.

## Verification 2: Start the stack and inspect startup logs

Use the gateway's normal deployment command:

```powershell
./deploy.ps1
```

Inspect both embedding-service logs. Each must include a line equivalent to:

```text
Embedding model ready: model=sentence-transformers/all-MiniLM-L6-v2, dimension=384
```

Also inspect all four shard logs. There must be no snapshot dimension error and no process restart loop.

## Verification 3: Measure an embedding response

Call the embedding service with the repository's existing gRPC method and a small text such as `dimension contract check`. Pipe the JSON response into PowerShell and count the returned vector values:

```powershell
$response = <existing grpcurl embedding command> | ConvertFrom-Json
$response.vector.Count
```

If the response field in the current protobuf is named differently, use that field in place of `vector`. The final count must be:

```text
384
```

Use the existing protobuf definition and README command to fill in the exact `grpcurl` service and method rather than guessing a new API.

## Verification 4: Prove one vector can travel through the full gateway path

Use the gateway's existing Insert API with:

- `client_id = 1001`;
- a temporary numeric vector label such as `1`;
- a short code fragment or text payload.

Then inspect the Kafka consumer logs. The insert must finish successfully and must not contain a dimension mismatch error.

Search using the same `client_id`, with partial results disabled:

```json
{
  "client_id": 1001,
  "allow_partial": false
}
```

Adapt that fragment to the existing request schema and include the required query and result-count fields. Confirm that:

- the inserted item is returned;
- four shards were queried;
- zero shards failed;
- no result belonging to another client is returned.

This is a transport sanity check. Full client-isolation testing is handled in a later step.

## Optional negative check

Temporarily set `EMBEDDING_DIMENSION=128` for one local embedding-service start. The service should fail during startup with the new `Embedding dimension mismatch` error. Restore `384` immediately afterward.

This proves the guard catches future configuration drift. Do not run the full stack against 384-dimensional data while the temporary value is active.

## Completion checklist

- [ ] Gateway `.env` declares the model and dimension.
- [ ] The embedding service validates its actual dimension at startup.
- [ ] Both embedding containers receive the contract.
- [ ] All four gateway-managed shards resolve to dimension 384.
- [ ] Gateway-managed 384-dimensional data uses new volume keys.
- [ ] All four standalone engine shards resolve to dimension 384 on the feature branch.
- [ ] Standalone engine 384-dimensional data uses new volume keys.
- [ ] Both embedding-service logs report dimension 384.
- [ ] A direct embedding response contains exactly 384 values.
- [ ] One insert and search completes without partial results or dimension errors.

Stop after every item passes. Record the command output and any errors. The next step will define the network and service-address contract between BugBrother, the gateway, Kafka, and the vector engine.
