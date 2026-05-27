# VexaDb Postman collection

This folder contains a ready-to-import Postman collection that covers every
REST endpoint exposed by the `vectordb-gateway` service.

| File | Description |
| --- | --- |
| `VexaDb-Gateway.postman_collection.json` | All 80+ routes, grouped by folder, with realistic example bodies. |
| `VexaDb-Local.postman_environment.json`  | A starter environment that targets `http://127.0.0.1:8080`. |

## Quick start

1. **Start the gateway.** The collection assumes the gateway is reachable at
   `http://127.0.0.1:8080`. Change `baseUrl` in the environment if you bind
   it elsewhere (`vectordb-gateway --http 0.0.0.0:18080 ...`).
2. **Import** both JSON files in Postman (`File → Import…`). Pick the
   `VexaDb Local` environment in the top-right selector.
3. **Authenticate.** Two options:

   **a. Bearer token via login** — run `Auth & RBAC → POST /v1/auth/login`
   with `{"username":"root","password":"VexaDb!"}`. The test script stores
   the returned token in `apiToken` for Bearer auth on later requests.
   404 here means RBAC is disabled, 401 means the password is wrong (try
   `FRESH=1 ./scripts/run-single.sh --clean` to reset).

   **b. API key (legacy superuser)** — `scripts/run-single.sh` prints a
   dev API key on every launch. Set the collection variable `apiToken`
   to that key and switch the auth type on the collection root from
   `Bearer Token` to `API Key` (header name `x-api-key`). Or run
   `scripts/bootstrap-rbac.sh` to print both credentials again.

## How auth works

* `scripts/run-single.sh` enables RBAC + creates a `root` user (password
  `VexaDb!`) and also writes a persistent dev API key to
  `run-data/single/.api-key`. **Both** auth flows work:
  * **Bearer token** — `POST /v1/auth/login`, then every request uses
    `Authorization: Bearer {{apiToken}}` (the login test script populates
    `apiToken` automatically).
  * **API key (legacy superuser)** — set `x-api-key: <key>` on any request
    and you skip the login dance entirely. Useful for scripts and tests.
* The `/health`, `/live`, `/ready`, `/metrics`, `/v1/version` and the
  login endpoint use `noauth` (HTTP-level), but on RBAC-on clusters the
  gateway still needs an upstream-authorized gRPC channel — that's why
  the script ships a persistent API key.
* A pre-request script automatically injects `X-Database: {{dbName}}` on
  every request so you can swap databases by editing one variable.

## Folder layout

```
Probes & Version          GET /health, /live, /ready, /metrics, /v1/version
Auth & RBAC               login, tokens, users, roles, privilege-groups, rbac/backup|restore
Databases                 v1/databases CRUD + properties
Collections               create / describe / drop / rename / properties / stats / aliases
Aliases                   v1/aliases CRUD
Partitions                create / list / has / drop / stats
Indexes                   HNSW & scalar create, describe, drop, properties
Load · Flush · Compact    load / release / load-state / flush / compact / segments
Vector — Insert/Upsert    /upsert, /bulk (incl. sparse vectors and partitions)
Vector — Search/Hybrid    /search, /hybrid-search (RRF + Weighted), /query, /scroll
Vector — Delete/Get       DELETE by id / filter+partition, GET by id
Analyzer                  /v1/admin/analyze (BM25 tokenizer)
Resource groups & Replicas  v1/resource-groups CRUD, transfer-replica
Snapshots                 v1/snapshots CRUD
Admin & Cluster           cluster, compact-wal, rebalance
```

## Editing the working set

The collection ships with these collection variables (overridable per
environment). Update them once and every request follows along:

| Variable | Default | Used by |
| --- | --- | --- |
| `baseUrl` | `http://127.0.0.1:8080` | every request |
| `dbName`  | `default`        | `X-Database` header |
| `collectionName` | `docs` | almost every vector/management route |
| `partitionName`  | `hot`  | partition routes + scoped insert/delete |
| `fieldName`      | `color` | index + filter examples |
| `pointId`        | `doc-001` | `GET /collections/:name/points/:id` |
| `aliasName`      | `docs_v1` | alias routes |
| `roleName` / `userName` / `privilegeGroup` | RBAC examples |
| `resourceGroup`  | `hot_rg` | resource-group + transfer-replica |
| `apiToken`       | (filled by login) | collection-level Bearer auth |
| `tokenId`        | (filled by login) | revoke token |
| `compactionId`, `snapshotId` | (filled by their POST handlers) | follow-up GET/DELETE |

## Tips

* `POST /collections/:name/compact` and `POST /snapshots` populate
  `compactionId` and `snapshotId` automatically via test scripts, so the
  follow-up `GET /v1/compactions/:id` and `DELETE /v1/snapshots/:id`
  requests work without manual copy/paste.
* The hybrid-search examples include both an RRF and a Weighted reranker
  payload. Swap the `reranker.kind` to `function` and supply
  `reranker.function = { ... }` to exercise the FunctionReranker stub.
* `POST /v1/admin/analyze` accepts either an ad-hoc `analyzer_params`
  object or a `collection` + `field` pair (it then reuses that field's
  BM25 configuration).
