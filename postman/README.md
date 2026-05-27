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
3. **Login (when using `scripts/run-single.sh`).** The dev launcher sets
   `VECTORDB_ROOT_PASSWORD=VexaDb!` by default and bootstraps a `root`
   user. Run `Auth & RBAC → POST /v1/auth/login` once; the test script
   stores the returned token in `apiToken` for Bearer auth on later calls.
   Credentials in the request body: `{"username":"root","password":"VexaDb!"}`.
   If you see **404** on login, RBAC is disabled — restart the gateway with
   `VECTORDB_ROOT_PASSWORD` set. If you see **401**, the password does not
   match the bootstrapped user (try `FRESH=1 ./scripts/run-single.sh --clean`
   to reset local data).

## How auth works

* The collection has a collection-level **Bearer** auth that pulls from
  `{{apiToken}}`. After `POST /v1/auth/login` the token is cached.
* The `/health`, `/live`, `/ready`, `/metrics`, `/v1/version` and the
  login endpoint use `noauth` so they always succeed on an open cluster.
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
