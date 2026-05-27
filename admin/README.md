# VectorDB Admin Panel

A web UI for inspecting and managing a running VectorDB cluster.

- **Backend** — Go + [Echo](https://echo.labstack.com/) reverse-proxies `/api/*`
  to the VectorDB REST gateway and serves the React build.
- **Frontend** — React 18 + TypeScript + Vite, no UI framework dependencies.

## Embedding model (LM Studio / Ollama)

The **Upsert** tab can call a local embedding server and fill `values` for you.

**nomic-embed-text-v1.5** (LM Studio):

1. Load the model in LM Studio and start the local server (default port **1234**).
2. In the admin UI → **Upsert** → **Embedding model** → click **Settings** if needed.
3. Preset **LM Studio — nomic-embed-text-v1.5** sets:
   - Model: `text-embedding-nomic-embed-text-v1.5`
   - URL: `http://127.0.0.1:1234/v1`
4. Create a collection with dimension **768** (button **768 (nomic)** in the New collection dialog).
5. Paste your text → **Embed with model → fill values** → **Upsert point**.

If admin runs **inside Docker** but LM Studio runs on your Mac, use preset **LM Studio (admin in Docker → host)** (`host.docker.internal:1234`).

The browser calls `POST /embed` on the admin backend, which proxies to LM Studio (no CORS issues).

## Features

- Health pill with live latency to the gateway
- **RBAC username/password login** (`POST /v1/auth/login`) — mints a bearer token
  that is used for every subsequent call; falls back to an `x-api-key` if
  RBAC is disabled or no user has been bootstrapped yet
- Browse collections with vector counts (uses `/v1/collections/:name/stats`)
- Create / delete collections (cosine, Euclidean, dot; optional BM25 text field)
- Search panel with **dense**, **BM25**, **hybrid (RRF)**, **hybrid (weighted)**,
  and **hybrid multi-AnnRequest** modes (Milvus parity reranker controls,
  `with_payload` / `with_vector`, `output_fields`, JSON filter or expression),
  with live result inspection (click a hit → see payload + vector preview)
- **Browse tab** — cursor-paginated `POST /v1/collections/:name/scroll` for
  walking every point in a collection (filter / partition / output-fields aware)
- Admin tab:
  - Reindex, WAL compaction (with optional snapshot first), snapshot list/create/delete
  - **Delete points** by IDs and/or Milvus-style filter expression, optionally
    scoped to a partition
  - **Analyzer tester** — `POST /v1/admin/analyze` to see exactly how BM25
    tokenizes a string (either the collection's analyzer or a custom one)
- Persists API key and bearer-token session per browser

## Architecture

```
┌─────────┐    /api/*    ┌────────────┐    /v1/*     ┌──────────────────┐
│ React   │ ───────────▶ │ Go (Echo)  │ ───────────▶ │ vectordb-gateway │
│ :5173   │              │ :8090      │              │ :8080            │
│ (Vite)  │              │            │              └──────────────────┘
└─────────┘              └────────────┘
   │                           │
   │ in production builds, the React `dist/` is served by the same Go binary.
   ▼
http://127.0.0.1:8090
```

## Quick start (dev mode)

Requires Go 1.21+, Node 20+, and a running VectorDB gateway (`docker compose up`
or `cargo run -p vectordb-gateway`).

```bash
cd admin
./scripts/dev.sh
```

Open <http://127.0.0.1:5173>. The Go backend is on `:8090` and the gateway URL
is read from `VECTORDB_URL` (default `http://127.0.0.1:8080`).

If you have auth enabled, the recommended flow is to click **Log in** in the
top-right corner and authenticate with the default local-dev credentials
(`root` / `VexaDb!` — see `scripts/run-single.sh`). The minted bearer token is
stored in `localStorage` and used for every gateway call.

The `x-api-key` input still works as a fallback (e.g. for legacy keys, or when
RBAC is disabled). When both are set the bearer token wins so RBAC roles are
honoured. You can also set `VECTORDB_API_KEY` on the admin backend to inject a
default key for unauthenticated browser requests.

## Production build

```bash
cd admin
./scripts/build.sh
./backend/vectordb-admin --listen :8090
```

This produces a single binary in `backend/vectordb-admin` that:

- Reverse-proxies `/api/*` to the gateway
- Serves the static React build from `frontend/dist`
- Falls back to `index.html` for client-side routing

## Configuration

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--listen` | `ADMIN_LISTEN` | `:8090` | Bind address |
| `--upstream` | `VECTORDB_URL` | `http://127.0.0.1:8080` | VectorDB REST gateway URL |
| `--static` | `ADMIN_STATIC` | `../frontend/dist` | Path to React build (set empty to disable static serving) |
| `--api-key` | `VECTORDB_API_KEY` | _(unset)_ | Default API key forwarded to the gateway when client request omits one |

## File map

```
admin/
├── README.md
├── backend/
│   ├── go.mod
│   └── main.go              # Echo server, /api proxy, static + /config.json
├── frontend/
│   ├── package.json
│   ├── tsconfig*.json
│   ├── vite.config.ts
│   ├── index.html
│   └── src/
│       ├── main.tsx
│       ├── App.tsx          # layout + routing between tabs
│       ├── api.ts           # typed fetch client + parseCollectionInfo
│       ├── types.ts
│       ├── styles.css
│       ├── auth.ts        # bearer-token + API-key state, shared across components
│       └── components/
│           ├── HealthBar.tsx
│           ├── LoginDialog.tsx
│           ├── CollectionList.tsx
│           ├── CollectionDetail.tsx
│           ├── SearchPanel.tsx    # dense / BM25 / hybrid (RRF, weighted, multi-AnnRequest)
│           ├── BrowsePanel.tsx    # scroll iterator
│           ├── AdminPanel.tsx     # reindex, WAL, snapshots, delete-by-filter, analyzer
│           └── CreateCollectionDialog.tsx
└── scripts/
    ├── dev.sh               # run backend + Vite together
    └── build.sh             # build frontend + Go binary
```

## Notes & limitations

- The collection *Overview* tab pulls structured data from
  `GET /v1/collections/:name/stats` (dimension, metric, BM25 field,
  sparse/quantization flags, payload index count, vector count) and only
  falls back to regex-parsing the legacy debug spec for HNSW parameters
  (`m`, `ef_construction`, `ef_search`), which aren't on `/stats` yet.
- The **Browse** tab now lists every point via cursor-based `/scroll`
  (Milvus parity), so you no longer need the old zero-vector / random-
  unit-vector workaround.
- All destructive actions (`Delete collection`, `Delete snapshot`,
  `Delete points`) go through a `confirm()` prompt — there is no
  soft-delete / undo.
- The **Analyzer tester** can either reuse the collection's own analyzer
  (when it has a BM25 text field) or run an ad-hoc Milvus-style
  `analyzer_params` JSON block against a sample input.
