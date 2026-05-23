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
- Browse collections with vector counts
- Create / delete collections (cosine, Euclidean, dot; optional BM25 text field)
- Search panel with **dense**, **BM25**, **hybrid (RRF)**, and **hybrid (weighted)** modes,
  optional JSON filter, and live result inspection (click a hit → see payload + vector preview)
- Admin actions: reindex, WAL compaction (with optional snapshot first), snapshot list/create/delete
- Persists API key per browser

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

If you have auth enabled, set `VECTORDB_API_KEY` once and the panel will
forward it to the gateway. You can also paste the key in the top-right input;
that is stored in `localStorage` per browser.

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
│       └── components/
│           ├── HealthBar.tsx
│           ├── CollectionList.tsx
│           ├── CollectionDetail.tsx
│           ├── SearchPanel.tsx
│           ├── AdminPanel.tsx
│           └── CreateCollectionDialog.tsx
└── scripts/
    ├── dev.sh               # run backend + Vite together
    └── build.sh             # build frontend + Go binary
```

## Notes & limitations

- The gateway returns a collection's spec as a debug-formatted string today;
  the frontend regex-parses it for the structured fields shown in *Overview*.
  When the gateway exposes a structured spec we'll switch the parser off.
- There is no "list points" endpoint in the gateway, so the panel surfaces
  data via `Search` only. Use a zero-vector + BM25 query, or a random unit
  vector, to browse a representative sample.
- All destructive actions (`Delete collection`, `Delete snapshot`) go through
  a `confirm()` prompt — there is no soft-delete / undo.
