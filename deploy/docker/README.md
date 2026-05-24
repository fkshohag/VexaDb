# Docker Compose — single-node VectorDB + admin panel

One Compose file, one command. Use this when you want VectorDB **and** the admin panel running in containers, with the database hidden behind admin so testers never need to handle API keys directly.

## Topology

```
                 ┌─────────────── docker network ────────────────┐
                 │                                               │
                 │  server (all_in_one, RF=1, 1 shard)            │
                 │     :6334  ── internal ──┐                    │
                 │                          ↓                    │
                 │  gateway (HTTP/JSON, API-key auth)             │
                 │     :8080  ── internal ──┐                    │
                 │                          ↓                    │
                 │  admin (web UI + API)                          │
                 │     :8090                                      │
                 │                                               │
                 └────────────────────────┬──────────────────────┘
                                          │
                                          ▼
                              host:${ADMIN_PORT:-8090}
                                  ← only public service
```

- **The database is never directly published.** The gateway and the data node only talk over the internal docker network.
- **The admin panel is the single public surface.** It serves the React UI, proxies `/v1/*` to the gateway, transparently injecting the API key.
- **API key lives only on the box** (in `.env`). Testers don't need it; they just open the admin URL.

## Quick start

```bash
cd deploy/docker

cp .env.example .env
echo "VECTORDB_API_KEY=$(openssl rand -hex 32)" >> .env

docker compose up -d --build
```

First `up` builds three images (`vectordb-server`, `vectordb-gateway` from the same image, `vectordb-admin` from `admin/Dockerfile`) — ~5–10 min on a fresh box. After that, restarts are seconds.

Open the panel:

```
http://<your-server-ip>:8090
```

Smoke test from the host (admin is the only thing reachable):

```bash
curl -sS http://127.0.0.1:8090/config.json
curl -sS http://127.0.0.1:8090/api/v1/collections
```

If you want to talk to the gateway directly for scripting / load tests, do it _from inside_ the docker network:

```bash
docker compose exec admin sh -c \
  'curl -sS -H "x-api-key: $VECTORDB_API_KEY" http://gateway:8080/v1/admin/cluster' | jq
```

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Three services: `server` (data), `gateway` (HTTP/JSON, internal), `admin` (web UI, **published**). |
| `node.toml` | Server config — single shard, `all_in_one` role, `/data` for WAL + snapshots. |
| `.env.example` | `VECTORDB_API_KEY`, optional `ADMIN_PORT`, embedding server overrides, Caddy vars. |
| `docker-compose.caddy.yml` | _Optional_ overlay: Caddy reverse proxy + auto Let's Encrypt in front of admin. |
| `Caddyfile` | _Optional_ — also has a commented basic-auth block to lock down the admin panel. |

## Public HTTPS (Caddy + Let's Encrypt)

Point your DNS at the server, then:

```bash
PUBLIC_HOSTNAME=db.example.com ACME_EMAIL=you@example.com \
  docker compose -f docker-compose.yml -f docker-compose.caddy.yml up -d --build
```

After that:

- `https://db.example.com` → admin panel
- Caddy auto-renews the cert
- Admin's host-port publish is reset (`!reset []`); only Caddy on `80/443` is reachable from outside

**Strongly recommended** with a public hostname: enable HTTP Basic auth on the admin URL by un-commenting the `basicauth` block in `Caddyfile`. Generate the hash with:

```bash
docker run --rm caddy:2-alpine caddy hash-password
```

Otherwise anyone with the URL can read/write to your database through the admin UI.

## Tunables (`.env`)

| Var | Default | Notes |
|---|---|---|
| `VECTORDB_API_KEY` | _(required)_ | Used internally between admin and gateway. Generate with `openssl rand -hex 32`. |
| `ADMIN_PORT` | `8090` | Host port for the admin panel. |
| `EMBED_DEFAULT_URL` | `http://host.docker.internal:1234/v1` | LMStudio on the docker host. Override for a remote embedding server. |
| `EMBED_DEFAULT_MODEL` | `text-embedding-nomic-embed-text-v1.5` | Default embedding model name. |
| `PUBLIC_HOSTNAME` | _(unset)_ | Caddy overlay only. |
| `ACME_EMAIL` | _(unset)_ | Caddy overlay only. |

Server-level tweaks (snapshot interval, WAL fsync, etc.) live in `node.toml` — edit then `docker compose restart server`.

## Lifecycle

```bash
docker compose ps                            # status
docker compose logs -f admin                 # tail admin
docker compose logs -f gateway               # tail gateway
docker compose logs -f server                # tail data node
docker compose restart admin gateway server  # restart in place

# Update to the latest code (after `git pull`):
docker compose up -d --build

# Stop (data preserved in the named volume):
docker compose down

# Wipe (DESTRUCTIVE — deletes the data volume):
docker compose down -v
```

## When NOT to use this

If you want **RF > 1** or **multiple shards**, use the repo-root setup:

```bash
make up RF=3 SHARDS=2
```

That setup uses `scripts/cluster.py` to generate per-replica configs and a much larger Compose file. The deploy here is intentionally limited to single-node because that's what 95% of "I have one box, I want to test" users actually need.
