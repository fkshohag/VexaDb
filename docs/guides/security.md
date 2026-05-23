# Security

VectorDB ships with API-key authentication and optional TLS for both the gRPC
server and the REST gateway. There’s no built-in user store or RBAC — keys are
shared secrets validated at every RPC.

---

## API keys

### Server (gRPC)

```toml
# config/example-secure.toml (excerpt)
[auth]
required = true
keys = ["dev-secret-key-change-me", "ci-key", "ops-key"]
```

A gRPC interceptor checks every RPC except `Health`. Send the key as either:

- `Authorization: Bearer <key>`, or
- `x-api-key: <key>`

Multiple keys are valid simultaneously — useful for rotation.

Without a key, RPCs return `UNAUTHENTICATED`.

### Gateway (HTTP)

```bash
VECTORDB_API_KEYS="dev-secret-key-change-me,ci-key" \
  VECTORDB_GRPC=http://127.0.0.1:6334 \
  cargo run -p vectordb-gateway --release
```

The first key is forwarded as the SDK API key on upstream gRPC calls. Same
two header forms are accepted on `/v1/...` paths. Probes (`/live`, `/ready`,
`/metrics`, `/health`) are exempt so load balancers and Prometheus work without
secrets.

---

## TLS

### gRPC TLS

```toml
[tls]
cert_file = "./certs/server.crt"
key_file  = "./certs/server.key"
```

When `[tls]` is present, the listener serves TLS only. Generate certs with
your CA, or for local testing:

```bash
mkdir -p certs
openssl req -newkey rsa:2048 -nodes -keyout certs/server.key \
  -x509 -out certs/server.crt -days 365 \
  -subj "/CN=localhost"
```

Clients must use `https://` and trust the CA. The Rust SDK uses the system
trust store by default.

### Gateway TLS

```bash
VECTORDB_TLS_CERT=./certs/server.crt \
VECTORDB_TLS_KEY=./certs/server.key \
cargo run -p vectordb-gateway --release
```

This serves HTTPS via `axum-server` + `rustls`.

---

## Recommended hardening

| Concern | Recommendation |
|---------|----------------|
| Network exposure | Bind data nodes to `127.0.0.1` or a private subnet; expose only the gateway publicly. |
| TLS | Always enable TLS on the gateway when public. |
| API keys | Rotate via two-key window; never commit to source. |
| Logs | Don’t log raw API keys. The gateway/server log structured JSON via `tracing`; review before shipping to log sinks. |
| Tenancy | Use one collection per tenant; restrict access via separate gateways per tenant if needed. |
| Disk | Encrypt the data directory at the OS level (LUKS / FileVault / cloud KMS). |
| Backups | Treat snapshots as sensitive — they contain raw vectors and payloads. |
| Resource limits | Run behind a reverse proxy (nginx, Envoy) that enforces request size and rate limits. |

---

## Sample “secure” deployment

```bash
# 1. Server with auth + TLS + Prometheus
cargo run -p vectordb-server -- --config config/example-secure.toml

# 2. Gateway with the same key + TLS
VECTORDB_API_KEYS=dev-secret-key-change-me \
VECTORDB_GRPC=http://127.0.0.1:6334 \
VECTORDB_TLS_CERT=./certs/server.crt \
VECTORDB_TLS_KEY=./certs/server.key \
cargo run -p vectordb-gateway

# 3. Client request
curl -sk https://localhost:8080/v1/collections \
  -H 'x-api-key: dev-secret-key-change-me'
```

---

## What’s not implemented

- RBAC / per-collection ACLs (use one gateway per scope as a workaround)
- mTLS (only server-auth TLS today)
- IP allow-lists (do this at the proxy / firewall)
- Audit log of admin operations

These belong to the future enterprise track.
