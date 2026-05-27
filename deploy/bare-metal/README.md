# Bare-metal install (single node, RF=1, no Docker)

For when you already have a Linux box (your own VPS, a Hetzner server you provisioned manually, a metal rack server, etc.) and want VectorDB compiled from source and running as a systemd service. **Single command** after the clone.

## What it does

1. Installs build deps (`apt`, `dnf`, or `pacman` auto-detected).
2. Installs `rustup` + stable toolchain (as the invoking sudo user, not root).
3. `cargo build --release` for `vectordb-server`, `vectordb-gateway`, `vectordb`.
4. Drops the binaries into `/usr/local/bin`.
5. Creates a `vectordb` system user and `/var/lib/vectordb` data dir.
6. Renders `/etc/vectordb/node.toml` (single-shard, `all_in_one` role — one binary handles data + routing).
7. Generates a 32-byte hex API key (or accepts one via env).
8. Installs and starts two systemd units: `vectordb-server` and `vectordb-gateway`.
9. Waits for `/health` and prints connection info + smoke-test commands.

Idempotent — safe to re-run after a `git pull` to rebuild and restart.

## Prerequisites

- Ubuntu 22.04+/Debian 12+/Fedora 39+/Arch — anything systemd-based with a recent glibc.
- ~2 GB free RAM during the build (cargo is memory-hungry with `tonic`).
- `sudo` access.

## Run

```bash
git clone <your-vectordb-fork-or-https>
cd vectordb
sudo ./deploy/bare-metal/install.sh
```

First build takes 5–10 min on a 4-vCPU box. Subsequent runs (after `git pull`) take seconds for `cargo` to detect no changes, or minutes if Rust crates changed.

## Tunables (env vars)

All optional. Pass via `sudo -E ./install.sh` or set inline `sudo VECTORDB_HTTP_PORT=9000 ./install.sh`.

| Var | Default | Purpose |
|---|---|---|
| `VECTORDB_PREFIX` | `/usr/local` | Where binaries land. |
| `VECTORDB_USER` | `vectordb` | System user the daemons run as. |
| `VECTORDB_DATA_DIR` | `/var/lib/vectordb` | Data + WAL location. |
| `VECTORDB_CONFIG_DIR` | `/etc/vectordb` | Config files. |
| `VECTORDB_GRPC_PORT` | `6334` | Internal gRPC port (loopback only). |
| `VECTORDB_HTTP_PORT` | `8080` | Gateway HTTP port. |
| `VECTORDB_BIND_HTTP` | `0.0.0.0:8080` | Listen addr for the gateway. |
| `VECTORDB_BIND_GRPC` | `0.0.0.0:6334` | Listen addr for the server. |
| `VECTORDB_API_KEY` | _auto-generated_ | Required on every gateway request. |
| `SKIP_BUILD` | `0` | Set `1` to skip cargo build (binaries already in PATH). |

## After installing

```bash
# Service control
systemctl status   vectordb-server vectordb-gateway
systemctl restart  vectordb-server vectordb-gateway
journalctl -u vectordb-server -f

# Open the firewall (Ubuntu/Debian)
sudo ufw allow 8080/tcp

# Update to latest main
cd /path/to/vectordb && git pull && sudo ./deploy/bare-metal/install.sh

# Inspect / use
source <(grep -E '^VECTORDB_API_KEYS=' /etc/vectordb/gateway.env)
KEY="${VECTORDB_API_KEYS}"
curl -sS -H "x-api-key: $KEY" http://127.0.0.1:8080/health
curl -sS -H "x-api-key: $KEY" http://127.0.0.1:8080/v1/admin/cluster | jq
```

## Uninstall

```bash
sudo ./deploy/bare-metal/uninstall.sh        # asks before each step
sudo FORCE=1 KEEP_DATA=1 ./uninstall.sh      # remove services, keep /var/lib/vectordb
```

## When NOT to use this

If you want **multi-shard** or **RF>1** topologies, use the Docker setup instead — `make up RF=3 SHARDS=2`. The bare-metal installer is intentionally limited to the all-in-one single-node mode because that's what 95% of "I have one box" users actually want.
