#!/usr/bin/env bash
# Bootstrap (or re-bootstrap) RBAC on a running VexaDb gateway.
#
# Idempotent: every step skips when the role/user/grant already exists.
# Uses the dev API key from `scripts/run-single.sh` for the initial superuser
# auth, then mints a fresh root token via /v1/auth/login.
#
# Usage:
#   scripts/bootstrap-rbac.sh                          # uses defaults below
#   ROOT_PASSWORD='Sup3r!' scripts/bootstrap-rbac.sh   # custom root password
#   GATEWAY=http://other:8080 scripts/bootstrap-rbac.sh
#
# Env knobs:
#   GATEWAY        gateway base URL          (default: http://127.0.0.1:8080)
#   API_KEY        legacy superuser API key  (default: read from $DATA_DIR/.api-key)
#   ROOT_USER      root username             (default: root)
#   ROOT_PASSWORD  root password             (default: VexaDb!)
#   DATA_DIR       data dir to locate .api-key (default: ./run-data/single)
#
# Prints the final API token to stdout on success.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
DATA_DIR="${DATA_DIR:-$ROOT/run-data/single}"
API_KEY_FILE="$DATA_DIR/.api-key"
API_KEY="${API_KEY:-}"
ROOT_USER="${ROOT_USER:-root}"
ROOT_PASSWORD="${ROOT_PASSWORD:-VexaDb!}"

if [[ -z "$API_KEY" && -s "$API_KEY_FILE" ]]; then
  API_KEY="$(cat "$API_KEY_FILE")"
fi
if [[ -z "$API_KEY" ]]; then
  echo "error: no API key. Set API_KEY=... or run scripts/run-single.sh first" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required" >&2; exit 1
fi

CURL=(curl -sS -o /tmp/.vexa-bootstrap-body -w '%{http_code}' \
      -H "x-api-key: $API_KEY" -H 'Content-Type: application/json')

# ---- helpers ----
say()  { printf '▶ %s\n' "$*"; }
warn() { printf '! %s\n' "$*" >&2; }

# Run a request, treat any 2xx as success and any 409/CONFLICT as no-op.
# Args: METHOD URI [JSON_BODY]
call() {
  local method="$1" uri="$2" body="${3:-}"
  local code
  if [[ -n "$body" ]]; then
    code="$("${CURL[@]}" -X "$method" "$GATEWAY$uri" -d "$body")"
  else
    code="$("${CURL[@]}" -X "$method" "$GATEWAY$uri")"
  fi
  if [[ "$code" =~ ^2 ]]; then
    return 0
  fi
  if [[ "$code" == "404" && "$method" == "GET" ]]; then
    return 1
  fi
  if [[ "$code" == "409" ]]; then
    return 2  # already exists
  fi
  warn "$method $uri → HTTP $code"
  cat /tmp/.vexa-bootstrap-body >&2 2>/dev/null || true
  echo >&2
  return 3
}

ensure_role() {
  local name="$1" desc="$2"
  if call GET "/v1/roles/$name" >/dev/null 2>&1; then
    say "role '$name' already exists"
    return 0
  fi
  if call POST "/v1/roles" "{\"name\":\"$name\",\"description\":\"$desc\"}"; then
    say "created role '$name'"
  else
    case $? in
      2) say "role '$name' already exists (409)" ;;
      *) warn "failed to create role '$name'"; return 1 ;;
    esac
  fi
}

ensure_user() {
  local name="$1" password="$2"
  if call GET "/v1/users/$name" >/dev/null 2>&1; then
    say "user '$name' already exists"
    return 0
  fi
  if call POST "/v1/users" "{\"name\":\"$name\",\"password\":\"$password\",\"roles\":[\"admin\"]}"; then
    say "created user '$name'"
  else
    case $? in
      2) say "user '$name' already exists (409)" ;;
      *) warn "failed to create user '$name'"; return 1 ;;
    esac
  fi
}

ensure_grant() {
  local role="$1" object_type="$2" object_name="$3" privilege="$4"
  local body
  body="$(printf '{"object_type":"%s","object_name":"%s","privilege":"%s"}' \
            "$object_type" "$object_name" "$privilege")"
  call POST "/v1/roles/$role/grants" "$body" >/dev/null 2>&1 || true
}

# ---- 1. Verify the gateway is reachable ----
say "Probing $GATEWAY/health"
if ! call GET /health >/dev/null; then
  warn "gateway not reachable at $GATEWAY"; exit 1
fi

# ---- 2. Ensure built-in roles exist (idempotent) ----
ensure_role "admin"       "Built-in superuser role"
ensure_role "read_write"  "Read/write access to all collections"
ensure_role "read_only"   "Read-only access to all collections"

# Admin gets Global:*:* and Collection:*:* (the gateway bootstrap already does
# this, but we re-issue here to make the script self-sufficient).
ensure_grant "admin" "Global"     "*" "*"
ensure_grant "admin" "Collection" "*" "*"

# ---- 3. Ensure root user exists with admin role ----
ensure_user "$ROOT_USER" "$ROOT_PASSWORD"
call POST "/v1/users/$ROOT_USER/roles/admin" >/dev/null 2>&1 || true

# ---- 4. Hand out a fresh token via /v1/auth/login ----
say "Logging in as $ROOT_USER to mint an API token…"
LOGIN_BODY="$(printf '{"username":"%s","password":"%s"}' "$ROOT_USER" "$ROOT_PASSWORD")"
if ! call POST /v1/auth/login "$LOGIN_BODY" >/dev/null; then
  warn "login failed — check gateway log"; exit 1
fi

# /tmp/.vexa-bootstrap-body now holds the LoginResponse JSON.
TOKEN="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' \
         < /tmp/.vexa-bootstrap-body 2>/dev/null || true)"

if [[ -z "$TOKEN" ]]; then
  warn "could not parse token from response:"
  cat /tmp/.vexa-bootstrap-body >&2; exit 1
fi

cat <<DONE
✔ RBAC bootstrap complete

  Gateway       : $GATEWAY
  Root user     : $ROOT_USER
  Root password : $ROOT_PASSWORD
  API key (x-api-key, legacy superuser):
    $API_KEY
  Root token (Authorization: Bearer …, scoped to admin):
    $TOKEN

Use either credential. The API key works without going through /v1/auth/login,
the token rotates whenever you call login again.
DONE

rm -f /tmp/.vexa-bootstrap-body 2>/dev/null || true
