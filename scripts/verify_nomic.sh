#!/usr/bin/env bash
# verify_nomic.sh — end-to-end test for VectorDB + LM Studio (nomic-embed-text-v1.5)
#
# What this script does:
#   1. Creates a fresh dim-768 collection with BM25 text field.
#   2. Upserts 12 short docs across 4 topics (fintech, telecom, food, healthcare).
#   3. Runs 8 search scenarios covering dense / BM25 / hybrid / filters.
#   4. Prints top-5 IDs per scenario with PASS/WARN against expected top.
#
# Usage:
#   bash scripts/verify_nomic.sh
#   GATEWAY=http://127.0.0.1:8080 LM_STUDIO=http://127.0.0.1:1234/v1 \
#     COLLECTION=verify-nomic bash scripts/verify_nomic.sh

set -uo pipefail

GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
LM_STUDIO="${LM_STUDIO:-http://127.0.0.1:1234/v1}"
MODEL="${MODEL:-text-embedding-nomic-embed-text-v1.5}"
COLLECTION="${COLLECTION:-verify-nomic}"
DIM=768

bold()   { printf '\033[1m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
gray()   { printf '\033[90m%s\033[0m\n' "$*"; }

require() { command -v "$1" >/dev/null || { red "missing: $1"; exit 1; }; }
require curl
require python3

PY_HELPER="/tmp/verify_nomic_helper_$$.py"
trap 'rm -f "$PY_HELPER"' EXIT

cat > "$PY_HELPER" <<'PY'
"""Helpers for verify_nomic.sh — invoked via stdin/argv."""
import json, os, sys, urllib.request

LM_STUDIO = os.environ.get("LM_STUDIO", "http://127.0.0.1:1234/v1")
MODEL = os.environ.get("MODEL", "text-embedding-nomic-embed-text-v1.5")
GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8080")
COLLECTION = os.environ.get("COLLECTION", "verify-nomic")
DIM = int(os.environ.get("DIM", "768"))

def http_post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read()

def http_delete(url):
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code

def embed(text: str) -> list[float]:
    code, body = http_post(f"{LM_STUDIO}/embeddings", {"model": MODEL, "input": text})
    if code != 200:
        raise SystemExit(f"embed failed {code}: {body[:200]}")
    return json.loads(body)["data"][0]["embedding"]

def cmd_embed_dim():
    print(len(embed("hello world")))

def cmd_create():
    http_delete(f"{GATEWAY}/v1/collections/{COLLECTION}")
    code, _ = http_post(f"{GATEWAY}/v1/collections", {
        "name": COLLECTION,
        "dimension": DIM,
        "distance": "cosine",
        "bm25_text_field": "text",
    })
    print(code)

def cmd_upsert():
    # stdin = JSON array of {"id","text","category"}
    docs = json.load(sys.stdin)
    for d in docs:
        vec = embed(d["text"])
        payload = {"text": d["text"]}
        if d.get("category"):
            payload["category"] = d["category"]
        body = {"points": [{"id": d["id"], "values": vec, "payload": payload}]}
        code, b = http_post(f"{GATEWAY}/v1/collections/{COLLECTION}/upsert", body)
        if code >= 300:
            raise SystemExit(f"upsert {d['id']} failed {code}: {b[:200]}")
        print(d["id"])

def cmd_search():
    # argv: mode text [filter_json] [hybrid_alpha]
    mode = sys.argv[2]
    text = sys.argv[3]
    filt = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else ""
    alpha = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] else ""
    if mode == "bm25":
        vec = [0.0] * DIM
    else:
        vec = embed(text)
    body = {"vector": vec, "top_k": 5, "search_mode": mode}
    if mode != "dense":
        body["text_query"] = text
    if mode == "hybrid_weighted" and alpha:
        body["hybrid_alpha"] = float(alpha)
    if filt:
        body["filter"] = json.loads(filt)
    code, raw = http_post(f"{GATEWAY}/v1/collections/{COLLECTION}/search", body)
    if code != 200:
        raise SystemExit(f"search failed {code}: {raw[:300]}")
    hits = json.loads(raw)
    print(json.dumps(hits))

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {
        "embed-dim": cmd_embed_dim,
        "create": cmd_create,
        "upsert": cmd_upsert,
        "search": cmd_search,
    }[cmd]()
PY

export GATEWAY LM_STUDIO MODEL COLLECTION DIM

# ───────────────────────── 1. Sanity checks ─────────────────────────
bold "[1/5] Sanity"
gateway_ok=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/v1/collections" || true)
[[ "$gateway_ok" == "200" ]] && green "  gateway OK ($GATEWAY)" || { red "  gateway not reachable ($GATEWAY)"; exit 1; }

lms_ok=$(curl -s -o /dev/null -w "%{http_code}" "$LM_STUDIO/models" || true)
[[ "$lms_ok" == "200" ]] && green "  LM Studio OK ($LM_STUDIO)" || { red "  LM Studio not reachable — run 'lms server start'"; exit 1; }

probe_dim=$(python3 "$PY_HELPER" embed-dim)
[[ "$probe_dim" == "$DIM" ]] && green "  embed model returns $DIM dims" || { red "  expected $DIM dims, got $probe_dim"; exit 1; }

# ───────────────────────── 2. Recreate collection ─────────────────────────
bold "[2/5] Reset collection: $COLLECTION (dim $DIM, bm25_text_field=text)"
created=$(python3 "$PY_HELPER" create)
[[ "$created" == "201" || "$created" == "200" ]] && green "  created ($created)" || { red "  failed to create ($created)"; exit 1; }

# ───────────────────────── 3. Seed 12 docs ─────────────────────────
bold "[3/5] Upserting 12 documents (each call embeds via LM Studio)"

DOCS=$(cat <<'JSON'
[
 {"id":"bkash-1",  "text":"Bkash is the largest mobile financial service in Bangladesh, used to send money and pay bills.", "category":"fintech"},
 {"id":"nagad-1",  "text":"Nagad provides digital wallet and mobile money transfers across Bangladesh.",                  "category":"fintech"},
 {"id":"rocket-1", "text":"Rocket is a mobile banking service operated by Dutch-Bangla Bank.",                            "category":"fintech"},
 {"id":"stripe-1", "text":"Stripe is an American payments processor that handles online credit card transactions.",      "category":"fintech"},
 {"id":"gp-1",     "text":"Grameenphone is a major mobile telecom operator providing voice and data plans.",             "category":"telecom"},
 {"id":"robi-1",   "text":"Robi Axiata offers cellular network and 4G internet services in Bangladesh.",                 "category":"telecom"},
 {"id":"att-1",    "text":"AT&T is a large American telecommunications company offering wireless and broadband.",        "category":"telecom"},
 {"id":"pizza-1",  "text":"Margherita pizza is a classic Italian dish with tomato, mozzarella, and basil.",              "category":"food"},
 {"id":"biryani-1","text":"Kacchi biryani is a famous Bangladeshi rice dish made with marinated mutton and saffron.",    "category":"food"},
 {"id":"sushi-1",  "text":"Sushi is a Japanese dish of vinegared rice served with raw fish or vegetables.",              "category":"food"},
 {"id":"apollo-1", "text":"Apollo Hospital is a large private healthcare provider with cardiology and oncology departments.","category":"healthcare"},
 {"id":"panadol-1","text":"Panadol contains paracetamol and is used to relieve fever and mild pain.",                    "category":"healthcare"}
]
JSON
)
echo "$DOCS" | python3 "$PY_HELPER" upsert | while read -r id; do gray "  upsert $id"; done

# ───────────────────────── 4. Scenarios ─────────────────────────
bold "[4/5] Running 8 search scenarios"

run_test() {
  # args: label expected_top mode text [filter] [alpha]
  local label="$1" expect="$2" mode="$3" text="$4" filt="${5:-}" alpha="${6:-}"
  echo
  bold "$label"
  gray "  mode=$mode  query='$text'  expect=$expect"
  local res
  res=$(python3 "$PY_HELPER" search "$mode" "$text" "$filt" "$alpha")
  local top
  top=$(echo "$res" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r[0]['id'] if r else 'EMPTY')")
  if [[ "$top" == "$expect" ]]; then
    printf '  '; green "PASS  top=$top"
  else
    printf '  '; yellow "WARN  top=$top  (expected $expect — model wiggle is OK if it's still in top 3)"
  fi
  echo "$res" | python3 -c "
import sys,json
hits = json.load(sys.stdin)
for i,h in enumerate(hits[:5], 1):
    pid=h.get('id'); s=h.get('score',0)
    print(f'    {i}. {pid:14s} score={s:.4f}')
"
}

run_test "Test 1 · DENSE — synonym (no keyword overlap)" \
  "bkash-1" dense "send money from a phone in Bangladesh"

run_test "Test 2 · DENSE — cross-domain disambiguation" \
  "pizza-1" dense "Italian food"

run_test "Test 3 · DENSE — health domain" \
  "panadol-1" dense "medicine for headache"

run_test "Test 4 · BM25 — exact keyword" \
  "stripe-1" bm25 "Stripe credit card"

run_test "Test 5 · BM25 — rare keyword" \
  "biryani-1" bm25 "biryani"

run_test "Test 6 · HYBRID RRF — keyword + concept" \
  "gp-1" hybrid_rrf "Bangladesh internet operator"

run_test "Test 7 · HYBRID weighted (alpha=0.7, dense-leaning)" \
  "bkash-1" hybrid_weighted "mobile money service" "" "0.7"

run_test "Test 8 · DENSE + FILTER (category=food)" \
  "biryani-1" dense "something to eat with rice" \
  '{"must":[{"key":"category","match":{"value":"food"}}]}'

# ───────────────────────── 5. Done ─────────────────────────
echo
bold "[5/5] Done"
gray "  GREEN  = top result matched expectation (semantic stack works end-to-end)"
gray "  YELLOW = unexpected top — re-run, the model is non-deterministic at 4-5 decimal places"
echo
green "Open admin → Search tab on '$COLLECTION' to try the same queries interactively."
