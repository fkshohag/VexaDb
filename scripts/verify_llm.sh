#!/usr/bin/env bash
# verify_llm.sh — semantic + keyword search demo on LLM / LLaMA domain.
#
# Seeds 14 short docs about open-source LLMs, training, inference, and runtimes
# (LLaMA, Mistral, Qwen, RAG, LoRA, attention, vLLM, Ollama, MoE, …) and runs
# 9 search scenarios so you can verify nomic embeddings + BM25 + hybrid in your db.
#
# Usage:
#   bash scripts/verify_llm.sh
#   GATEWAY=... LM_STUDIO=... MODEL=... COLLECTION=verify-llm \
#     bash scripts/verify_llm.sh

set -uo pipefail

GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
LM_STUDIO="${LM_STUDIO:-http://127.0.0.1:1234/v1}"
MODEL="${MODEL:-text-embedding-nomic-embed-text-v1.5}"
COLLECTION="${COLLECTION:-verify-llm}"
DIM=768

bold()   { printf '\033[1m%s\033[0m\n' "$*"; }
green()  { printf '\033[32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
gray()   { printf '\033[90m%s\033[0m\n' "$*"; }

require() { command -v "$1" >/dev/null || { red "missing: $1"; exit 1; }; }
require curl
require python3

PY_HELPER="/tmp/verify_llm_helper_$$.py"
trap 'rm -f "$PY_HELPER"' EXIT

cat > "$PY_HELPER" <<'PY'
"""Helpers for verify_llm.sh — invoked via stdin/argv."""
import json, os, sys, urllib.request, urllib.error

LM_STUDIO = os.environ["LM_STUDIO"]
MODEL = os.environ["MODEL"]
GATEWAY = os.environ["GATEWAY"]
COLLECTION = os.environ["COLLECTION"]
DIM = int(os.environ["DIM"])

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

def embed(text):
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
    print(raw.decode())

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

# ───────────────────────── 1. Sanity ─────────────────────────
bold "[1/5] Sanity"
gateway_ok=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/v1/collections" || true)
[[ "$gateway_ok" == "200" ]] && green "  gateway OK ($GATEWAY)" || { red "  gateway not reachable ($GATEWAY)"; exit 1; }

lms_ok=$(curl -s -o /dev/null -w "%{http_code}" "$LM_STUDIO/models" || true)
[[ "$lms_ok" == "200" ]] && green "  LM Studio OK ($LM_STUDIO)" || { red "  LM Studio not reachable — run 'lms server start'"; exit 1; }

probe_dim=$(python3 "$PY_HELPER" embed-dim)
[[ "$probe_dim" == "$DIM" ]] && green "  embed model returns $DIM dims" || { red "  expected $DIM dims, got $probe_dim"; exit 1; }

# ───────────────────────── 2. Reset collection ─────────────────────────
bold "[2/5] Reset collection: $COLLECTION (dim $DIM, bm25_text_field=text)"
created=$(python3 "$PY_HELPER" create)
[[ "$created" == "201" || "$created" == "200" ]] && green "  created ($created)" || { red "  failed ($created)"; exit 1; }

# ───────────────────────── 3. Seed LLM docs ─────────────────────────
bold "[3/5] Upserting 14 LLM docs"

DOCS=$(cat <<'JSON'
[
 {"id":"llama-2",       "text":"LLaMA 2 is an open-weight large language model family from Meta released in 2023, available in 7B, 13B, and 70B parameter sizes.", "category":"model"},
 {"id":"llama-3",       "text":"LLaMA 3 from Meta improved tokenizer and training data scale, with 8B and 70B variants and a long-context 405B flagship.",       "category":"model"},
 {"id":"mistral-7b",    "text":"Mistral 7B uses sliding window attention and grouped-query attention to deliver strong quality at small parameter count.",         "category":"model"},
 {"id":"qwen-2",        "text":"Qwen2 is a multilingual open-weight model series from Alibaba covering 0.5B to 72B parameters with strong code and math abilities.","category":"model"},
 {"id":"gpt-4",         "text":"GPT-4 is a closed-source frontier model from OpenAI accessed via API, known for reasoning and multimodal capabilities.",            "category":"model"},
 {"id":"phi-3",         "text":"Phi-3 from Microsoft is a small language model trained on filtered web data and synthetic curricula, optimized for on-device use.","category":"model"},

 {"id":"transformer",   "text":"The Transformer architecture replaced recurrence with self-attention, enabling parallel training over long sequences.",              "category":"architecture"},
 {"id":"attention",     "text":"Self-attention computes weighted sums of token representations, letting each position attend to every other position in the input.","category":"architecture"},
 {"id":"moe",           "text":"Mixture-of-Experts models route each token to a subset of expert FFNs, scaling parameters without proportionally scaling compute.","category":"architecture"},

 {"id":"lora",          "text":"LoRA fine-tunes a frozen base model by training small low-rank adapter matrices, drastically reducing memory needed for adaptation.","category":"training"},
 {"id":"rlhf",          "text":"Reinforcement Learning from Human Feedback aligns language models to human preferences using a reward model and PPO updates.",       "category":"training"},
 {"id":"rag",           "text":"Retrieval-Augmented Generation grounds LLM answers in external documents fetched from a vector database to reduce hallucinations.",  "category":"technique"},

 {"id":"vllm",          "text":"vLLM is a fast inference and serving engine for LLMs that uses PagedAttention to maximize GPU memory throughput.",                  "category":"runtime"},
 {"id":"ollama",        "text":"Ollama runs open-weight language models locally on a laptop, exposing an HTTP API compatible with chat and embedding clients.",     "category":"runtime"}
]
JSON
)
echo "$DOCS" | python3 "$PY_HELPER" upsert | while read -r id; do gray "  upsert $id"; done

# ───────────────────────── 4. Tests ─────────────────────────
bold "[4/5] Running 9 search scenarios"

run_test() {
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
    printf '  '; yellow "WARN  top=$top  (expected $expect)"
  fi
  echo "$res" | python3 -c "
import sys,json
hits=json.load(sys.stdin)
for i,h in enumerate(hits[:5],1):
    print(f'    {i}. {h[\"id\"]:14s} score={h[\"score\"]:.4f}')
"
}

run_test "Test 1 · DENSE — paraphrase Meta's open model" \
  "llama-2" dense "Meta's open large language model"

run_test "Test 2 · DENSE — efficient parameter scaling concept" \
  "moe" dense "scale model parameters without scaling FLOPs per token"

run_test "Test 3 · DENSE — efficient fine-tuning" \
  "lora" dense "fine-tune a model cheaply with adapters"

run_test "Test 4 · DENSE — knowledge-grounding pattern" \
  "rag" dense "ground LLM answers in a knowledge base to avoid hallucination"

run_test "Test 5 · DENSE — alignment / preference learning" \
  "rlhf" dense "align a chatbot using human preference data"

run_test "Test 6 · DENSE — local inference on a Mac" \
  "ollama" dense "how can I run a language model on my laptop"

run_test "Test 7 · BM25 — exact term lookup" \
  "vllm" bm25 "PagedAttention vLLM"

run_test "Test 8 · HYBRID RRF — concept + brand" \
  "mistral-7b" hybrid_rrf "sliding window attention small open model"

run_test "Test 9 · DENSE + FILTER (category=architecture)" \
  "transformer" dense "self-attention replacing recurrent networks" \
  '{"must":[{"key":"category","match":{"value":"architecture"}}]}'

# ───────────────────────── 5. Done ─────────────────────────
echo
bold "[5/5] Done"
gray "  GREEN  = top result matched expectation"
gray "  YELLOW = unexpected top — usually still in top-3 (model wiggle is normal)"
echo
green "Open admin → '$COLLECTION' → Search → Embed query → run any of the queries above."
