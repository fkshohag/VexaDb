import { useEffect, useMemo, useState } from "react";
import {
  api,
  ApiError,
  type AuthHeaders,
  type HybridSearchBody,
  type SearchRequest,
} from "../api";
import type { CollectionInfo, PointDetail, SearchHit } from "../types";
import { embedSmart, pickEmbedStrategy } from "../embeddingStrategy";

interface Props {
  auth: AuthHeaders;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess?: (msg: string) => void;
}

const MODES = [
  { value: "dense", label: "Dense (vector)" },
  { value: "bm25", label: "BM25 (text)" },
  { value: "hybrid_rrf", label: "Hybrid — single (RRF)" },
  { value: "hybrid_weighted", label: "Hybrid — single (weighted)" },
  { value: "hybrid_multi", label: "Hybrid — multi (AnnRequest)" },
] as const;

export function SearchPanel({ auth, info, onError, onSuccess }: Props) {
  const [mode, setMode] = useState<string>(info.bm25TextField ? "hybrid_rrf" : "dense");
  const [topK, setTopK] = useState(10);
  const [textQuery, setTextQuery] = useState("");
  const [vectorText, setVectorText] = useState("");
  const [filterText, setFilterText] = useState("");
  const [hybridAlpha, setHybridAlpha] = useState(0.5);
  const [rerankerKind, setRerankerKind] = useState<"rrf" | "weighted">("rrf");
  const [rerankerWeights, setRerankerWeights] = useState("1.0, 1.0");
  const [withPayload, setWithPayload] = useState(true);
  const [withVector, setWithVector] = useState(false);
  const [outputFields, setOutputFields] = useState("");
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [point, setPoint] = useState<PointDetail | null>(null);
  const [searching, setSearching] = useState(false);
  const [tookMs, setTookMs] = useState<number | null>(null);
  const [embedBusy, setEmbedBusy] = useState(false);

  // When the user switches collection, reset transient state.
  useEffect(() => {
    setHits([]);
    setSelected(null);
    setPoint(null);
    setTookMs(null);
    setMode(info.bm25TextField ? "hybrid_rrf" : "dense");
  }, [info.name, info.bm25TextField]);

  const dim = info.dimension ?? 0;
  const isHybrid = mode === "hybrid_rrf" || mode === "hybrid_weighted" || mode === "hybrid_multi";
  const needsVector = mode !== "bm25";
  const needsText = mode === "bm25" || isHybrid;
  const embedPick = useMemo(() => pickEmbedStrategy(dim), [dim]);

  const embedQuery = async () => {
    const text = (textQuery || "").trim();
    if (!text) {
      onError("Type a text query first to embed it");
      return;
    }
    if (!dim) {
      onError("Collection dimension unknown");
      return;
    }
    setEmbedBusy(true);
    try {
      const result = await embedSmart(text, dim, {
        fallbackToHash: embedPick.method === "model",
      });
      setVectorText(JSON.stringify(result.vector));
      const note =
        result.method === "model"
          ? `Semantic embed (${result.detail})`
          : `Hash embed (${result.detail})`;
      onSuccess?.(`${note} → ${result.dimensions} dims`);
    } catch (e) {
      onError((e as Error).message);
    } finally {
      setEmbedBusy(false);
    }
  };

  const buildVector = (): number[] => {
    const txt = vectorText.trim();
    if (!txt) return new Array(dim).fill(0);
    let parsed: unknown;
    try {
      parsed = JSON.parse(txt);
    } catch {
      throw new Error("Vector must be a JSON array of numbers, e.g. [0.1, 0.2, ...]");
    }
    if (!Array.isArray(parsed) || parsed.some((x) => typeof x !== "number")) {
      throw new Error("Vector must be a JSON array of numbers");
    }
    if (dim > 0 && parsed.length !== dim) {
      throw new Error(`Vector has ${parsed.length} dims; collection expects ${dim}`);
    }
    return parsed as number[];
  };

  const parseFilter = (): unknown | undefined => {
    const t = filterText.trim();
    if (!t) return undefined;
    // Accept either JSON or a Milvus-style expression string ("category == 'a'");
    // strings pass through to the server, which parses them.
    if (t.startsWith("{") || t.startsWith("[")) {
      try {
        return JSON.parse(t);
      } catch {
        throw new Error("Filter must be valid JSON, or a quoted expression string");
      }
    }
    return t;
  };

  const parseOutputFields = (): string[] | undefined => {
    const t = outputFields.trim();
    if (!t) return undefined;
    return t.split(/[\s,]+/).filter((s) => s.length > 0);
  };

  const runSimple = async () => {
    const body: SearchRequest = {
      vector: buildVector(),
      top_k: topK,
      search_mode: mode,
      with_payload: withPayload,
      with_vector: withVector,
    };
    const of = parseOutputFields();
    if (of) body.output_fields = of;
    if (mode !== "dense") body.text_query = textQuery;
    if (mode === "hybrid_weighted") body.hybrid_alpha = hybridAlpha;
    const f = parseFilter();
    if (f !== undefined) body.filter = f;
    return api.search(info.name, body, auth);
  };

  const runMultiHybrid = async () => {
    const weights = rerankerWeights
      .split(/[\s,]+/)
      .map((s) => Number(s))
      .filter((n) => Number.isFinite(n));
    const vec = buildVector();
    const f = parseFilter();
    const text = textQuery.trim();
    const body: HybridSearchBody = {
      limit: topK,
      with_payload: withPayload,
      with_vector: withVector,
      reranker:
        rerankerKind === "weighted"
          ? { kind: "weighted", weights: weights.length ? weights : [1, 1] }
          : { kind: "rrf" },
      requests: [
        // Leg 1: dense ANN
        {
          field: "vector",
          dense: vec,
          limit: topK,
          ...(f !== undefined ? { filter: f } : {}),
        },
        // Leg 2: BM25 on the configured text field (skip if collection has no text field).
        ...(text && info.bm25TextField
          ? [
              {
                field: info.bm25TextField,
                text,
                limit: topK,
                ...(f !== undefined ? { filter: f } : {}),
              },
            ]
          : []),
      ],
    };
    const of = parseOutputFields();
    if (of) body.output_fields = of;
    return api.hybridSearch(info.name, body, auth);
  };

  const onSearch = async () => {
    setSearching(true);
    setHits([]);
    setSelected(null);
    setPoint(null);
    try {
      const t = performance.now();
      const result = mode === "hybrid_multi" ? await runMultiHybrid() : await runSimple();
      setTookMs(Math.round(performance.now() - t));
      setHits(result);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`search failed: ${msg}`);
    } finally {
      setSearching(false);
    }
  };

  const selectHit = async (h: SearchHit) => {
    setSelected(h.id);
    // If the search included payload/vector inline, surface it without a
    // second round-trip — feels instant and survives RBAC ReadVectors gating
    // that may forbid the /points GET while allowing the /search response.
    if (h.payload !== undefined || h.vector !== undefined) {
      setPoint({
        id: h.id,
        values: h.vector ?? [],
        payload: h.payload ?? null,
      });
      return;
    }
    setPoint(null);
    try {
      const p = await api.getPoint(info.name, h.id, auth);
      setPoint(p);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`fetch point failed: ${msg}`);
    }
  };

  return (
    <>
      <section className="card">
        <h3>Search</h3>
        <div className="form-grid">
          <div>
            <label>Mode</label>
            <select value={mode} onChange={(e) => setMode(e.target.value)}>
              {MODES.map((m) => (
                <option
                  key={m.value}
                  value={m.value}
                  disabled={
                    (m.value === "bm25" || m.value.startsWith("hybrid")) && !info.bm25TextField
                  }
                >
                  {m.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label>Top K</label>
            <input
              type="number"
              min={1}
              max={500}
              value={topK}
              onChange={(e) => setTopK(Math.max(1, Number(e.target.value) || 1))}
            />
          </div>
          {mode === "hybrid_weighted" && (
            <div>
              <label>Hybrid α (dense weight)</label>
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={hybridAlpha}
                onChange={(e) => setHybridAlpha(Number(e.target.value))}
              />
            </div>
          )}
          {mode === "hybrid_multi" && (
            <>
              <div>
                <label>Reranker</label>
                <select
                  value={rerankerKind}
                  onChange={(e) => setRerankerKind(e.target.value as "rrf" | "weighted")}
                >
                  <option value="rrf">RRF (parameter-free)</option>
                  <option value="weighted">Weighted</option>
                </select>
              </div>
              {rerankerKind === "weighted" && (
                <div>
                  <label>Weights (one per leg)</label>
                  <input
                    value={rerankerWeights}
                    onChange={(e) => setRerankerWeights(e.target.value)}
                    placeholder="1.0, 0.7"
                  />
                </div>
              )}
            </>
          )}
          <div className="field-full">
            <label>
              Text query{" "}
              <span className="hint">
                — used for{" "}
                {mode === "dense"
                  ? "embedding (optional)"
                  : mode === "bm25"
                  ? "BM25"
                  : "BM25 + dense embedding"}
              </span>
            </label>
            <div className="row" style={{ gap: 6 }}>
              <input
                placeholder="how does Raft elect a leader?"
                value={textQuery}
                onChange={(e) => setTextQuery(e.target.value)}
                style={{ flex: 1 }}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) embedQuery();
                }}
              />
              {needsVector && (
                <button
                  type="button"
                  onClick={embedQuery}
                  disabled={embedBusy || !textQuery.trim() || !dim}
                  title={`Smart embed (${embedPick.label}) — fills the vector below`}
                >
                  {embedBusy ? "Embedding…" : `Embed query (${embedPick.label})`}
                </button>
              )}
              <button type="button" onClick={() => setVectorText("")} disabled={!vectorText}>
                Clear vector
              </button>
            </div>
            {needsText && !textQuery.trim() && (
              <p className="hint" style={{ marginTop: 6, fontSize: 11 }}>
                {mode === "hybrid_multi"
                  ? "Without a text query the BM25 leg is skipped — only dense is searched."
                  : "BM25 mode needs a text query."}
              </p>
            )}
          </div>
          {needsVector && (
            <div className="field-full">
              <label>
                Vector (JSON array of {dim} numbers){" "}
                <span className="hint">— filled by Embed query, or paste your own</span>
              </label>
              <textarea
                placeholder={`[${"0, ".repeat(Math.min(dim, 4))}…]`}
                value={vectorText}
                onChange={(e) => setVectorText(e.target.value)}
                rows={3}
              />
            </div>
          )}
          <div className="field-full">
            <label>
              Filter{" "}
              <span className="hint">
                — optional JSON, or a Milvus expression like{" "}
                <code>category == &quot;books&quot;</code>
              </span>
            </label>
            <textarea
              placeholder={`{"must":[{"key":"category","match":{"value":"books"}}]}`}
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              rows={2}
            />
          </div>
          <div className="field-full">
            <label>
              Output fields{" "}
              <span className="hint">— optional, comma-separated; empty = full payload</span>
            </label>
            <input
              placeholder="text, category"
              value={outputFields}
              onChange={(e) => setOutputFields(e.target.value)}
            />
          </div>
          <div className="field-full">
            <div className="row" style={{ gap: 16, flexWrap: "wrap" }}>
              <label
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  margin: 0,
                  textTransform: "none",
                  letterSpacing: 0,
                  fontSize: 12,
                  color: withPayload ? "var(--accent)" : "var(--text-muted)",
                }}
              >
                <input
                  type="checkbox"
                  checked={withPayload}
                  onChange={(e) => setWithPayload(e.target.checked)}
                  style={{ width: "auto" }}
                />
                with_payload
              </label>
              <label
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  margin: 0,
                  textTransform: "none",
                  letterSpacing: 0,
                  fontSize: 12,
                  color: withVector ? "var(--accent)" : "var(--text-muted)",
                }}
              >
                <input
                  type="checkbox"
                  checked={withVector}
                  onChange={(e) => setWithVector(e.target.checked)}
                  style={{ width: "auto" }}
                />
                with_vector
              </label>
            </div>
          </div>
        </div>

        <div className="row">
          <button className="primary" onClick={onSearch} disabled={searching}>
            {searching ? "Searching…" : "Search"}
          </button>
          {tookMs != null && (
            <span className="hint">
              {hits.length} hits in {tookMs} ms
            </span>
          )}
        </div>
      </section>

      <section className="card">
        <h3>Results</h3>
        {hits.length === 0 ? (
          <div className="empty">
            {searching ? "Searching…" : "No results yet — run a search above."}
          </div>
        ) : (
          <div className="split">
            <div style={{ maxHeight: 480, overflow: "auto" }}>
              <table className="results">
                <thead>
                  <tr>
                    <th style={{ width: 40 }}>#</th>
                    <th>id</th>
                    <th style={{ width: 90, textAlign: "right" }}>score</th>
                  </tr>
                </thead>
                <tbody>
                  {hits.map((h, i) => (
                    <tr
                      key={h.id}
                      className={h.id === selected ? "selected" : ""}
                      onClick={() => selectHit(h)}
                    >
                      <td>{i + 1}</td>
                      <td>{h.id}</td>
                      <td className="score">{h.score.toFixed(4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div>
              {selected ? (
                point ? (
                  <PointView point={point} bm25Field={info.bm25TextField} />
                ) : (
                  <div className="empty">Loading…</div>
                )
              ) : (
                <div className="empty">Click a row to view the point</div>
              )}
            </div>
          </div>
        )}
      </section>
    </>
  );
}

function PointView({ point, bm25Field }: { point: PointDetail; bm25Field: string | null }) {
  const payload = point.payload as Record<string, unknown> | null;
  const text = payload && bm25Field ? (payload[bm25Field] as string | undefined) : undefined;

  return (
    <div>
      <div className="kv-grid" style={{ marginBottom: 12 }}>
        <div className="k">id</div>
        <div className="v" style={{ wordBreak: "break-all" }}>
          {point.id}
        </div>
        <div className="k">values</div>
        <div className="v">
          {point.values.length === 0 ? (
            <span className="hint">— (with_vector was false)</span>
          ) : (
            <span className="hint">
              {point.values.length} dims · first 6: [
              {point.values
                .slice(0, 6)
                .map((x) => x.toFixed(3))
                .join(", ")}
              {point.values.length > 6 ? ", …" : ""}]
            </span>
          )}
        </div>
      </div>
      {text && (
        <>
          <label>Text ({bm25Field})</label>
          <pre>{text}</pre>
        </>
      )}
      <label style={{ marginTop: 12 }}>Payload</label>
      <pre>{JSON.stringify(payload, null, 2)}</pre>
    </div>
  );
}
