import { useEffect, useState } from "react";
import { api, ApiError, type SearchRequest } from "../api";
import type { CollectionInfo, PointDetail, SearchHit } from "../types";

interface Props {
  apiKey: string;
  info: CollectionInfo;
  onError: (msg: string) => void;
}

const MODES = [
  { value: "dense", label: "Dense (vector)" },
  { value: "bm25", label: "BM25 (text)" },
  { value: "hybrid_rrf", label: "Hybrid (RRF)" },
  { value: "hybrid_weighted", label: "Hybrid (weighted)" },
] as const;

export function SearchPanel({ apiKey, info, onError }: Props) {
  const [mode, setMode] = useState<string>(info.bm25TextField ? "hybrid_rrf" : "dense");
  const [topK, setTopK] = useState(10);
  const [textQuery, setTextQuery] = useState("");
  const [vectorText, setVectorText] = useState("");
  const [filterText, setFilterText] = useState("");
  const [hybridAlpha, setHybridAlpha] = useState(0.5);
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [point, setPoint] = useState<PointDetail | null>(null);
  const [searching, setSearching] = useState(false);
  const [tookMs, setTookMs] = useState<number | null>(null);

  // When the user switches collection, reset transient state.
  useEffect(() => {
    setHits([]);
    setSelected(null);
    setPoint(null);
    setTookMs(null);
    setMode(info.bm25TextField ? "hybrid_rrf" : "dense");
  }, [info.name, info.bm25TextField]);

  const dim = info.dimension ?? 0;
  const needsVector = mode !== "bm25";

  const buildVector = (): number[] => {
    const txt = vectorText.trim();
    if (!txt) {
      // Pad/truncate to dim with zeros so BM25-only paths still satisfy the API.
      return new Array(dim).fill(0);
    }
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

  const onSearch = async () => {
    setSearching(true);
    setHits([]);
    setSelected(null);
    setPoint(null);
    try {
      const body: SearchRequest = {
        vector: buildVector(),
        top_k: topK,
        search_mode: mode,
      };
      if (mode !== "dense") body.text_query = textQuery;
      if (mode === "hybrid_weighted") body.hybrid_alpha = hybridAlpha;
      if (filterText.trim()) {
        try {
          body.filter = JSON.parse(filterText);
        } catch {
          throw new Error("Filter must be valid JSON");
        }
      }

      const t = performance.now();
      const result = await api.search(info.name, body, apiKey || undefined);
      setTookMs(Math.round(performance.now() - t));
      setHits(result);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`search failed: ${msg}`);
    } finally {
      setSearching(false);
    }
  };

  const selectHit = async (id: string) => {
    setSelected(id);
    setPoint(null);
    try {
      const p = await api.getPoint(info.name, id, apiKey || undefined);
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
          {mode !== "dense" && (
            <div className="field-full">
              <label>Text query</label>
              <input
                placeholder="how does Raft elect a leader?"
                value={textQuery}
                onChange={(e) => setTextQuery(e.target.value)}
              />
            </div>
          )}
          {needsVector && (
            <div className="field-full">
              <label>
                Vector (JSON array of {dim} numbers){" "}
                <span className="hint">— leave empty for zero vector</span>
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
              Filter <span className="hint">— optional JSON</span>
            </label>
            <textarea
              placeholder='{"must":[{"key":"category","match":{"value":"books"}}]}'
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              rows={2}
            />
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
                      onClick={() => selectHit(h.id)}
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
        <div className="v" style={{ wordBreak: "break-all" }}>{point.id}</div>
        <div className="k">values</div>
        <div className="v">
          <span className="hint">
            {point.values.length} dims · first 6: [
            {point.values
              .slice(0, 6)
              .map((x) => x.toFixed(3))
              .join(", ")}
            {point.values.length > 6 ? ", …" : ""}]
          </span>
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
