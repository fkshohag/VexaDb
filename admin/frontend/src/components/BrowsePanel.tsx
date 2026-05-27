import { useEffect, useState } from "react";
import { api, ApiError, type AuthHeaders } from "../api";
import type { CollectionInfo, ScrollPoint } from "../types";

interface Props {
  auth: AuthHeaders;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
}

/**
 * Browse every point in a collection using cursor-based scrolling
 * (`POST /v1/collections/:name/scroll`).
 *
 * This replaces the old "random unit vector + dense search" workaround.
 * Scroll is partition-aware, filter-aware, and stable across reads.
 */
export function BrowsePanel({ auth, info, onError, onSuccess }: Props) {
  const [points, setPoints] = useState<ScrollPoint[]>([]);
  const [cursor, setCursor] = useState<string>("");
  const [nextCursor, setNextCursor] = useState<string>("");
  const [pageSize, setPageSize] = useState(50);
  const [partition, setPartition] = useState("");
  const [filterText, setFilterText] = useState("");
  const [withPayload, setWithPayload] = useState(true);
  const [withVector, setWithVector] = useState(false);
  const [outputFields, setOutputFields] = useState("");
  const [busy, setBusy] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [pageIndex, setPageIndex] = useState(0);
  const [history, setHistory] = useState<string[]>([""]);

  // Reset on collection change.
  useEffect(() => {
    setPoints([]);
    setCursor("");
    setNextCursor("");
    setSelected(null);
    setPageIndex(0);
    setHistory([""]);
  }, [info.name]);

  const parseFilter = (): unknown | undefined => {
    const t = filterText.trim();
    if (!t) return undefined;
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

  const fetchPage = async (nextCur: string, resetHistory = false) => {
    setBusy(true);
    try {
      const filter = parseFilter();
      const r = await api.scroll(
        info.name,
        {
          cursor: nextCur,
          limit: pageSize,
          partition: partition.trim() || undefined,
          ...(filter !== undefined ? { filter } : {}),
          with_payload: withPayload,
          with_vector: withVector,
          ...(parseOutputFields() ? { output_fields: parseOutputFields() } : {}),
        },
        auth
      );
      setPoints(r.points || []);
      setNextCursor(r.next_cursor || "");
      setCursor(nextCur);
      setSelected(null);
      if (resetHistory) {
        setHistory([""]);
        setPageIndex(0);
      }
      onSuccess(`Scrolled ${(r.points || []).length} points`);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`scroll failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  const onFirstPage = async () => {
    await fetchPage("", true);
  };

  const onNextPage = async () => {
    if (!nextCursor) return;
    await fetchPage(nextCursor);
    setHistory((h) => {
      const nh = h.slice(0, pageIndex + 1);
      nh.push(nextCursor);
      return nh;
    });
    setPageIndex((i) => i + 1);
  };

  const onPrevPage = async () => {
    if (pageIndex <= 0) return;
    const target = history[pageIndex - 1] ?? "";
    await fetchPage(target);
    setPageIndex((i) => Math.max(0, i - 1));
  };

  const selectedPoint = points.find((p) => p.id === selected) ?? null;

  return (
    <>
      <section className="card">
        <h3>Browse — {info.name}</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Cursor-paginated iteration over the collection via{" "}
          <code>POST /v1/collections/{info.name}/scroll</code>. Combine with a
          filter or partition to narrow what you see.
        </p>

        <div className="form-grid">
          <div>
            <label>Page size</label>
            <input
              type="number"
              min={1}
              max={1000}
              value={pageSize}
              onChange={(e) => setPageSize(Math.max(1, Number(e.target.value) || 1))}
            />
          </div>
          <div>
            <label>Partition (optional)</label>
            <input
              value={partition}
              onChange={(e) => setPartition(e.target.value)}
              placeholder="_default"
            />
          </div>
          <div className="field-full">
            <label>
              Filter{" "}
              <span className="hint">
                — JSON or expression (<code>category == &quot;books&quot;</code>)
              </span>
            </label>
            <textarea
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              placeholder='category == "books"'
              rows={2}
            />
          </div>
          <div className="field-full">
            <label>
              Output fields{" "}
              <span className="hint">— optional, comma-separated</span>
            </label>
            <input
              value={outputFields}
              onChange={(e) => setOutputFields(e.target.value)}
              placeholder="text, category"
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

        <div className="row" style={{ gap: 8, flexWrap: "wrap" }}>
          <button className="primary" onClick={onFirstPage} disabled={busy}>
            {busy ? "Loading…" : "Start / Refresh"}
          </button>
          <button onClick={onPrevPage} disabled={busy || pageIndex <= 0}>
            ← Previous
          </button>
          <button onClick={onNextPage} disabled={busy || !nextCursor}>
            Next →
          </button>
          {points.length > 0 && (
            <span className="hint">
              Page {pageIndex + 1} · {points.length} point(s)
              {nextCursor ? " · more available" : " · end of stream"}
            </span>
          )}
        </div>
      </section>

      <section className="card">
        <h3>Points</h3>
        {points.length === 0 ? (
          <div className="empty">
            {busy ? "Loading…" : "No points yet — press Start / Refresh."}
          </div>
        ) : (
          <div className="split">
            <div style={{ maxHeight: 480, overflow: "auto" }}>
              <table className="results">
                <thead>
                  <tr>
                    <th style={{ width: 40 }}>#</th>
                    <th>id</th>
                    <th style={{ width: 60, textAlign: "right" }}>has</th>
                  </tr>
                </thead>
                <tbody>
                  {points.map((p, i) => (
                    <tr
                      key={p.id}
                      className={p.id === selected ? "selected" : ""}
                      onClick={() => setSelected(p.id)}
                    >
                      <td>{i + 1 + pageIndex * pageSize}</td>
                      <td>{p.id}</td>
                      <td className="score">
                        {[p.payload != null ? "p" : "", p.values?.length ? "v" : ""]
                          .filter(Boolean)
                          .join("+") || "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div>
              {selectedPoint ? (
                <PointView point={selectedPoint} bm25Field={info.bm25TextField} />
              ) : (
                <div className="empty">Click a row to view the point</div>
              )}
            </div>
          </div>
        )}
      </section>

      {cursor && (
        <section className="card">
          <h3>Cursor</h3>
          <p className="hint" style={{ marginTop: 0 }}>
            Send this back as <code>cursor</code> to resume the same scroll
            (matches the Go SDK&apos;s <code>QueryIterator</code> behaviour).
          </p>
          <pre style={{ fontSize: 11 }}>{cursor || "(start)"}</pre>
          <label style={{ marginTop: 12 }}>Next cursor</label>
          <pre style={{ fontSize: 11 }}>{nextCursor || "(end of stream)"}</pre>
        </section>
      )}
    </>
  );
}

function PointView({
  point,
  bm25Field,
}: {
  point: ScrollPoint;
  bm25Field: string | null;
}) {
  const payload = point.payload as Record<string, unknown> | null;
  const text = payload && bm25Field ? (payload[bm25Field] as string | undefined) : undefined;
  const values = point.values ?? [];

  return (
    <div>
      <div className="kv-grid" style={{ marginBottom: 12 }}>
        <div className="k">id</div>
        <div className="v" style={{ wordBreak: "break-all" }}>
          {point.id}
        </div>
        <div className="k">values</div>
        <div className="v">
          {values.length === 0 ? (
            <span className="hint">— (with_vector was false)</span>
          ) : (
            <span className="hint">
              {values.length} dims · first 6: [
              {values
                .slice(0, 6)
                .map((x) => x.toFixed(3))
                .join(", ")}
              {values.length > 6 ? ", …" : ""}]
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
      <pre>{payload != null ? JSON.stringify(payload, null, 2) : "(none)"}</pre>
    </div>
  );
}
