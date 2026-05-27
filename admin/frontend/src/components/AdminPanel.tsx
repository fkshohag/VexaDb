import { useEffect, useState } from "react";
import { api, ApiError, type AuthHeaders, type SnapshotInfo } from "../api";
import type { AnalyzerToken, CollectionInfo } from "../types";

interface Props {
  auth: AuthHeaders;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
  onCollectionDeleted: () => void;
}

export function AdminPanel({
  auth,
  info,
  onError,
  onSuccess,
  onCollectionDeleted,
}: Props) {
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [busy, setBusy] = useState<string | null>(null);

  // ── Delete by IDs / filter ─────────────────────────────────────────────
  const [deleteIds, setDeleteIds] = useState("");
  const [deleteFilter, setDeleteFilter] = useState("");
  const [deletePartition, setDeletePartition] = useState("");

  // ── Analyzer ───────────────────────────────────────────────────────────
  const [analyzeText, setAnalyzeText] = useState("Vector databases store embeddings.");
  const [analyzeFromCollection, setAnalyzeFromCollection] = useState(true);
  const [analyzeParams, setAnalyzeParams] = useState(
    '{"tokenizer": "standard", "filter": ["lowercase"]}'
  );
  const [analyzerResults, setAnalyzerResults] = useState<AnalyzerToken[][] | null>(null);

  const reloadSnapshots = async () => {
    try {
      const list = await api.listSnapshots(auth);
      setSnapshots(list ?? []);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`list snapshots failed: ${msg}`);
    }
  };

  useEffect(() => {
    reloadSnapshots();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth.apiKey, auth.bearer]);

  const wrap = async (label: string, fn: () => Promise<unknown>, refresh = false) => {
    setBusy(label);
    try {
      const r = await fn();
      onSuccess(
        typeof r === "object" && r != null
          ? `${label} ✓ ${JSON.stringify(r)}`
          : `${label} ✓`
      );
      if (refresh) reloadSnapshots();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`${label} failed: ${msg}`);
    } finally {
      setBusy(null);
    }
  };

  const deleteCollection = async () => {
    if (!confirm(`Delete collection "${info.name}" — this cannot be undone. Proceed?`)) return;
    setBusy("delete");
    try {
      await api.deleteCollection(info.name, auth);
      onSuccess(`deleted collection ${info.name}`);
      onCollectionDeleted();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`delete collection failed: ${msg}`);
    } finally {
      setBusy(null);
    }
  };

  const runDelete = async () => {
    const ids = deleteIds
      .split(/[\s,\n]+/)
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    const filter = deleteFilter.trim();
    if (ids.length === 0 && !filter) {
      onError("Provide point IDs or a Milvus-style filter expression");
      return;
    }
    if (
      !confirm(
        `Delete ${ids.length ? `${ids.length} id(s)` : ""}${
          ids.length && filter ? " + " : ""
        }${filter ? `filter "${filter}"` : ""} from "${info.name}"?`
      )
    ) {
      return;
    }
    setBusy("delete-points");
    try {
      const r = await api.deletePoints(
        info.name,
        {
          ids: ids.length ? ids : undefined,
          filter: filter || undefined,
          partition: deletePartition.trim() || undefined,
        },
        auth
      );
      onSuccess(`deleted ${r.deleted} point(s)`);
      setDeleteIds("");
      setDeleteFilter("");
      setDeletePartition("");
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`delete points failed: ${msg}`);
    } finally {
      setBusy(null);
    }
  };

  const runAnalyzer = async () => {
    setBusy("analyze");
    setAnalyzerResults(null);
    try {
      const useCollection = analyzeFromCollection && !!info.bm25TextField;
      let params: unknown = undefined;
      if (!useCollection && analyzeParams.trim()) {
        try {
          params = JSON.parse(analyzeParams);
        } catch {
          throw new Error("Analyzer params must be valid JSON");
        }
      }
      const r = await api.runAnalyzer(
        {
          text: [analyzeText],
          ...(useCollection ? { collection: info.name, field: info.bm25TextField! } : {}),
          ...(params !== undefined ? { analyzer_params: params } : {}),
          with_detail: true,
          with_hash: true,
        },
        auth
      );
      setAnalyzerResults(r.results.map((x) => x.tokens));
      onSuccess(`analyzer produced ${r.results.reduce((n, x) => n + x.tokens.length, 0)} tokens`);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`analyze failed: ${msg}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <>
      <section className="card">
        <h3>Maintenance — {info.name}</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Reindex rebuilds the in-memory HNSW graph from stored vectors. WAL compaction
          rewrites the write-ahead log against current state. Both are safe online.
        </p>
        <div className="row" style={{ flexWrap: "wrap", gap: 8 }}>
          <button
            disabled={!!busy}
            onClick={() => wrap("reindex", () => api.reindex(info.name, auth))}
          >
            {busy === "reindex" ? "Reindexing…" : "Reindex collection"}
          </button>
          <button
            disabled={!!busy}
            onClick={() => wrap("compact-wal", () => api.compactWal(false, auth))}
          >
            {busy === "compact-wal" ? "Compacting…" : "Compact WAL"}
          </button>
          <button
            disabled={!!busy}
            onClick={() =>
              wrap("compact-wal+snapshot", () => api.compactWal(true, auth), true)
            }
          >
            Compact WAL + snapshot first
          </button>
          <span className="grow" style={{ flex: 1 }} />
          <button className="danger" disabled={!!busy} onClick={deleteCollection}>
            Delete collection…
          </button>
        </div>
      </section>

      <section className="card">
        <h3>Delete points</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Delete by IDs and/or a Milvus-style boolean expression. When both are set,
          the engine deletes the union. Optional partition scope.
        </p>
        <div className="form-grid">
          <div className="field-full">
            <label>
              IDs <span className="hint">— comma- or newline-separated</span>
            </label>
            <textarea
              value={deleteIds}
              onChange={(e) => setDeleteIds(e.target.value)}
              placeholder="doc-1, doc-2, doc-3"
              rows={2}
            />
          </div>
          <div className="field-full">
            <label>
              Filter expression{" "}
              <span className="hint">
                — e.g. <code>category == &quot;demo&quot; and ord {">"} 100</code>
              </span>
            </label>
            <input
              value={deleteFilter}
              onChange={(e) => setDeleteFilter(e.target.value)}
              placeholder='category == "demo"'
            />
          </div>
          <div>
            <label>Partition (optional)</label>
            <input
              value={deletePartition}
              onChange={(e) => setDeletePartition(e.target.value)}
              placeholder="_default"
            />
          </div>
        </div>
        <div className="row">
          <button
            className="danger"
            disabled={!!busy || (!deleteIds.trim() && !deleteFilter.trim())}
            onClick={runDelete}
          >
            {busy === "delete-points" ? "Deleting…" : "Delete points"}
          </button>
        </div>
      </section>

      <section className="card">
        <h3>BM25 analyzer — try it</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Calls <code>POST /v1/admin/analyze</code> (Milvus parity) to show exactly
          how text gets tokenized before BM25 indexing or querying.
        </p>
        <div className="form-grid">
          <div className="field-full">
            <label>Text</label>
            <textarea
              value={analyzeText}
              onChange={(e) => setAnalyzeText(e.target.value)}
              rows={2}
            />
          </div>
          <div className="field-full">
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                margin: 0,
                textTransform: "none",
                letterSpacing: 0,
                color: analyzeFromCollection ? "var(--accent)" : "var(--text-muted)",
                cursor: info.bm25TextField ? "pointer" : "not-allowed",
              }}
            >
              <input
                type="checkbox"
                checked={analyzeFromCollection && !!info.bm25TextField}
                disabled={!info.bm25TextField}
                onChange={(e) => setAnalyzeFromCollection(e.target.checked)}
                style={{ width: "auto" }}
              />
              Use this collection&apos;s analyzer
              {!info.bm25TextField && (
                <span className="hint" style={{ marginLeft: 6 }}>
                  (no BM25 field on {info.name})
                </span>
              )}
            </label>
          </div>
          {!(analyzeFromCollection && info.bm25TextField) && (
            <div className="field-full">
              <label>
                Analyzer params{" "}
                <span className="hint">— JSON, Milvus-style</span>
              </label>
              <textarea
                value={analyzeParams}
                onChange={(e) => setAnalyzeParams(e.target.value)}
                rows={2}
              />
            </div>
          )}
        </div>
        <div className="row">
          <button className="primary" disabled={!!busy} onClick={runAnalyzer}>
            {busy === "analyze" ? "Analysing…" : "Run analyzer"}
          </button>
        </div>
        {analyzerResults && (
          <div style={{ marginTop: 14 }}>
            {analyzerResults.map((tokens, ri) => (
              <div key={ri} style={{ marginBottom: 12 }}>
                <div className="hint" style={{ marginBottom: 6 }}>
                  Result {ri + 1} — {tokens.length} token(s)
                </div>
                <div className="row" style={{ flexWrap: "wrap", gap: 4 }}>
                  {tokens.map((t, i) => (
                    <span
                      key={`${ri}-${i}`}
                      className="badge violet"
                      title={`pos ${t.position} · offsets ${t.start_offset}–${t.end_offset} · hash ${t.hash}`}
                    >
                      {t.token}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="card">
        <h3>Snapshots</h3>
        <div className="row" style={{ marginBottom: 12 }}>
          <button
            className="primary"
            disabled={!!busy}
            onClick={() => wrap("create-snapshot", () => api.createSnapshot(auth), true)}
          >
            {busy === "create-snapshot" ? "Creating…" : "Create snapshot"}
          </button>
          <button disabled={!!busy} onClick={reloadSnapshots}>
            Refresh
          </button>
          <span className="hint">{snapshots.length} snapshot(s)</span>
        </div>
        {snapshots.length === 0 ? (
          <div className="empty">No snapshots</div>
        ) : (
          <table className="results">
            <thead>
              <tr>
                <th>id</th>
                <th>created</th>
                <th>path</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {snapshots.map((s) => (
                <tr key={s.id}>
                  <td>{s.id}</td>
                  <td>{s.created_at_ms ? new Date(s.created_at_ms).toLocaleString() : "—"}</td>
                  <td style={{ wordBreak: "break-all" }}>{s.path}</td>
                  <td>
                    <button
                      className="danger"
                      disabled={!!busy}
                      onClick={() =>
                        wrap(
                          `delete-snapshot ${s.id}`,
                          () => api.deleteSnapshot(s.id, auth),
                          true
                        )
                      }
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </>
  );
}
