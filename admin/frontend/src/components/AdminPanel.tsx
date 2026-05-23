import { useEffect, useState } from "react";
import { api, ApiError, type SnapshotInfo } from "../api";
import type { CollectionInfo } from "../types";

interface Props {
  apiKey: string;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
  onCollectionDeleted: () => void;
}

export function AdminPanel({
  apiKey,
  info,
  onError,
  onSuccess,
  onCollectionDeleted,
}: Props) {
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [busy, setBusy] = useState<string | null>(null);

  const reloadSnapshots = async () => {
    try {
      const list = await api.listSnapshots(apiKey || undefined);
      setSnapshots(list ?? []);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`list snapshots failed: ${msg}`);
    }
  };

  useEffect(() => {
    reloadSnapshots();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiKey]);

  const wrap = async (label: string, fn: () => Promise<unknown>, refresh = false) => {
    setBusy(label);
    try {
      const r = await fn();
      onSuccess(typeof r === "object" && r != null ? `${label} ✓ ${JSON.stringify(r)}` : `${label} ✓`);
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
      await api.deleteCollection(info.name, apiKey || undefined);
      onSuccess(`deleted collection ${info.name}`);
      onCollectionDeleted();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`delete collection failed: ${msg}`);
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
            onClick={() => wrap("reindex", () => api.reindex(info.name, apiKey || undefined))}
          >
            {busy === "reindex" ? "Reindexing…" : "Reindex collection"}
          </button>
          <button
            disabled={!!busy}
            onClick={() => wrap("compact-wal", () => api.compactWal(false, apiKey || undefined))}
          >
            {busy === "compact-wal" ? "Compacting…" : "Compact WAL"}
          </button>
          <button
            disabled={!!busy}
            onClick={() =>
              wrap("compact-wal+snapshot", () => api.compactWal(true, apiKey || undefined), true)
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
        <h3>Snapshots</h3>
        <div className="row" style={{ marginBottom: 12 }}>
          <button
            className="primary"
            disabled={!!busy}
            onClick={() => wrap("create-snapshot", () => api.createSnapshot(apiKey || undefined), true)}
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
                          () => api.deleteSnapshot(s.id, apiKey || undefined),
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
