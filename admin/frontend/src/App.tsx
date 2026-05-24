import { useEffect, useMemo, useState } from "react";
import { api, ApiError, parseCollectionInfo } from "./api";
import type { CollectionInfo, ServerConfig } from "./types";
import { HealthBar } from "./components/HealthBar";
import { CollectionList } from "./components/CollectionList";
import { CollectionDetail } from "./components/CollectionDetail";
import { SearchPanel } from "./components/SearchPanel";
import { UpsertPanel } from "./components/UpsertPanel";
import { PdfUploadPanel } from "./components/PdfUploadPanel";
import { AdminPanel } from "./components/AdminPanel";
import { CreateCollectionDialog } from "./components/CreateCollectionDialog";

type Tab = "overview" | "search" | "upsert" | "pdf" | "admin";

interface Toast {
  level: "error" | "success";
  message: string;
}

const API_KEY_STORAGE = "vectordb-admin-api-key";

export default function App() {
  const [config, setConfig] = useState<ServerConfig | null>(null);
  const [apiKey, setApiKey] = useState<string>(
    () => localStorage.getItem(API_KEY_STORAGE) ?? ""
  );

  const [selected, setSelected] = useState<string | null>(null);
  const [info, setInfo] = useState<CollectionInfo | null>(null);
  const [tab, setTab] = useState<Tab>("overview");
  const [refreshKey, setRefreshKey] = useState(0);
  const [toast, setToast] = useState<Toast | null>(null);
  const [createOpen, setCreateOpen] = useState(false);

  // Persist API key for convenience.
  useEffect(() => {
    if (apiKey) localStorage.setItem(API_KEY_STORAGE, apiKey);
    else localStorage.removeItem(API_KEY_STORAGE);
  }, [apiKey]);

  // Load /config.json from the Go backend.
  useEffect(() => {
    api.config().then(setConfig).catch(() => setConfig(null));
  }, []);

  // Load the selected collection details.
  useEffect(() => {
    if (!selected) {
      setInfo(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const desc = await api.describeCollection(selected, apiKey || undefined);
        if (!cancelled) setInfo(parseCollectionInfo(selected, desc));
      } catch (e) {
        if (!cancelled) {
          const msg = e instanceof ApiError ? e.body : (e as Error).message;
          showError(`describe ${selected} failed: ${msg}`);
          setInfo(null);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selected, apiKey, refreshKey]);

  // Auto-dismiss toasts.
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 5000);
    return () => clearTimeout(t);
  }, [toast]);

  const showError = (m: string) => setToast({ level: "error", message: m });
  const showSuccess = (m: string) => setToast({ level: "success", message: m });

  const upstream = useMemo(
    () => config?.upstream ?? "http://127.0.0.1:8080",
    [config]
  );

  return (
    <div className="layout">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" />
          <span>VectorDB Admin</span>
        </div>
        <span className="hint mono" style={{ marginLeft: 4 }}>
          → {upstream}
        </span>

        <span className="spacer" />

        <input
          type="password"
          placeholder="API key (optional)"
          value={apiKey}
          onChange={(e) => setApiKey(e.target.value)}
          style={{ width: 220 }}
          aria-label="API key"
        />
        <HealthBar apiKey={apiKey} />
      </header>

      <CollectionList
        apiKey={apiKey}
        selected={selected}
        onSelect={setSelected}
        refreshKey={refreshKey}
        onCreate={() => setCreateOpen(true)}
      />

      <main className="main">
        {!selected && (
          <div className="card">
            <h3>Welcome</h3>
            <p style={{ color: "var(--text-muted)", margin: "4px 0 12px" }}>
              Pick a collection on the left, or create a new one.
            </p>
            <ul style={{ color: "var(--text-muted)", margin: 0, paddingLeft: 20 }}>
              <li>Browse collection metadata: dimension, metric, indexes, vector count.</li>
              <li>Run dense, BM25, and hybrid searches; click any hit to inspect its payload.</li>
              <li>Create snapshots, compact the WAL, or trigger a reindex.</li>
            </ul>
          </div>
        )}

        {selected && info && (
          <>
            <div className="row" style={{ marginBottom: 14 }}>
              <h2 style={{ margin: 0, fontSize: 18 }}>{info.name}</h2>
              <span className="hint">
                {info.dimension ? `dim ${info.dimension}` : ""}
                {info.metric ? ` · ${info.metric.toLowerCase()}` : ""}
                {` · ${info.vectorCount.toLocaleString()} vectors`}
              </span>
              <span className="grow" style={{ flex: 1 }} />
              <button onClick={() => setRefreshKey((k) => k + 1)}>Refresh</button>
            </div>

            <div className="tabs">
              {(["overview", "search", "upsert", "pdf", "admin"] as const).map((t) => (
                <div
                  key={t}
                  className={`tab ${tab === t ? "active" : ""}`}
                  onClick={() => setTab(t)}
                >
                  {t === "pdf" ? "PDF" : t.charAt(0).toUpperCase() + t.slice(1)}
                </div>
              ))}
            </div>

            {tab === "overview" && <CollectionDetail info={info} />}

            {tab === "search" && (
              <SearchPanel apiKey={apiKey} info={info} onError={showError} onSuccess={showSuccess} />
            )}

            {tab === "upsert" && (
              <UpsertPanel
                apiKey={apiKey}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
                onUpserted={() => setRefreshKey((k) => k + 1)}
              />
            )}

            {tab === "pdf" && (
              <PdfUploadPanel
                apiKey={apiKey}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
                onUpserted={() => setRefreshKey((k) => k + 1)}
              />
            )}

            {tab === "admin" && (
              <AdminPanel
                apiKey={apiKey}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
                onCollectionDeleted={() => {
                  setSelected(null);
                  setRefreshKey((k) => k + 1);
                }}
              />
            )}
          </>
        )}

        {selected && !info && (
          <div className="card">
            <div className="empty">Loading collection…</div>
          </div>
        )}
      </main>

      <CreateCollectionDialog
        apiKey={apiKey}
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreated={(name) => {
          setRefreshKey((k) => k + 1);
          setSelected(name);
          showSuccess(`created ${name}`);
        }}
        onError={showError}
      />

      {toast && (
        <div className={`toast ${toast.level}`} onClick={() => setToast(null)}>
          {toast.message}
        </div>
      )}
    </div>
  );
}
