import { useEffect, useMemo, useState } from "react";
import { api, ApiError, buildCollectionInfo } from "./api";
import { useAuth } from "./auth";
import type { CollectionInfo, ServerConfig } from "./types";
import { HealthBar } from "./components/HealthBar";
import { CollectionList } from "./components/CollectionList";
import { CollectionDetail } from "./components/CollectionDetail";
import { SearchPanel } from "./components/SearchPanel";
import { UpsertPanel } from "./components/UpsertPanel";
import { PdfUploadPanel } from "./components/PdfUploadPanel";
import { BrowsePanel } from "./components/BrowsePanel";
import { AdminPanel } from "./components/AdminPanel";
import { CreateCollectionDialog } from "./components/CreateCollectionDialog";
import { LoginDialog } from "./components/LoginDialog";

type Tab = "overview" | "search" | "browse" | "upsert" | "pdf" | "admin";

interface Toast {
  level: "error" | "success";
  message: string;
}

export default function App() {
  const auth = useAuth();
  const [config, setConfig] = useState<ServerConfig | null>(null);

  const [selected, setSelected] = useState<string | null>(null);
  const [info, setInfo] = useState<CollectionInfo | null>(null);
  const [tab, setTab] = useState<Tab>("overview");
  const [refreshKey, setRefreshKey] = useState(0);
  const [toast, setToast] = useState<Toast | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [loginOpen, setLoginOpen] = useState(false);

  // Load /config.json from the Go backend.
  useEffect(() => {
    api.config().then(setConfig).catch(() => setConfig(null));
  }, []);

  // Load the selected collection details. We pull both /stats (structured)
  // and the legacy describe (debug spec — needed for HNSW params) in parallel
  // and merge them so we always show as much as possible.
  useEffect(() => {
    if (!selected) {
      setInfo(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const [statsRes, descRes] = await Promise.allSettled([
          api.collectionStats(selected, auth.headers),
          api.describeCollection(selected, auth.headers),
        ]);
        if (cancelled) return;
        const stats = statsRes.status === "fulfilled" ? statsRes.value : null;
        const desc = descRes.status === "fulfilled" ? descRes.value : null;
        if (!stats && !desc) {
          const err = descRes.status === "rejected" ? descRes.reason : statsRes.status === "rejected" ? statsRes.reason : null;
          throw err ?? new Error("collection lookup failed");
        }
        setInfo(buildCollectionInfo(selected, stats, desc));
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
  }, [selected, auth.headers, refreshKey]);

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

  const handleLogout = async () => {
    const tokenId = auth.session?.tokenId;
    auth.logout();
    if (tokenId) {
      // Best-effort revoke — works as long as the bearer is still valid.
      try {
        await api.revokeToken(tokenId, auth.headers);
      } catch {
        /* token may already be revoked or the gateway is down */
      }
    }
    showSuccess("Logged out");
  };

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

        {auth.session ? (
          <span className="auth-badge user" title={`Token ${auth.session.tokenId}`}>
            <span className="dot" />
            <span className="user-name">{auth.session.user}</span>
            <button
              type="button"
              className="linklike"
              onClick={handleLogout}
              title="Revoke this token and log out"
            >
              log out
            </button>
          </span>
        ) : (
          <button
            type="button"
            onClick={() => setLoginOpen(true)}
            title="Log in with username + password (RBAC)"
          >
            Log in…
          </button>
        )}

        <input
          type="password"
          placeholder="API key (fallback)"
          value={auth.apiKey}
          onChange={(e) => auth.setApiKey(e.target.value)}
          style={{ width: 220 }}
          aria-label="API key"
          title="Legacy x-api-key fallback. Bearer token from login takes precedence when set."
        />
        <HealthBar auth={auth.headers} />
      </header>

      <CollectionList
        auth={auth.headers}
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
              <li>
                Run dense, BM25, hybrid (RRF / weighted), and multi-vector
                hybrid searches; click any hit to inspect its payload.
              </li>
              <li>
                Paginate every point in a collection with the new <strong>Browse</strong> tab
                (cursor-based scroll).
              </li>
              <li>
                Create snapshots, compact the WAL, run a reindex, delete by filter
                expression, or test the BM25 tokenizer.
              </li>
            </ul>
            {!auth.configured && (
              <p
                style={{
                  marginTop: 14,
                  padding: "10px 12px",
                  borderRadius: 8,
                  border: "1px solid rgba(245,158,11,0.45)",
                  background: "rgba(245,158,11,0.06)",
                  color: "var(--amber)",
                  fontSize: 13,
                }}
              >
                No credentials configured. Click <strong>Log in</strong> (default
                <code> root</code> / <code>VexaDb!</code>) or paste your API key.
              </p>
            )}
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
              {(["overview", "search", "browse", "upsert", "pdf", "admin"] as const).map(
                (t) => (
                  <div
                    key={t}
                    className={`tab ${tab === t ? "active" : ""}`}
                    onClick={() => setTab(t)}
                  >
                    {t === "pdf" ? "PDF" : t.charAt(0).toUpperCase() + t.slice(1)}
                  </div>
                )
              )}
            </div>

            {tab === "overview" && <CollectionDetail info={info} />}

            {tab === "search" && (
              <SearchPanel
                auth={auth.headers}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
              />
            )}

            {tab === "browse" && (
              <BrowsePanel
                auth={auth.headers}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
              />
            )}

            {tab === "upsert" && (
              <UpsertPanel
                auth={auth.headers}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
                onUpserted={() => setRefreshKey((k) => k + 1)}
              />
            )}

            {tab === "pdf" && (
              <PdfUploadPanel
                auth={auth.headers}
                info={info}
                onError={showError}
                onSuccess={showSuccess}
                onUpserted={() => setRefreshKey((k) => k + 1)}
              />
            )}

            {tab === "admin" && (
              <AdminPanel
                auth={auth.headers}
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
        auth={auth.headers}
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreated={(name) => {
          setRefreshKey((k) => k + 1);
          setSelected(name);
          showSuccess(`created ${name}`);
        }}
        onError={showError}
      />

      <LoginDialog
        open={loginOpen}
        onClose={() => setLoginOpen(false)}
        onLogin={(session) => {
          auth.setSession(session);
          showSuccess(`Logged in as ${session.user}`);
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
