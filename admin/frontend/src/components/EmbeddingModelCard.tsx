import { useEffect, useState } from "react";
import { api } from "../api";
import { embedViaProxy } from "../embedApi";
import {
  loadEmbeddingSettings,
  NOMIC_V15_DIM,
  NOMIC_V15_MODEL,
  PRESETS,
  saveEmbeddingSettings,
  type EmbeddingSettings,
  type EmbedProvider,
} from "../embeddingSettings";

interface Props {
  collectionDim: number;
  bm25TextField: string | null;
  text: string;
  onTextChange: (t: string) => void;
  onEmbedded: (vec: number[], text: string) => void;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
  /** When true, text is edited in the parent Smart embed block */
  hideTextInput?: boolean;
}

export function EmbeddingModelCard({
  collectionDim,
  bm25TextField,
  text,
  onTextChange,
  onEmbedded,
  onError,
  onSuccess,
  hideTextInput = false,
}: Props) {
  const [settings, setSettings] = useState<EmbeddingSettings>(loadEmbeddingSettings);
  const [busy, setBusy] = useState(false);
  const [showSettings, setShowSettings] = useState(false);

  // When admin runs in Docker, /config.json supplies host.docker.internal defaults.
  useEffect(() => {
    api.config().then((cfg) => {
      const d = cfg.embedDefaults;
      if (!d?.baseUrl) return;
      const saved = localStorage.getItem("vectordb-admin-embedding");
      if (saved) return;
      setSettings({
        provider: d.provider === "ollama" ? "ollama" : "openai",
        baseUrl: d.baseUrl,
        model: d.model || settings.model,
        apiKey: "",
      });
    }).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    saveEmbeddingSettings(settings);
  }, [settings]);

  const dimMismatch =
    collectionDim > 0 && settings.model.includes("nomic") && collectionDim !== NOMIC_V15_DIM;

  const applyPreset = (key: keyof typeof PRESETS) => {
    const p = PRESETS[key];
    setSettings((s) => ({
      ...s,
      provider: p.provider,
      baseUrl: p.baseUrl,
      model: p.model,
    }));
    onSuccess(`Preset: ${p.label}`);
  };

  const runEmbed = async () => {
    if (!text.trim()) {
      onError("Type text to embed first");
      return;
    }
    setBusy(true);
    try {
      const t0 = performance.now();
      const result = await embedViaProxy(text, settings);
      const ms = Math.round(performance.now() - t0);
      if (collectionDim > 0 && result.dimensions !== collectionDim) {
        onError(
          `Model returned ${result.dimensions} dims but this collection expects ${collectionDim}. ` +
            (settings.model.includes("nomic")
              ? `Create a new collection with dimension ${result.dimensions} (nomic-embed-text-v1.5 = ${NOMIC_V15_DIM}).`
              : "Pick a collection that matches the model output size.")
        );
        return;
      }
      onEmbedded(result.embedding, text);
      onSuccess(
        `Embedded with ${settings.model} → ${result.dimensions} dims in ${ms}ms`
      );
    } catch (e) {
      onError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      className="field-full"
      style={{
        padding: 12,
        borderRadius: 8,
        border: "1px solid rgba(6, 182, 212, 0.35)",
        background: "rgba(6, 182, 212, 0.06)",
      }}
    >
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <label style={{ margin: 0, color: "var(--accent)" }}>
          Embedding model <span className="badge accent">recommended</span>
        </label>
        <button
          type="button"
          onClick={() => setShowSettings((v) => !v)}
          style={{ padding: "2px 10px", fontSize: 12 }}
        >
          {showSettings ? "Hide settings" : "Settings"}
        </button>
      </div>

      <p className="hint" style={{ margin: "0 0 10px", fontSize: 11 }}>
        Uses your local server (LM Studio, Ollama). Model ID:{" "}
        <code>{NOMIC_V15_MODEL}</code> → <strong>{NOMIC_V15_DIM} dimensions</strong>.
        {collectionDim > 0 && collectionDim !== NOMIC_V15_DIM && (
          <span style={{ color: "var(--amber)", display: "block", marginTop: 4 }}>
            This collection is dim={collectionDim}; nomic needs dim={NOMIC_V15_DIM} — create a
            new collection or use hash embed below.
          </span>
        )}
      </p>

      {showSettings && (
        <div className="form-grid" style={{ marginBottom: 10 }}>
          <div className="field-full">
            <label>Preset</label>
            <div className="row" style={{ flexWrap: "wrap", gap: 6 }}>
              {(Object.keys(PRESETS) as (keyof typeof PRESETS)[]).map((k) => (
                <button key={k} type="button" onClick={() => applyPreset(k)}>
                  {PRESETS[k].label}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label>Provider</label>
            <select
              value={settings.provider}
              onChange={(e) =>
                setSettings((s) => ({ ...s, provider: e.target.value as EmbedProvider }))
              }
            >
              <option value="openai">OpenAI-compatible (LM Studio)</option>
              <option value="ollama">Ollama</option>
            </select>
          </div>
          <div>
            <label>Model</label>
            <input
              value={settings.model}
              onChange={(e) => setSettings((s) => ({ ...s, model: e.target.value }))}
              placeholder={NOMIC_V15_MODEL}
            />
          </div>
          <div className="field-full">
            <label>Base URL</label>
            <input
              value={settings.baseUrl}
              onChange={(e) => setSettings((s) => ({ ...s, baseUrl: e.target.value }))}
              placeholder="http://127.0.0.1:1234/v1"
            />
          </div>
          <div className="field-full">
            <label>API key (optional)</label>
            <input
              type="password"
              value={settings.apiKey}
              onChange={(e) => setSettings((s) => ({ ...s, apiKey: e.target.value }))}
              placeholder="only if your server requires it"
            />
          </div>
        </div>
      )}

      {hideTextInput ? (
        <div className="row" style={{ gap: 6 }}>
          <button
            type="button"
            onClick={runEmbed}
            disabled={busy || !text.trim() || dimMismatch}
            title={dimMismatch ? `Collection dim ${collectionDim} ≠ model dim ${NOMIC_V15_DIM}` : ""}
          >
            {busy ? "Embedding…" : "Force model embed (uses text above)"}
          </button>
        </div>
      ) : (
        <div className="row" style={{ gap: 6 }}>
          <input
            placeholder="books is good, how many books will help me…"
            value={text}
            onChange={(e) => onTextChange(e.target.value)}
            style={{ flex: 1 }}
          />
          <button
            type="button"
            className="primary"
            onClick={runEmbed}
            disabled={busy || !text.trim() || dimMismatch}
            title={dimMismatch ? `Collection dim ${collectionDim} ≠ model dim ${NOMIC_V15_DIM}` : ""}
          >
            {busy ? "Embedding…" : "Embed with model → fill values"}
          </button>
        </div>
      )}
      {!showSettings && (
        <p className="hint" style={{ margin: "6px 0 0", fontSize: 11 }}>
          {settings.provider} · {settings.baseUrl} · <code>{settings.model}</code>
        </p>
      )}
      {bm25TextField && (
        <p className="hint" style={{ margin: "4px 0 0", fontSize: 11 }}>
          Payload.{bm25TextField} is filled automatically when empty.
        </p>
      )}
    </div>
  );
}
