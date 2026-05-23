import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "../api";
import { NOMIC_V15_DIM } from "../embeddingSettings";

interface Props {
  apiKey: string;
  open: boolean;
  onClose: () => void;
  onCreated: (name: string) => void;
  onError: (msg: string) => void;
}

export function CreateCollectionDialog({ apiKey, open, onClose, onCreated, onError }: Props) {
  const ref = useRef<HTMLDialogElement | null>(null);
  const [name, setName] = useState("");
  const [dim, setDim] = useState(128);
  const [metric, setMetric] = useState<"cosine" | "euclidean" | "dot">("cosine");
  const [bm25, setBm25] = useState("text");
  const [enableBm25, setEnableBm25] = useState(true);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) el.showModal();
    if (!open && el.open) el.close();
  }, [open]);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    setBusy(true);
    try {
      await api.createCollection(
        {
          name: name.trim(),
          dimension: dim,
          metric,
          bm25_text_field: enableBm25 ? bm25 : undefined,
        },
        apiKey || undefined
      );
      onCreated(name.trim());
      onClose();
      setName("");
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`create collection failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <dialog
      ref={ref}
      className="modal"
      onClose={onClose}
      onClick={(e) => {
        // Close when clicking the backdrop
        if (e.target === ref.current) onClose();
      }}
    >
      <form onSubmit={submit}>
        <h2>New collection</h2>
        <div className="form-grid">
          <div className="field-full">
            <label>Name</label>
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. embeddings"
              required
              pattern="[A-Za-z0-9_\-]+"
              title="Letters, digits, underscores, hyphens"
            />
          </div>
          <div>
            <label>Dimension</label>
            <div className="row" style={{ gap: 6, marginBottom: 4 }}>
              <input
                type="number"
                min={1}
                max={65536}
                value={dim}
                onChange={(e) => setDim(Number(e.target.value) || 1)}
                required
                style={{ flex: 1 }}
              />
              <button
                type="button"
                title="nomic-embed-text-v1.5 outputs 768-dimensional vectors"
                onClick={() => setDim(NOMIC_V15_DIM)}
              >
                {NOMIC_V15_DIM} (nomic)
              </button>
            </div>
          </div>
          <div>
            <label>Metric</label>
            <select value={metric} onChange={(e) => setMetric(e.target.value as typeof metric)}>
              <option value="cosine">cosine</option>
              <option value="euclidean">euclidean</option>
              <option value="dot">dot</option>
            </select>
          </div>
          <div className="field-full">
            <label>
              <input
                type="checkbox"
                checked={enableBm25}
                onChange={(e) => setEnableBm25(e.target.checked)}
                style={{ width: "auto", marginRight: 8 }}
              />
              Enable BM25 text index on payload field
            </label>
            <input
              value={bm25}
              onChange={(e) => setBm25(e.target.value)}
              disabled={!enableBm25}
              placeholder="text"
            />
          </div>
        </div>
        <div className="row" style={{ justifyContent: "flex-end", marginTop: 8 }}>
          <button type="button" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="submit" className="primary" disabled={busy}>
            {busy ? "Creating…" : "Create"}
          </button>
        </div>
      </form>
    </dialog>
  );
}
