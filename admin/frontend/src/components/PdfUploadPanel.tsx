import { useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError, type PdfChunk, type PdfExtractResult } from "../api";
import { embedViaProxy } from "../embedApi";
import {
  loadEmbeddingSettings,
  saveEmbeddingSettings,
  PRESETS,
  NOMIC_V15_DIM,
  type EmbeddingSettings,
} from "../embeddingSettings";
import { hashEmbed } from "../embed";
import type { CollectionInfo } from "../types";

interface Props {
  apiKey: string;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
  onUpserted: () => void;
}

type Phase = "idle" | "extracting" | "embedding" | "upserting" | "done" | "error";

interface UploadProgress {
  phase: Phase;
  total: number;
  done: number;
  embedFailed: number;
  startedAt: number;
  message?: string;
}

const sanitizeBaseId = (filename: string): string => {
  const stem = filename.replace(/\.[^.]+$/, "");
  return (
    stem
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48) || "pdf"
  );
};

const fmtSecs = (ms: number) => {
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)}s`;
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
};

export function PdfUploadPanel({ apiKey, info, onError, onSuccess, onUpserted }: Props) {
  const dim = info.dimension ?? 0;
  const dimMissing = dim === 0;

  const [embedSettings, setEmbedSettings] = useState<EmbeddingSettings>(() =>
    loadEmbeddingSettings()
  );
  useEffect(() => saveEmbeddingSettings(embedSettings), [embedSettings]);

  const [file, setFile] = useState<File | null>(null);
  const [chunkSize, setChunkSize] = useState(1200);
  const [chunkOverlap, setChunkOverlap] = useState(150);
  const [docIdPrefix, setDocIdPrefix] = useState("");
  const [batchSize, setBatchSize] = useState(64);
  const [useHashFallback, setUseHashFallback] = useState(true);

  // Server-side extraction result cached so the user can preview before
  // committing to embed + upsert. Cleared whenever a new file is selected.
  const [extracted, setExtracted] = useState<PdfExtractResult | null>(null);
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [previewIdx, setPreviewIdx] = useState(0);
  const cancelRef = useRef(false);

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const dropRef = useRef<HTMLDivElement | null>(null);
  const [dragActive, setDragActive] = useState(false);

  const dimMismatch = useMemo(() => dim > 0 && dim !== NOMIC_V15_DIM, [dim]);

  const onPickFile = (f: File | null) => {
    if (!f) return;
    if (!/\.pdf$/i.test(f.name) && f.type !== "application/pdf") {
      onError(`Not a PDF: ${f.name}`);
      return;
    }
    setFile(f);
    setExtracted(null);
    setProgress(null);
    if (!docIdPrefix.trim()) {
      setDocIdPrefix(`pdf-${sanitizeBaseId(f.name)}`);
    }
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragActive(false);
    const f = e.dataTransfer?.files?.[0];
    if (f) onPickFile(f);
  };

  const handleExtract = async () => {
    if (!file) {
      onError("Pick a PDF first");
      return;
    }
    cancelRef.current = false;
    setProgress({ phase: "extracting", total: 0, done: 0, embedFailed: 0, startedAt: Date.now() });
    try {
      const r = await api.extractPdf(file, chunkSize, chunkOverlap);
      setExtracted(r);
      setPreviewIdx(0);
      setProgress(null);
      if (r.chunks.length === 0) {
        onError("PDF had no extractable text (image-only scan?). Try a different file.");
      } else {
        onSuccess(`Extracted ${r.chunks.length} chunks from ${r.pages} pages (${r.chars.toLocaleString()} chars)`);
      }
    } catch (e) {
      setProgress({
        phase: "error",
        total: 0,
        done: 0,
        embedFailed: 0,
        startedAt: 0,
        message: (e as Error).message,
      });
      onError(`extract failed: ${(e as Error).message}`);
    }
  };

  const cancel = () => {
    cancelRef.current = true;
  };

  // Embed a single chunk with model + optional hash fallback. Bubbles errors
  // up only when both paths fail.
  const embedOne = async (text: string): Promise<{ vec: number[]; viaModel: boolean }> => {
    try {
      const r = await embedViaProxy(text, embedSettings);
      if (r.dimensions !== dim) {
        throw new Error(
          `model returned ${r.dimensions}-dim vectors but collection expects ${dim}`
        );
      }
      return { vec: r.embedding, viaModel: true };
    } catch (modelErr) {
      if (useHashFallback && dim > 0) {
        return { vec: hashEmbed(text, dim), viaModel: false };
      }
      throw modelErr;
    }
  };

  const handleEmbedAndUpsert = async () => {
    if (!extracted || extracted.chunks.length === 0) {
      onError("Run extract first");
      return;
    }
    if (dimMissing) {
      onError("Collection dimension unknown");
      return;
    }
    cancelRef.current = false;
    const total = extracted.chunks.length;
    setProgress({
      phase: "embedding",
      total,
      done: 0,
      embedFailed: 0,
      startedAt: Date.now(),
    });

    const filename = extracted.filename;
    const bm25Field = info.bm25TextField ?? "text";
    let upsertedTotal = 0;
    let embedFailures = 0;
    let modelHits = 0;
    let hashHits = 0;

    try {
      // Process in batches: embed in parallel inside a batch, then one upsert.
      // Bounded concurrency keeps LM Studio responsive on a typical laptop.
      for (let start = 0; start < total; start += batchSize) {
        if (cancelRef.current) {
          throw new Error("cancelled by user");
        }
        const end = Math.min(start + batchSize, total);
        const batch = extracted.chunks.slice(start, end);

        const embedded = await Promise.all(
          batch.map(async (chunk: PdfChunk) => {
            try {
              const { vec, viaModel } = await embedOne(chunk.text);
              if (viaModel) modelHits++;
              else hashHits++;
              return {
                id: `${docIdPrefix || "pdf"}-${String(chunk.idx).padStart(5, "0")}`,
                values: vec,
                payload: {
                  [bm25Field]: chunk.text,
                  source: filename,
                  chunk: chunk.idx,
                  page: chunk.page,
                  char_start: chunk.char_start,
                  char_end: chunk.char_end,
                },
              };
            } catch (e) {
              embedFailures++;
              // Surface the first error so the user knows what's wrong, but
              // don't tear down the whole pipeline — they can retry the rest.
              if (embedFailures === 1) {
                onError(`embed chunk ${chunk.idx} failed: ${(e as Error).message}`);
              }
              return null;
            }
          })
        );
        const points = embedded.filter((p): p is NonNullable<typeof p> => !!p);
        if (points.length > 0) {
          setProgress((p) =>
            p ? { ...p, phase: "upserting", done: start + batch.length - points.length } : p
          );
          const r = await api.upsertPoints(info.name, points, apiKey || undefined);
          upsertedTotal += r.upserted;
        }
        setProgress((p) =>
          p
            ? {
                ...p,
                phase: end < total ? "embedding" : "done",
                done: end,
                embedFailed: embedFailures,
              }
            : p
        );
      }

      const elapsed = Date.now() - (progress?.startedAt ?? Date.now());
      const note =
        hashHits > 0 ? ` (model: ${modelHits}, hash fallback: ${hashHits})` : ` (model: ${modelHits})`;
      onSuccess(
        `PDF "${filename}" → ${upsertedTotal}/${total} points into ${info.name} in ${fmtSecs(
          elapsed
        )}${note}`
      );
      onUpserted();
    } catch (e) {
      setProgress((p) =>
        p ? { ...p, phase: "error", message: (e as Error).message } : null
      );
      onError(`PDF upload failed: ${(e as Error).message}`);
    }
  };

  const isBusy =
    !!progress &&
    progress.phase !== "idle" &&
    progress.phase !== "done" &&
    progress.phase !== "error";

  const previewChunk = extracted?.chunks[previewIdx];

  return (
    <>
      <section className="card">
        <h3>Upload a PDF</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          The PDF is parsed server-side, split into overlapping text chunks, embedded with your
          configured model, and upserted into <strong>{info.name}</strong>. Each point gets a
          payload:{" "}
          <code>
            {`{ ${info.bm25TextField ?? "text"}, source, chunk, page, char_start, char_end }`}
          </code>
          .
        </p>

        {dimMismatch && (
          <p className="hint" style={{ color: "var(--amber)", marginTop: 0 }}>
            Heads-up: collection dim is <strong>{dim}</strong>, but nomic-embed-text-v1.5 emits
            768. Either change the embedding model below, or recreate the collection with dim 768
            for semantic search to work.
          </p>
        )}

        <div
          ref={dropRef}
          onDragOver={(e) => {
            e.preventDefault();
            setDragActive(true);
          }}
          onDragLeave={() => setDragActive(false)}
          onDrop={handleDrop}
          onClick={() => fileInputRef.current?.click()}
          style={{
            cursor: "pointer",
            border: "2px dashed",
            borderColor: dragActive ? "var(--accent)" : "rgba(139, 92, 246, 0.45)",
            background: dragActive ? "rgba(6, 182, 212, 0.08)" : "rgba(139, 92, 246, 0.05)",
            borderRadius: 10,
            padding: "28px 18px",
            textAlign: "center",
            marginBottom: 14,
            transition: "border-color .15s, background .15s",
          }}
        >
          <input
            ref={fileInputRef}
            type="file"
            accept="application/pdf,.pdf"
            style={{ display: "none" }}
            onChange={(e) => onPickFile(e.target.files?.[0] ?? null)}
          />
          {file ? (
            <>
              <div style={{ color: "var(--accent)", fontWeight: 600 }}>{file.name}</div>
              <div className="hint" style={{ marginTop: 4 }}>
                {(file.size / 1024 / 1024).toFixed(2)} MB — click to choose a different file
              </div>
            </>
          ) : (
            <>
              <div style={{ fontSize: 14 }}>Drop a PDF here, or click to browse</div>
              <div className="hint" style={{ marginTop: 4 }}>Max 25 MB</div>
            </>
          )}
        </div>

        <div className="form-grid">
          <div>
            <label>Chunk size (chars)</label>
            <input
              type="number"
              min={200}
              max={8000}
              value={chunkSize}
              onChange={(e) => setChunkSize(Math.max(200, Number(e.target.value) || 1200))}
            />
          </div>
          <div>
            <label>Chunk overlap (chars)</label>
            <input
              type="number"
              min={0}
              max={2000}
              value={chunkOverlap}
              onChange={(e) => setChunkOverlap(Math.max(0, Number(e.target.value) || 0))}
            />
          </div>
          <div>
            <label>Document id prefix</label>
            <input
              value={docIdPrefix}
              onChange={(e) => setDocIdPrefix(e.target.value)}
              placeholder="pdf-paper"
            />
          </div>
          <div>
            <label>Embed batch size</label>
            <input
              type="number"
              min={1}
              max={256}
              value={batchSize}
              onChange={(e) => setBatchSize(Math.max(1, Math.min(256, Number(e.target.value) || 32)))}
            />
          </div>
        </div>

        <div className="row" style={{ marginTop: 12, gap: 8, flexWrap: "wrap" }}>
          <button
            className="primary"
            disabled={!file || isBusy}
            onClick={handleExtract}
          >
            {progress?.phase === "extracting" ? "Extracting…" : "1. Extract chunks"}
          </button>
          <button
            className="primary"
            disabled={!extracted || extracted.chunks.length === 0 || isBusy || dimMissing}
            onClick={handleEmbedAndUpsert}
          >
            {progress?.phase === "embedding"
              ? `Embedding ${progress.done}/${progress.total}…`
              : progress?.phase === "upserting"
                ? `Upserting ${progress.done}/${progress.total}…`
                : "2. Embed + upsert into collection"}
          </button>
          {isBusy && (
            <button onClick={cancel}>Cancel</button>
          )}
          <span className="grow" style={{ flex: 1 }} />
          <label
            className="hint"
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              margin: 0,
              textTransform: "none",
              letterSpacing: 0,
              cursor: "pointer",
            }}
          >
            <input
              type="checkbox"
              checked={useHashFallback}
              onChange={(e) => setUseHashFallback(e.target.checked)}
              style={{ width: "auto" }}
            />
            Hash fallback if model fails
          </label>
        </div>

        {progress && (
          <div style={{ marginTop: 14 }}>
            <div className="hint" style={{ marginBottom: 6 }}>
              {progress.phase === "extracting" && "Parsing PDF on the server…"}
              {progress.phase === "embedding" &&
                `Embedding chunks via ${embedSettings.model}: ${progress.done}/${progress.total}`}
              {progress.phase === "upserting" &&
                `Upserting batch into ${info.name}: ${progress.done}/${progress.total}`}
              {progress.phase === "done" &&
                `Done: ${progress.done} processed${progress.embedFailed ? `, ${progress.embedFailed} embed errors` : ""}`}
              {progress.phase === "error" && (
                <span style={{ color: "var(--amber)" }}>Error: {progress.message}</span>
              )}
            </div>
            {progress.total > 0 && (
              <div
                style={{
                  height: 6,
                  background: "var(--surface-2)",
                  borderRadius: 3,
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    height: "100%",
                    width: `${(progress.done / progress.total) * 100}%`,
                    background:
                      progress.phase === "error"
                        ? "var(--amber)"
                        : "linear-gradient(90deg, var(--accent), var(--violet))",
                    transition: "width .15s",
                  }}
                />
              </div>
            )}
          </div>
        )}
      </section>

      <section className="card">
        <h3>Embedding model</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          The admin backend proxies to this URL — same settings as the Upsert tab. Pick a preset
          or enter your own.
        </p>
        <div className="row" style={{ gap: 6, marginBottom: 8, flexWrap: "wrap" }}>
          {Object.values(PRESETS).map((p) => (
            <button
              key={p.label}
              type="button"
              onClick={() =>
                setEmbedSettings({
                  provider: p.provider,
                  baseUrl: p.baseUrl,
                  model: p.model,
                  apiKey: embedSettings.apiKey,
                })
              }
            >
              {p.label}
            </button>
          ))}
        </div>
        <div className="form-grid">
          <div>
            <label>Provider</label>
            <select
              value={embedSettings.provider}
              onChange={(e) =>
                setEmbedSettings({
                  ...embedSettings,
                  provider: e.target.value === "ollama" ? "ollama" : "openai",
                })
              }
            >
              <option value="openai">openai (LM Studio, OpenAI)</option>
              <option value="ollama">ollama</option>
            </select>
          </div>
          <div>
            <label>Model</label>
            <input
              value={embedSettings.model}
              onChange={(e) => setEmbedSettings({ ...embedSettings, model: e.target.value })}
            />
          </div>
          <div className="field-full">
            <label>Base URL</label>
            <input
              value={embedSettings.baseUrl}
              onChange={(e) => setEmbedSettings({ ...embedSettings, baseUrl: e.target.value })}
            />
          </div>
          <div className="field-full">
            <label>API key (optional)</label>
            <input
              type="password"
              value={embedSettings.apiKey}
              onChange={(e) => setEmbedSettings({ ...embedSettings, apiKey: e.target.value })}
              placeholder="leave empty for local models"
            />
          </div>
        </div>
      </section>

      {extracted && extracted.chunks.length > 0 && (
        <section className="card">
          <h3>
            Preview — chunk{" "}
            <span className="hint" style={{ fontSize: 13 }}>
              {previewIdx + 1} / {extracted.chunks.length}
              {previewChunk ? ` · page ${previewChunk.page}` : ""}
            </span>
          </h3>
          <div className="row" style={{ gap: 6, marginBottom: 8 }}>
            <button
              disabled={previewIdx === 0}
              onClick={() => setPreviewIdx((i) => Math.max(0, i - 1))}
            >
              ← Prev
            </button>
            <button
              disabled={previewIdx >= extracted.chunks.length - 1}
              onClick={() =>
                setPreviewIdx((i) => Math.min(extracted.chunks.length - 1, i + 1))
              }
            >
              Next →
            </button>
            <span className="grow" style={{ flex: 1 }} />
            <span className="hint">
              {extracted.pages} pages · {extracted.chars.toLocaleString()} chars ·{" "}
              {extracted.chunks.length} chunks
            </span>
          </div>
          <pre
            style={{
              maxHeight: 320,
              overflow: "auto",
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              background: "var(--surface-2)",
              padding: 12,
              borderRadius: 6,
              fontSize: 12,
              lineHeight: 1.6,
              margin: 0,
            }}
          >
            {previewChunk?.text}
          </pre>
        </section>
      )}
    </>
  );
}
