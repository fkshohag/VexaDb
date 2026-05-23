import { useMemo, useState } from "react";
import { api, ApiError } from "../api";
import { hashEmbed } from "../embed";
import { pickEmbedStrategy, embedSmart } from "../embeddingStrategy";
import { NOMIC_V15_DIM } from "../embeddingSettings";
import { EmbeddingModelCard } from "./EmbeddingModelCard";
import type { CollectionInfo } from "../types";

interface Props {
  apiKey: string;
  info: CollectionInfo;
  onError: (msg: string) => void;
  onSuccess: (msg: string) => void;
  onUpserted: () => void;
}

function randomUnitVector(dim: number): number[] {
  const v = new Array<number>(dim);
  let ss = 0;
  for (let i = 0; i < dim; i++) {
    v[i] = Math.random() - 0.5;
    ss += v[i] * v[i];
  }
  const norm = Math.sqrt(ss) || 1;
  for (let i = 0; i < dim; i++) v[i] = v[i] / norm;
  return v;
}

function tryParseJson<T = unknown>(s: string): { ok: true; value: T } | { ok: false; error: string } {
  try {
    return { ok: true, value: JSON.parse(s) as T };
  } catch (e) {
    return { ok: false, error: (e as Error).message };
  }
}

function makeExamplePoints(dim: number, bm25Field: string | null) {
  const vec1 = randomUnitVector(dim);
  const vec2 = randomUnitVector(dim);
  return [
    {
      id: "doc-1",
      values: vec1,
      payload: bm25Field
        ? { [bm25Field]: "Kafka is a distributed log", topic: "messaging" }
        : { topic: "messaging" },
    },
    {
      id: "doc-2",
      values: vec2,
      payload: bm25Field
        ? { [bm25Field]: "Redis is an in-memory cache", topic: "caching" }
        : { topic: "caching" },
    },
  ];
}

export function UpsertPanel({ apiKey, info, onError, onSuccess, onUpserted }: Props) {
  const dim = info.dimension ?? 0;

  // ── Single point ───────────────────────────────────────────────────────
  const [singleId, setSingleId] = useState("");
  const [singleValues, setSingleValues] = useState("");
  const [singlePayload, setSinglePayload] = useState("");
  const [singleBusy, setSingleBusy] = useState(false);
  const [embedText, setEmbedText] = useState("");
  const [smartBusy, setSmartBusy] = useState(false);

  // ── Bulk auto-embed ────────────────────────────────────────────────────
  const [bulkAutoEmbed, setBulkAutoEmbed] = useState(false);

  // ── Bulk JSON ──────────────────────────────────────────────────────────
  const [bulkText, setBulkText] = useState("");
  const [bulkBusy, setBulkBusy] = useState(false);

  // ── Quick generate ─────────────────────────────────────────────────────
  const [genCount, setGenCount] = useState(100);
  const [genPrefix, setGenPrefix] = useState(`gen-${Date.now()}`);
  const [genPayloadTpl, setGenPayloadTpl] = useState(
    info.bm25TextField ? `{"text": "synthetic point #{i}"}` : ""
  );
  const [genBusy, setGenBusy] = useState(false);
  const [genProgress, setGenProgress] = useState<{ done: number; total: number } | null>(null);

  const dimMissing = dim === 0;
  const dimHint = useMemo(() => (dim ? `${dim} dims` : "unknown dim"), [dim]);
  const embedPick = useMemo(() => pickEmbedStrategy(dim), [dim]);

  // ─── Single point handlers ─────────────────────────────────────────────

  const fillRandom = () => setSingleValues(JSON.stringify(randomUnitVector(dim || 4)));
  const fillZero = () => setSingleValues(JSON.stringify(new Array(dim || 4).fill(0)));

  const fillFromHashEmbed = () => {
    if (!embedText.trim()) {
      onError("Type some text first, then click Hash embed");
      return;
    }
    if (dimMissing) {
      onError("Collection dimension unknown");
      return;
    }
    const vec = hashEmbed(embedText, dim);
    setSingleValues(JSON.stringify(vec));
    if (info.bm25TextField && !singlePayload.trim()) {
      setSinglePayload(JSON.stringify({ [info.bm25TextField]: embedText }, null, 2));
    }
    onSuccess(`Hash-embedded ${embedText.length} chars → ${dim}-dim vector`);
  };

  const onModelEmbedded = (vec: number[], text: string) => {
    setSingleValues(JSON.stringify(vec));
    if (info.bm25TextField && !singlePayload.trim()) {
      setSinglePayload(JSON.stringify({ [info.bm25TextField]: text }, null, 2));
    }
  };

  const applyEmbedded = (vec: number[], text: string) => {
    onModelEmbedded(vec, text);
  };

  const runSmartEmbed = async () => {
    if (!embedText.trim()) {
      onError("Type text to embed first");
      return;
    }
    if (dimMissing) {
      onError("Collection dimension unknown");
      return;
    }
    setSmartBusy(true);
    try {
      const result = await embedSmart(embedText, dim, {
        fallbackToHash: embedPick.method === "model",
      });
      applyEmbedded(result.vector, embedText);
      const via =
        result.method === "model"
          ? `Semantic embed (${result.detail})`
          : `Hash embed (${result.detail})`;
      onSuccess(`${via} → ${result.dimensions} dims`);
    } catch (e) {
      onError((e as Error).message);
    } finally {
      setSmartBusy(false);
    }
  };

  const submitSingle = async () => {
    if (!singleId.trim()) {
      onError("Point id is required");
      return;
    }
    if (dimMissing) {
      onError("Cannot upsert: collection dimension unknown");
      return;
    }

    const vp = tryParseJson<unknown>(singleValues.trim() || "[]");
    if (!vp.ok) {
      onError(`Values JSON: ${vp.error}`);
      return;
    }
    if (!Array.isArray(vp.value) || (vp.value as unknown[]).some((x) => typeof x !== "number")) {
      onError("Values must be a JSON array of numbers");
      return;
    }
    const values = vp.value as number[];
    if (values.length !== dim) {
      onError(`Values has ${values.length} dims; collection expects ${dim}`);
      return;
    }

    let payload: unknown = undefined;
    if (singlePayload.trim()) {
      const pp = tryParseJson<unknown>(singlePayload);
      if (!pp.ok) {
        onError(`Payload JSON: ${pp.error}`);
        return;
      }
      payload = pp.value;
    }

    setSingleBusy(true);
    try {
      await api.upsertPoints(
        info.name,
        [{ id: singleId.trim(), values, payload }],
        apiKey || undefined
      );
      onSuccess(`Upserted 1 point into ${info.name}`);
      setSingleId("");
      setSingleValues("");
      setSinglePayload("");
      onUpserted();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`upsert failed: ${msg}`);
    } finally {
      setSingleBusy(false);
    }
  };

  // ─── Bulk JSON handlers ────────────────────────────────────────────────

  const submitBulk = async () => {
    if (!bulkText.trim()) {
      onError("Paste at least one point as a JSON array");
      return;
    }
    const parsed = tryParseJson<unknown>(bulkText);
    if (!parsed.ok) {
      onError(`JSON parse: ${parsed.error}`);
      return;
    }
    if (!Array.isArray(parsed.value)) {
      onError("Bulk JSON must be an array of {id, values, payload?} objects");
      return;
    }
    const points = parsed.value as Array<Record<string, unknown>>;
    let embedded = 0;
    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      if (typeof p?.id !== "string" || !p.id) {
        onError(`Row ${i + 1}: missing string "id"`);
        return;
      }
      // Auto-embed: if this row has no `values` (or wrong-length values) and
      // the collection has a BM25 field, derive the vector from payload[field].
      if (bulkAutoEmbed && info.bm25TextField && dim > 0) {
        const needsEmbed =
          !Array.isArray(p.values) ||
          (p.values as unknown[]).length === 0 ||
          (p.values as number[]).length !== dim;
        if (needsEmbed) {
          const payload = (p.payload as Record<string, unknown> | undefined) ?? {};
          const text = payload[info.bm25TextField];
          if (typeof text === "string" && text.trim().length > 0) {
            (p as Record<string, unknown>).values = hashEmbed(text, dim);
            embedded++;
            continue;
          }
        }
      }
      if (
        !Array.isArray(p.values) ||
        (p.values as unknown[]).some((x) => typeof x !== "number")
      ) {
        onError(`Row ${i + 1} (id=${p.id}): "values" must be a number array`);
        return;
      }
      if (dim > 0 && (p.values as number[]).length !== dim) {
        onError(
          `Row ${i + 1} (id=${p.id}): values has ${(p.values as number[]).length} dims; expected ${dim}`
        );
        return;
      }
    }

    setBulkBusy(true);
    try {
      const t = performance.now();
      const result = await api.upsertPoints(
        info.name,
        points.map((p) => ({
          id: p.id as string,
          values: p.values as number[],
          payload: p.payload,
        })),
        apiKey || undefined
      );
      const ms = Math.round(performance.now() - t);
      const note = embedded > 0 ? ` (auto-embedded ${embedded} from payload.${info.bm25TextField})` : "";
      onSuccess(`Upserted ${result.upserted} points into ${info.name} in ${ms} ms${note}`);
      setBulkText("");
      onUpserted();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`bulk upsert failed: ${msg}`);
    } finally {
      setBulkBusy(false);
    }
  };

  const fillBulkExample = () => {
    if (dimMissing) {
      onError("Collection dimension unknown — refresh the collection first");
      return;
    }
    setBulkText(JSON.stringify(makeExamplePoints(dim, info.bm25TextField), null, 2));
  };

  // ─── Generate handlers ─────────────────────────────────────────────────

  const submitGenerate = async () => {
    if (dimMissing) {
      onError("Cannot generate: collection dimension unknown");
      return;
    }
    const total = Math.max(1, Math.min(genCount, 1_000_000));

    let payloadTemplate: unknown = undefined;
    if (genPayloadTpl.trim()) {
      // Lazy-validate the template by replacing {i} on the first row.
      const sample = genPayloadTpl.replace(/\{i\}/g, "0");
      const tp = tryParseJson<unknown>(sample);
      if (!tp.ok) {
        onError(`Payload template: ${tp.error}`);
        return;
      }
      payloadTemplate = tp.value;
    }

    setGenBusy(true);
    setGenProgress({ done: 0, total });

    const BATCH = 500;
    const t0 = performance.now();
    let inserted = 0;

    try {
      for (let start = 0; start < total; start += BATCH) {
        const end = Math.min(start + BATCH, total);
        const points = [];
        for (let i = start; i < end; i++) {
          const point: { id: string; values: number[]; payload?: unknown } = {
            id: `${genPrefix}-${i}`,
            values: randomUnitVector(dim),
          };
          if (payloadTemplate !== undefined) {
            // Re-template with the actual i for each row.
            const filled = genPayloadTpl.replace(/\{i\}/g, String(i));
            const r = tryParseJson<unknown>(filled);
            if (r.ok) point.payload = r.value;
          }
          points.push(point);
        }
        const r = await api.upsertPoints(info.name, points, apiKey || undefined);
        inserted += r.upserted;
        setGenProgress({ done: end, total });
      }
      const secs = (performance.now() - t0) / 1000;
      const rate = inserted / secs;
      onSuccess(
        `Generated ${inserted.toLocaleString()} random vectors in ${secs.toFixed(2)}s (${Math.round(
          rate
        )}/s)`
      );
      onUpserted();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body : (e as Error).message;
      onError(`generate failed after ${inserted}: ${msg}`);
    } finally {
      setGenBusy(false);
      setTimeout(() => setGenProgress(null), 1500);
    }
  };

  // ─── Render ────────────────────────────────────────────────────────────

  return (
    <>
      <section className="card">
        <h3>Add a single point</h3>
        <div className="form-grid">
          <div className="field-full">
            <label>ID</label>
            <input
              placeholder="doc-1"
              value={singleId}
              onChange={(e) => setSingleId(e.target.value)}
            />
          </div>

          <div
            className="field-full"
            style={{
              padding: 14,
              borderRadius: 8,
              border: "1px solid rgba(139, 92, 246, 0.45)",
              background: "rgba(139, 92, 246, 0.08)",
            }}
          >
            <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
              <label style={{ margin: 0, color: "var(--violet)" }}>Text → vector</label>
              <span className={`badge ${embedPick.method === "model" ? "accent" : "violet"}`}>
                Smart: {embedPick.label}
              </span>
            </div>
            <p className="hint" style={{ margin: "0 0 10px", fontSize: 11 }}>
              {embedPick.reason}
              {dim > 0 && dim !== NOMIC_V15_DIM && (
                <>
                  {" "}
                  For nomic semantic search, create a collection with dim{" "}
                  <strong>{NOMIC_V15_DIM}</strong>.
                </>
              )}
            </p>
            <div className="row" style={{ gap: 6 }}>
              <input
                placeholder="Good for health"
                value={embedText}
                onChange={(e) => setEmbedText(e.target.value)}
                style={{ flex: 1 }}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) runSmartEmbed();
                }}
              />
              <button
                type="button"
                className="primary"
                onClick={runSmartEmbed}
                disabled={smartBusy || singleBusy || dimMissing || !embedText.trim()}
              >
                {smartBusy ? "Embedding…" : "Smart embed → fill values"}
              </button>
            </div>
            <p className="hint" style={{ margin: "6px 0 0", fontSize: 11 }}>
              dim={dim}: uses <strong>{embedPick.method === "model" ? "LM Studio (nomic)" : "hash"}</strong>
              {embedPick.method === "model" && " — if the server is down, falls back to hash"}
              . ⌘/Ctrl+Enter to run.
            </p>
          </div>

          <div className="field-full">
            <details style={{ marginBottom: 4 }}>
              <summary style={{ cursor: "pointer", color: "var(--text-muted)", fontSize: 13 }}>
                Advanced: manual model or hash embed
              </summary>
              <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 12 }}>
                <EmbeddingModelCard
                  collectionDim={dim}
                  bm25TextField={info.bm25TextField}
                  text={embedText}
                  onTextChange={setEmbedText}
                  onEmbedded={onModelEmbedded}
                  onError={onError}
                  onSuccess={onSuccess}
                  hideTextInput
                />
                <div>
                  <label>Hash embed only</label>
                  <div className="row" style={{ gap: 6 }}>
                    <button
                      type="button"
                      onClick={fillFromHashEmbed}
                      disabled={dimMissing || !embedText.trim()}
                    >
                      Hash embed → fill values
                    </button>
                  </div>
                </div>
              </div>
            </details>
          </div>

          <div className="field-full">
            <label>
              Values <span className="hint">— JSON array of {dimHint}</span>
            </label>
            <div className="row" style={{ gap: 6, marginBottom: 6 }}>
              <button type="button" onClick={fillRandom} disabled={dimMissing}>
                Random unit vector
              </button>
              <button type="button" onClick={fillZero} disabled={dimMissing}>
                Zero vector
              </button>
              <button type="button" onClick={() => setSingleValues("")} disabled={!singleValues}>
                Clear
              </button>
            </div>
            <textarea
              placeholder={`[${"0, ".repeat(Math.min(dim || 4, 4))}…]`}
              value={singleValues}
              onChange={(e) => setSingleValues(e.target.value)}
              rows={3}
            />
          </div>
          <div className="field-full">
            <label>
              Payload <span className="hint">— optional JSON object</span>
            </label>
            <textarea
              placeholder={info.bm25TextField
                ? `{"${info.bm25TextField}": "your text…", "category": "demo"}`
                : '{"category": "demo"}'}
              value={singlePayload}
              onChange={(e) => setSinglePayload(e.target.value)}
              rows={3}
            />
          </div>
        </div>
        <div className="row">
          <button className="primary" onClick={submitSingle} disabled={singleBusy || dimMissing}>
            {singleBusy ? "Upserting…" : "Upsert point"}
          </button>
          {dimMissing && (
            <span className="hint" style={{ color: "var(--amber)" }}>
              Cannot infer dimension from spec; reload the collection.
            </span>
          )}
        </div>
      </section>

      <section className="card">
        <h3>Bulk upsert (paste JSON)</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Each object needs an <code>id</code> and a <code>values</code> array — that array{" "}
          <strong>is the vector</strong>. It must have exactly{" "}
          <strong>{dim || "?"} numbers</strong> (one per dimension). Optional{" "}
          <code>payload</code> is metadata (e.g. text for BM25 search).
        </p>
        <div className="row" style={{ gap: 6, marginBottom: 6, flexWrap: "wrap" }}>
          <button type="button" onClick={fillBulkExample}>
            Insert example
          </button>
          <button type="button" onClick={() => setBulkText("")} disabled={!bulkText}>
            Clear
          </button>
          {info.bm25TextField && (
            <label
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                margin: 0,
                padding: "0 10px",
                fontSize: 12,
                textTransform: "none",
                letterSpacing: 0,
                color: bulkAutoEmbed ? "var(--accent)" : "var(--text-muted)",
                cursor: "pointer",
              }}
              title={`When checked, rows missing 'values' get them from hashEmbed(payload.${info.bm25TextField})`}
            >
              <input
                type="checkbox"
                checked={bulkAutoEmbed}
                onChange={(e) => setBulkAutoEmbed(e.target.checked)}
                style={{ width: "auto" }}
              />
              auto-embed from payload.{info.bm25TextField}
            </label>
          )}
          <span className="grow" style={{ flex: 1 }} />
          <span className="hint">Collection dim: {dimHint}</span>
        </div>
        <textarea
          value={bulkText}
          onChange={(e) => setBulkText(e.target.value)}
          placeholder={
            dim
              ? `[\n  {\n    "id": "doc-1",\n    "values": [/* ${dim} floats */],\n    "payload": { "text": "…" }\n  }\n]`
              : '[{"id":"doc-1","values":[…],"payload":{}}]'
          }
          rows={12}
          spellCheck={false}
        />
        <div className="row" style={{ marginTop: 10 }}>
          <button className="primary" onClick={submitBulk} disabled={bulkBusy || !bulkText.trim()}>
            {bulkBusy ? "Upserting…" : "Bulk upsert"}
          </button>
        </div>
      </section>

      <section className="card">
        <h3>Generate random vectors</h3>
        <p className="hint" style={{ marginTop: 0 }}>
          Quickly populate the collection with synthetic data. Useful for benchmarking,
          smoke tests, and exploring the search/admin features.
        </p>
        <div className="form-grid">
          <div>
            <label>Count</label>
            <input
              type="number"
              min={1}
              max={1_000_000}
              value={genCount}
              onChange={(e) => setGenCount(Math.max(1, Number(e.target.value) || 1))}
            />
          </div>
          <div>
            <label>ID prefix</label>
            <input
              value={genPrefix}
              onChange={(e) => setGenPrefix(e.target.value)}
              placeholder="gen-..."
            />
          </div>
          <div className="field-full">
            <label>
              Payload template <span className="hint">— JSON; <code>{"{i}"}</code> is the row index</span>
            </label>
            <textarea
              value={genPayloadTpl}
              onChange={(e) => setGenPayloadTpl(e.target.value)}
              placeholder={'{"text": "synthetic point #{i}", "ord": "{i}"}'}
              rows={2}
            />
          </div>
        </div>
        <div className="row">
          <button className="primary" onClick={submitGenerate} disabled={genBusy || dimMissing}>
            {genBusy
              ? `Generating ${genProgress?.done ?? 0}/${genProgress?.total ?? genCount}…`
              : `Generate ${genCount.toLocaleString()} vectors`}
          </button>
          {genProgress && genProgress.total > 0 && (
            <div
              style={{
                flex: 1,
                height: 6,
                marginLeft: 12,
                background: "var(--surface-2)",
                borderRadius: 3,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  height: "100%",
                  width: `${(genProgress.done / genProgress.total) * 100}%`,
                  background: "linear-gradient(90deg, var(--accent), var(--violet))",
                  transition: "width .12s",
                }}
              />
            </div>
          )}
        </div>
      </section>
    </>
  );
}
