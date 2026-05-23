import { VectorDbClient, VectorPoint } from "./client.js";

export type EmbedFn = (text: string) => Promise<number[]> | number[];

export type RagDocument = {
  id: string;
  text: string;
  payload?: Record<string, unknown>;
};

export type RagHit = {
  id: string;
  score: number;
  text: string;
  payload: Record<string, unknown>;
  chunkIndex: number;
  documentId: string;
};

export function chunkText(
  text: string,
  opts: { chunkSize?: number; overlap?: number } = {},
): string[] {
  const chunkSize = opts.chunkSize ?? 512;
  const overlap = opts.overlap ?? 64;
  const trimmed = text.trim();
  if (!trimmed) return [];
  if (trimmed.length <= chunkSize) return [trimmed];

  const chunks: string[] = [];
  let start = 0;
  while (start < trimmed.length) {
    let end = Math.min(start + chunkSize, trimmed.length);
    if (end < trimmed.length) {
      const split = trimmed.lastIndexOf(" ", start, end);
      if (split > start) end = split;
    }
    const piece = trimmed.slice(start, end).trim();
    if (piece) chunks.push(piece);
    if (end >= trimmed.length) break;
    start = Math.max(end - overlap, start + 1);
  }
  return chunks;
}

export function expandQuery(query: string, maxVariants = 3): string[] {
  const q = query.trim();
  if (!q) return [];
  const variants = [q];
  const lower = q.toLowerCase();
  if (lower !== q) variants.push(lower);
  const words = q.match(/\w+/g)?.filter((w) => w.length > 2) ?? [];
  if (words.length >= 2) variants.push(words.join(" "));
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of variants) {
    if (!seen.has(v)) {
      seen.add(v);
      out.push(v);
    }
    if (out.length >= maxVariants) break;
  }
  return out;
}

export function rerankByOverlap(query: string, hits: RagHit[], topK?: number): RagHit[] {
  const qTokens = new Set(query.toLowerCase().match(/\w+/g) ?? []);
  if (!qTokens.size) return topK ? hits.slice(0, topK) : hits;

  const scored = hits.map((hit) => {
    const tTokens = new Set(hit.text.toLowerCase().match(/\w+/g) ?? []);
    let overlap = 0;
    for (const t of qTokens) if (tTokens.has(t)) overlap++;
    const boost = overlap / Math.max(qTokens.size, 1);
    return { hit, score: hit.score + boost };
  });
  scored.sort((a, b) => b.score - a.score);
  const ranked = scored.map((s) => ({ ...s.hit, score: s.score }));
  return topK ? ranked.slice(0, topK) : ranked;
}

export type RagPipelineOptions = {
  textField?: string;
  chunkSize?: number;
  overlap?: number;
  metric?: string;
  bm25TextField?: string;
  sparseEnabled?: boolean;
};

export class RagPipeline {
  private collectionReady = false;

  constructor(
    private readonly client: VectorDbClient,
    private readonly collection: string,
    private readonly dimension: number,
    private readonly embed: EmbedFn,
    private readonly opts: RagPipelineOptions = {},
  ) {}

  private get textField(): string {
    return this.opts.textField ?? "text";
  }

  async ensureCollection(): Promise<void> {
    if (this.collectionReady) return;
    const names = await this.client.listCollections();
    if (!names.includes(this.collection)) {
      await this.client.createCollection(this.collection, this.dimension, {
        metric: this.opts.metric,
        bm25TextField: this.opts.bm25TextField ?? this.textField,
        sparseEnabled: this.opts.sparseEnabled,
      });
    }
    this.collectionReady = true;
  }

  async ingest(documents: RagDocument[], bulk = true): Promise<number> {
    await this.ensureCollection();
    const points: VectorPoint[] = [];

    for (const doc of documents) {
      const chunks = chunkText(doc.text, {
        chunkSize: this.opts.chunkSize,
        overlap: this.opts.overlap,
      });
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i]!;
        const vector = await this.embed(chunk);
        if (vector.length !== this.dimension) {
          throw new Error(`embedding dimension ${vector.length} != ${this.dimension}`);
        }
        points.push({
          id: `${doc.id}#${i}`,
          values: vector,
          payload: {
            ...(doc.payload ?? {}),
            [this.textField]: chunk,
            document_id: doc.id,
            chunk_index: i,
          },
        });
      }
    }

    if (!points.length) return 0;
    const resp = bulk
      ? await this.client.bulkUpsert(this.collection, points)
      : await this.client.upsert(this.collection, points);
    return resp.upserted;
  }

  async query(
    queryText: string,
    opts: {
      topK?: number;
      filter?: Record<string, unknown>;
      searchMode?: string;
      hybridAlpha?: number;
      multiQuery?: boolean;
      rerank?: boolean;
    } = {},
  ): Promise<RagHit[]> {
    await this.ensureCollection();
    const topK = opts.topK ?? 5;
    const queries = opts.multiQuery ? expandQuery(queryText) : [queryText];
    const byId = new Map<string, RagHit>();

    for (const q of queries) {
      const vector = await this.embed(q);
      const raw = await this.client.search(this.collection, vector, {
        topK,
        filter: opts.filter,
        textQuery: opts.searchMode !== "dense" ? q : undefined,
        searchMode: opts.searchMode,
        hybridAlpha: opts.hybridAlpha,
      });

      for (let rank = 0; rank < raw.length; rank++) {
        const row = raw[rank]!;
        const point = await this.client.getPoint(this.collection, row.id);
        const payload = (point?.payload as Record<string, unknown>) ?? {};
        const text = String(payload[this.textField] ?? "");
        const documentId = String(payload.document_id ?? row.id.split("#")[0]);
        const chunkIndex = Number(payload.chunk_index ?? 0);
        const rrf = 1 / (60 + rank + 1);
        const existing = byId.get(row.id);
        if (existing) {
          existing.score += rrf;
        } else {
          byId.set(row.id, {
            id: row.id,
            score: rrf,
            text,
            payload,
            chunkIndex,
            documentId,
          });
        }
      }
    }

    let hits = [...byId.values()].sort((a, b) => b.score - a.score).slice(0, topK);
    if (opts.rerank) hits = rerankByOverlap(queryText, hits, topK);
    return hits;
  }
}
