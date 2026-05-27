import type {
  AnalyzerResult,
  CollectionDescription,
  CollectionInfo,
  CollectionStats,
  LoginResponse,
  PointDetail,
  ScrollResponse,
  SearchHit,
  ServerConfig,
} from "./types";

const BASE = "/api";

/**
 * Authentication header bundle. The gateway accepts either an `x-api-key`
 * (legacy superuser key or `tokenid:secret`) or `Authorization: Bearer …`.
 * We send both when available so a single value works on every endpoint.
 */
export interface AuthHeaders {
  /** API key from the local dev launcher (`run-data/single/.api-key`) or a
   *  long-lived legacy key. */
  apiKey?: string;
  /** Bearer token minted by `POST /v1/auth/login` (or `/v1/auth/tokens`). */
  bearer?: string;
}

function authToHeaders(auth?: AuthHeaders | string): Record<string, string> {
  if (!auth) return {};
  // Back-compat: callers used to pass `apiKey` as a bare string.
  if (typeof auth === "string") {
    const k = auth.trim();
    if (!k) return {};
    return { "x-api-key": k, Authorization: `Bearer ${k}` };
  }
  const h: Record<string, string> = {};
  const key = auth.apiKey?.trim();
  const bearer = auth.bearer?.trim();
  if (key) h["x-api-key"] = key;
  // Bearer wins over the legacy key when both are present.
  if (bearer) h["Authorization"] = `Bearer ${bearer}`;
  else if (key) h["Authorization"] = `Bearer ${key}`;
  return h;
}

async function http<T>(
  method: string,
  path: string,
  body?: unknown,
  auth?: AuthHeaders | string
): Promise<T> {
  const headers: Record<string, string> = { ...authToHeaders(auth) };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new ApiError(res.status, text);
  }
  if (res.status === 204) return undefined as T;
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) return res.json();
  return (await res.text()) as unknown as T;
}

export class ApiError extends Error {
  constructor(public status: number, public body: string) {
    super(`${status}: ${body}`);
  }
}

export interface CreateCollectionRequest {
  name: string;
  dimension: number;
  metric?: "cosine" | "euclidean" | "dot";
  bm25_text_field?: string;
  sparse_enabled?: boolean;
  scalar_quantization?: boolean;
  payload_indexes?: { field: string; kind: "keyword" | "numeric" | "bool" }[];
}

export interface SearchRequest {
  vector: number[];
  top_k: number;
  search_mode?: string;
  text_query?: string;
  hybrid_alpha?: number;
  filter?: unknown;
  sparse_query?: { indices: number[]; values: number[] };
  with_payload?: boolean;
  with_vector?: boolean;
  output_fields?: string[];
}

/** One leg of a multi-vector hybrid search. */
export interface AnnRequest {
  field?: string;
  limit?: number;
  dense?: number[];
  sparse?: { indices: number[]; values: number[] };
  text?: string;
  filter?: unknown;
}

export type Reranker =
  | { kind: "rrf" }
  | { kind: "weighted"; weights: number[] };

export interface HybridSearchBody {
  requests: AnnRequest[];
  limit: number;
  reranker?: Reranker;
  output_fields?: string[];
  partitions?: string[];
  with_payload?: boolean;
  with_vector?: boolean;
}

export interface ScrollRequest {
  cursor?: string;
  limit?: number;
  filter?: unknown;
  partition?: string;
  output_fields?: string[];
  with_payload?: boolean;
  with_vector?: boolean;
}

export interface QueryRequest {
  filter?: unknown;
  ids?: string[];
  limit?: number;
  offset?: number;
  output_fields?: string[];
  with_payload?: boolean;
  with_vector?: boolean;
}

export interface DeletePointsBody {
  ids?: string[];
  /** Milvus-style boolean expression, e.g. `category == 'demo'`. */
  filter?: string;
  partition?: string;
}

export interface SnapshotInfo {
  id: string;
  created_at_ms: number;
  path: string;
  size_bytes?: number;
}

export interface PdfChunk {
  idx: number;
  text: string;
  page: number;
  char_start: number;
  char_end: number;
}

export interface PdfExtractResult {
  filename: string;
  pages: number;
  chars: number;
  chunks: PdfChunk[];
}

export const api = {
  config: () => fetch("/config.json").then((r) => r.json() as Promise<ServerConfig>),

  // ---- Auth / RBAC ------------------------------------------------------
  login: (username: string, password: string, auth?: AuthHeaders) =>
    http<LoginResponse>("POST", "/v1/auth/login", { username, password }, auth),
  revokeToken: (tokenId: string, auth?: AuthHeaders) =>
    http<void>("DELETE", `/v1/auth/tokens/${encodeURIComponent(tokenId)}`, undefined, auth),

  // ---- Cluster / lifecycle ---------------------------------------------
  health: (auth?: AuthHeaders) => http<{ status: string }>("GET", "/health", undefined, auth),
  version: (auth?: AuthHeaders) =>
    http<{ version: string; server: string }>("GET", "/v1/version", undefined, auth),

  // ---- Collections ------------------------------------------------------
  listCollections: (auth?: AuthHeaders) =>
    http<string[]>("GET", "/v1/collections", undefined, auth),
  describeCollection: (name: string, auth?: AuthHeaders) =>
    http<CollectionDescription>(
      "GET",
      `/v1/collections/${encodeURIComponent(name)}`,
      undefined,
      auth
    ),
  /**
   * Structured stats. Strongly preferred over `describeCollection` for the
   * UI because it doesn't depend on debug-formatted spec parsing.
   */
  collectionStats: (name: string, auth?: AuthHeaders) =>
    http<CollectionStats>(
      "GET",
      `/v1/collections/${encodeURIComponent(name)}/stats`,
      undefined,
      auth
    ),
  createCollection: (body: CreateCollectionRequest, auth?: AuthHeaders) =>
    http<void>("POST", "/v1/collections", body, auth),
  deleteCollection: (name: string, auth?: AuthHeaders) =>
    http<void>("DELETE", `/v1/collections/${encodeURIComponent(name)}`, undefined, auth),

  // ---- Search ----------------------------------------------------------
  search: (name: string, body: SearchRequest, auth?: AuthHeaders) =>
    http<SearchHit[]>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/search`,
      body,
      auth
    ),
  hybridSearch: (name: string, body: HybridSearchBody, auth?: AuthHeaders) =>
    http<SearchHit[]>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/hybrid-search`,
      body,
      auth
    ),
  /** `POST /v1/collections/:name/query` — Milvus-parity filter+IDs lookup. */
  query: (name: string, body: QueryRequest, auth?: AuthHeaders) =>
    http<{ points: { id: string; values?: number[] | null; payload?: unknown }[] }>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/query`,
      body,
      auth
    ),
  /** `POST /v1/collections/:name/scroll` — cursor-paginated iteration. */
  scroll: (name: string, body: ScrollRequest, auth?: AuthHeaders) =>
    http<ScrollResponse>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/scroll`,
      body,
      auth
    ),

  // ---- Points ----------------------------------------------------------
  getPoint: (name: string, id: string, auth?: AuthHeaders) =>
    http<PointDetail>(
      "GET",
      `/v1/collections/${encodeURIComponent(name)}/points/${encodeURIComponent(id)}`,
      undefined,
      auth
    ),
  upsertPoints: (
    name: string,
    points: { id: string; values: number[]; payload?: unknown }[],
    auth?: AuthHeaders
  ) =>
    http<{ upserted: number }>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/upsert`,
      { points },
      auth
    ),
  /**
   * Delete points by IDs, by Milvus-style filter expression, or both.
   * The gateway accepts an empty `ids` array when `filter` is set.
   */
  deletePoints: (name: string, body: DeletePointsBody, auth?: AuthHeaders) =>
    http<{ deleted: number }>(
      "DELETE",
      `/v1/collections/${encodeURIComponent(name)}/points`,
      body,
      auth
    ),

  // ---- Admin -----------------------------------------------------------
  reindex: (name: string, auth?: AuthHeaders) =>
    http<{ vectors_reindexed: number }>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/reindex`,
      {},
      auth
    ),
  compactWal: (snapshotFirst: boolean, auth?: AuthHeaders) =>
    http<unknown>("POST", "/v1/admin/compact-wal", { snapshot_first: snapshotFirst }, auth),
  listSnapshots: (auth?: AuthHeaders) =>
    http<SnapshotInfo[]>("GET", "/v1/snapshots", undefined, auth),
  createSnapshot: (auth?: AuthHeaders) =>
    http<SnapshotInfo>("POST", "/v1/snapshots", {}, auth),
  deleteSnapshot: (id: string, auth?: AuthHeaders) =>
    http<void>("DELETE", `/v1/snapshots/${encodeURIComponent(id)}`, undefined, auth),

  /** `POST /v1/admin/analyze` — Milvus-parity RunAnalyzer (BM25 tokenizer). */
  runAnalyzer: (
    body: {
      text: string[];
      collection?: string;
      field?: string;
      analyzer_params?: unknown;
      analyzer_name?: string[];
      with_detail?: boolean;
      with_hash?: boolean;
    },
    auth?: AuthHeaders
  ) => http<AnalyzerResult>("POST", "/v1/admin/analyze", body, auth),

  /**
   * Upload a PDF to the admin backend (`/pdf/extract`) which parses it server-
   * side, splits the text into chunks, and returns them. The frontend then
   * embeds + upserts each chunk — keeping all DB-side code unchanged.
   */
  extractPdf: async (
    file: File,
    chunkSize: number,
    chunkOverlap: number
  ): Promise<PdfExtractResult> => {
    const fd = new FormData();
    fd.append("file", file);
    fd.append("chunk_size", String(chunkSize));
    fd.append("chunk_overlap", String(chunkOverlap));
    const res = await fetch("/pdf/extract", { method: "POST", body: fd });
    const body = await res.text();
    if (!res.ok) {
      let msg = body;
      try {
        const j = JSON.parse(body) as { error?: string };
        if (j.error) msg = j.error;
      } catch {
        /* not JSON */
      }
      throw new Error(msg || `extract failed: HTTP ${res.status}`);
    }
    return JSON.parse(body) as PdfExtractResult;
  },
};

/**
 * Build a CollectionInfo by merging the structured /stats response with
 * regex-parsed fields from the debug spec (HNSW params live in spec only).
 * Either argument may be null — if /stats failed we fall back to spec only;
 * if /describe failed we still surface what /stats gave us.
 */
export function buildCollectionInfo(
  name: string,
  stats: CollectionStats | null,
  desc: CollectionDescription | null
): CollectionInfo {
  const spec = desc?.spec || "";
  const m = spec.match.bind(spec);
  const num = (re: RegExp): number | null => {
    const x = m(re)?.[1];
    return x !== undefined ? Number(x) : null;
  };
  const str = (re: RegExp): string | null => {
    const x = m(re)?.[1];
    return x && x.length > 0 ? x : null;
  };
  return {
    name,
    dimension: stats?.dimension ?? num(/dimension:\s*(\d+)/),
    metric: stats?.metric ?? str(/metric:\s*(\w+)/),
    bm25TextField:
      (stats?.bm25_text_field ? stats.bm25_text_field : null) ??
      str(/bm25_text_field:\s*"([^"]*)"/),
    sparseEnabled: stats?.sparse_enabled ?? /sparse_enabled:\s*true/.test(spec),
    scalarQuantization:
      stats?.scalar_quantization ?? /scalar_quantization:\s*true/.test(spec),
    m: num(/[\s\{,]\s*m:\s*(\d+)/),
    efConstruction: num(/ef_construction:\s*(\d+)/),
    efSearch: num(/ef_search:\s*(\d+)/),
    vectorCount: stats?.vector_count ?? desc?.vector_count ?? 0,
    payloadIndexCount: stats?.payload_index_count ?? null,
    rawSpec: spec,
  };
}

/**
 * @deprecated Use `buildCollectionInfo(name, await api.collectionStats(name), desc)`.
 * Kept for any external code that still relies on the regex-only parse.
 */
export function parseCollectionInfo(
  name: string,
  desc: CollectionDescription
): CollectionInfo {
  return buildCollectionInfo(name, null, desc);
}
