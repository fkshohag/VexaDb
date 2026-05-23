import type {
  CollectionDescription,
  CollectionInfo,
  PointDetail,
  SearchHit,
  ServerConfig,
} from "./types";

const BASE = "/api";

async function http<T>(
  method: string,
  path: string,
  body?: unknown,
  apiKey?: string
): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (apiKey) {
    headers["x-api-key"] = apiKey;
    headers["Authorization"] = `Bearer ${apiKey}`;
  }
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
  // Plain text (e.g. /metrics)
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
}

export interface SnapshotInfo {
  id: string;
  created_at_ms: number;
  path: string;
  size_bytes?: number;
}

export const api = {
  config: () => fetch("/config.json").then((r) => r.json() as Promise<ServerConfig>),

  health: (apiKey?: string) => http<{ status: string }>("GET", "/health", undefined, apiKey),
  listCollections: (apiKey?: string) =>
    http<string[]>("GET", "/v1/collections", undefined, apiKey),
  describeCollection: (name: string, apiKey?: string) =>
    http<CollectionDescription>(
      "GET",
      `/v1/collections/${encodeURIComponent(name)}`,
      undefined,
      apiKey
    ),
  createCollection: (body: CreateCollectionRequest, apiKey?: string) =>
    http<void>("POST", "/v1/collections", body, apiKey),
  deleteCollection: (name: string, apiKey?: string) =>
    http<void>("DELETE", `/v1/collections/${encodeURIComponent(name)}`, undefined, apiKey),
  search: (name: string, body: SearchRequest, apiKey?: string) =>
    http<SearchHit[]>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/search`,
      body,
      apiKey
    ),
  getPoint: (name: string, id: string, apiKey?: string) =>
    http<PointDetail>(
      "GET",
      `/v1/collections/${encodeURIComponent(name)}/points/${encodeURIComponent(id)}`,
      undefined,
      apiKey
    ),
  upsertPoints: (name: string, points: { id: string; values: number[]; payload?: unknown }[], apiKey?: string) =>
    http<{ upserted: number }>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/upsert`,
      { points },
      apiKey
    ),
  deletePoints: (name: string, ids: string[], apiKey?: string) =>
    http<{ deleted: number }>(
      "DELETE",
      `/v1/collections/${encodeURIComponent(name)}/points`,
      { ids },
      apiKey
    ),
  reindex: (name: string, apiKey?: string) =>
    http<{ vectors_reindexed: number }>(
      "POST",
      `/v1/collections/${encodeURIComponent(name)}/reindex`,
      {},
      apiKey
    ),
  compactWal: (snapshotFirst: boolean, apiKey?: string) =>
    http<unknown>("POST", "/v1/admin/compact-wal", { snapshot_first: snapshotFirst }, apiKey),
  listSnapshots: (apiKey?: string) =>
    http<SnapshotInfo[]>("GET", "/v1/snapshots", undefined, apiKey),
  createSnapshot: (apiKey?: string) =>
    http<SnapshotInfo>("POST", "/v1/snapshots", {}, apiKey),
  deleteSnapshot: (id: string, apiKey?: string) =>
    http<void>("DELETE", `/v1/snapshots/${encodeURIComponent(id)}`, undefined, apiKey),
};

/**
 * The gateway returns the collection spec as a debug-formatted string. Parse
 * the bits we actually care about so the UI can show structured fields.
 */
export function parseCollectionInfo(
  name: string,
  desc: CollectionDescription
): CollectionInfo {
  const spec = desc.spec || "";
  const m = spec.match.bind(spec);
  const num = (re: RegExp): number | null => {
    const x = m(re)?.[1];
    return x ? Number(x) : null;
  };
  const str = (re: RegExp): string | null => {
    const x = m(re)?.[1];
    return x && x.length > 0 ? x : null;
  };
  return {
    name,
    dimension: num(/dimension:\s*(\d+)/),
    metric: str(/metric:\s*(\w+)/),
    bm25TextField: str(/bm25_text_field:\s*"([^"]*)"/),
    sparseEnabled: /sparse_enabled:\s*true/.test(spec),
    scalarQuantization: /scalar_quantization:\s*true/.test(spec),
    m: num(/[\s\{,]\s*m:\s*(\d+)/),
    efConstruction: num(/ef_construction:\s*(\d+)/),
    efSearch: num(/ef_search:\s*(\d+)/),
    vectorCount: desc.vector_count ?? 0,
    rawSpec: spec,
  };
}
