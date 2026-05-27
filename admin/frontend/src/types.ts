export interface ServerConfig {
  upstream: string;
  hasApiKey: boolean;
  serverTime: string;
  embedDefaults?: {
    baseUrl: string;
    model: string;
    provider?: "openai" | "ollama";
  };
}

export interface CollectionDescription {
  // The gateway currently returns spec as a debug-formatted string.
  spec: string;
  vector_count: number;
}

/**
 * Structured stats payload returned by `GET /v1/collections/:name/stats`.
 * This is the preferred source of truth for collection metadata — the
 * `parseCollectionInfo()` regex fallback is only used when /stats is missing
 * fields (e.g. HNSW parameters, which still live in the debug spec for now).
 */
export interface CollectionStats {
  name: string;
  vector_count: number;
  dimension: number;
  metric: "cosine" | "euclidean" | "dot" | string;
  sparse_enabled: boolean;
  bm25_text_field: string;
  payload_index_count: number;
  scalar_quantization: boolean;
}

export interface CollectionInfo {
  name: string;
  dimension: number | null;
  metric: string | null;
  bm25TextField: string | null;
  sparseEnabled: boolean;
  scalarQuantization: boolean;
  m: number | null;
  efConstruction: number | null;
  efSearch: number | null;
  vectorCount: number;
  payloadIndexCount: number | null;
  rawSpec: string;
}

export interface SearchHit {
  id: string;
  score: number;
  payload?: unknown;
  vector?: number[];
}

export interface PointDetail {
  id: string;
  values: number[];
  payload: unknown;
}

export type SearchMode =
  | "dense"
  | "bm25"
  | "hybrid_rrf"
  | "hybrid_weighted"
  | "sparse";

/** Result of `POST /v1/auth/login`. The `token` is `tokenid:secret` and can
 *  be sent as either `x-api-key` or `Authorization: Bearer <token>`. */
export interface LoginResponse {
  token_id: string;
  token: string;
  user: string;
}

export interface ScrollPoint {
  id: string;
  values?: number[] | null;
  payload?: unknown;
}

export interface ScrollResponse {
  points: ScrollPoint[];
  next_cursor: string;
}

export interface AnalyzerToken {
  token: string;
  start_offset: number;
  end_offset: number;
  position: number;
  hash: number;
}

export interface AnalyzerResult {
  results: { tokens: AnalyzerToken[] }[];
}
