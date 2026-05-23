export interface ServerConfig {
  upstream: string;
  hasApiKey: boolean;
  serverTime: string;
}

export interface CollectionDescription {
  // The gateway currently returns spec as a debug-formatted string.
  spec: string;
  vector_count: number;
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
  rawSpec: string;
}

export interface SearchHit {
  id: string;
  score: number;
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
