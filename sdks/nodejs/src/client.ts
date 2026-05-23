/** HTTP/JSON client for the VectorDB REST gateway. */

export class VectorDbError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
  ) {
    super(message);
    this.name = "VectorDbError";
  }
}

export type PayloadIndex = { field: string; kind: "keyword" | "numeric" | "bool" };

export type VectorPoint = {
  id: string;
  values: number[];
  payload?: Record<string, unknown>;
  sparse?: { indices: number[]; values: number[] };
};

export type SearchHit = { id: string; score: number };

export type VectorDbClientOptions = {
  apiKey?: string;
  timeoutMs?: number;
};

export class VectorDbClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly timeoutMs: number;

  constructor(baseUrl = "http://127.0.0.1:8080", options: VectorDbClientOptions = {}) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs ?? 60_000;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) {
      h["x-api-key"] = this.apiKey;
      h["Authorization"] = `Bearer ${this.apiKey}`;
    }
    return h;
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    try {
      const res = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: this.headers(),
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: ctrl.signal,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new VectorDbError(res.status, text);
      }
      if (res.status === 204) return undefined as T;
      const text = await res.text();
      return text ? (JSON.parse(text) as T) : (undefined as T);
    } finally {
      clearTimeout(timer);
    }
  }

  health(): Promise<{ status: string }> {
    return this.request("GET", "/health");
  }

  async live(): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/live`);
    return res.ok;
  }

  async ready(): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/ready`);
    return res.ok;
  }

  listCollections(): Promise<string[]> {
    return this.request("GET", "/v1/collections");
  }

  createCollection(
    name: string,
    dimension: number,
    opts: {
      metric?: string;
      payloadIndexes?: PayloadIndex[];
      sparseEnabled?: boolean;
      bm25TextField?: string;
      scalarQuantization?: boolean;
    } = {},
  ): Promise<void> {
    return this.request("POST", "/v1/collections", {
      name,
      dimension,
      metric: opts.metric ?? "cosine",
      payload_indexes: opts.payloadIndexes,
      sparse_enabled: opts.sparseEnabled ?? false,
      bm25_text_field: opts.bm25TextField,
      scalar_quantization: opts.scalarQuantization ?? false,
    });
  }

  describeCollection(name: string): Promise<Record<string, unknown>> {
    return this.request("GET", `/v1/collections/${encodeURIComponent(name)}`);
  }

  deleteCollection(name: string): Promise<void> {
    return this.request("DELETE", `/v1/collections/${encodeURIComponent(name)}`);
  }

  upsert(collection: string, points: VectorPoint[]): Promise<{ upserted: number }> {
    return this.request("POST", `/v1/collections/${encodeURIComponent(collection)}/upsert`, {
      points,
    });
  }

  bulkUpsert(
    collection: string,
    points: VectorPoint[],
    chunkSize = 500,
  ): Promise<{ upserted: number }> {
    return this.request("POST", `/v1/collections/${encodeURIComponent(collection)}/bulk`, {
      points,
      chunk_size: chunkSize,
    });
  }

  search(
    collection: string,
    vector: number[],
    opts: {
      topK?: number;
      filter?: Record<string, unknown>;
      sparseQuery?: { indices: number[]; values: number[] };
      textQuery?: string;
      searchMode?: string;
      hybridAlpha?: number;
    } = {},
  ): Promise<SearchHit[]> {
    return this.request("POST", `/v1/collections/${encodeURIComponent(collection)}/search`, {
      vector,
      top_k: opts.topK ?? 10,
      filter: opts.filter,
      sparse_query: opts.sparseQuery,
      text_query: opts.textQuery,
      search_mode: opts.searchMode ?? "dense",
      hybrid_alpha: opts.hybridAlpha ?? 0.5,
    });
  }

  deletePoints(collection: string, ids: string[]): Promise<{ deleted: number }> {
    return this.request("DELETE", `/v1/collections/${encodeURIComponent(collection)}/points`, {
      ids,
    });
  }

  async getPoint(
    collection: string,
    id: string,
  ): Promise<{ id: string; values: number[]; payload: Record<string, unknown> } | null> {
    try {
      return await this.request(
        "GET",
        `/v1/collections/${encodeURIComponent(collection)}/points/${encodeURIComponent(id)}`,
      );
    } catch (e) {
      if (e instanceof VectorDbError && e.statusCode === 404) return null;
      throw e;
    }
  }

  compactWal(snapshotFirst = false): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/admin/compact-wal", { snapshot_first: snapshotFirst });
  }

  reindexCollection(collection: string): Promise<{ vectors_reindexed: number }> {
    return this.request("POST", `/v1/collections/${encodeURIComponent(collection)}/reindex`);
  }

  createSnapshot(): Promise<Record<string, unknown>> {
    return this.request("POST", "/v1/snapshots");
  }

  listSnapshots(): Promise<Record<string, unknown>[]> {
    return this.request("GET", "/v1/snapshots");
  }

  deleteSnapshot(id: string): Promise<void> {
    return this.request("DELETE", `/v1/snapshots/${encodeURIComponent(id)}`);
  }
}
