import type { CollectionInfo } from "../types";

interface Props {
  info: CollectionInfo;
}

export function CollectionDetail({ info }: Props) {
  return (
    <section className="card">
      <h3>Overview</h3>
      <div className="kv-grid">
        <div className="k">Name</div>
        <div className="v">{info.name}</div>

        <div className="k">Vector count</div>
        <div className="v">{info.vectorCount.toLocaleString()}</div>

        <div className="k">Dimension</div>
        <div className="v">{info.dimension ?? <span className="hint">unknown</span>}</div>

        <div className="k">Distance metric</div>
        <div className="v">
          {info.metric ? (
            <span className="badge accent">{info.metric.toLowerCase()}</span>
          ) : (
            <span className="hint">—</span>
          )}
        </div>

        <div className="k">BM25 text field</div>
        <div className="v">
          {info.bm25TextField ? (
            <span className="badge violet">{info.bm25TextField}</span>
          ) : (
            <span className="hint">disabled</span>
          )}
        </div>

        <div className="k">Sparse index</div>
        <div className="v">
          {info.sparseEnabled ? (
            <span className="badge accent">enabled</span>
          ) : (
            <span className="hint">disabled</span>
          )}
        </div>

        <div className="k">Scalar quantization</div>
        <div className="v">
          {info.scalarQuantization ? (
            <span className="badge accent">on</span>
          ) : (
            <span className="hint">off</span>
          )}
        </div>

        <div className="k">HNSW parameters</div>
        <div className="v">
          {info.m != null ? `m=${info.m}` : "—"}
          {info.efConstruction != null ? `, ef_construction=${info.efConstruction}` : ""}
          {info.efSearch != null ? `, ef_search=${info.efSearch}` : ""}
        </div>

        {info.payloadIndexCount != null && (
          <>
            <div className="k">Payload indexes</div>
            <div className="v">{info.payloadIndexCount}</div>
          </>
        )}
      </div>

      <details style={{ marginTop: 14 }}>
        <summary style={{ cursor: "pointer", color: "var(--text-muted)", fontSize: 12 }}>
          Raw spec
        </summary>
        <pre style={{ marginTop: 8 }}>{info.rawSpec}</pre>
      </details>
    </section>
  );
}
