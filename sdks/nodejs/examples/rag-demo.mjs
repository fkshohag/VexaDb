import { createHash } from "node:crypto";
import { VectorDbClient, RagPipeline } from "../dist/index.js";

const DIM = 32;

function fakeEmbed(text) {
  const digest = createHash("sha256").update(text).digest();
  const vec = Array.from({ length: DIM }, (_, i) => (digest[i % digest.length] / 255) * 2 - 1);
  const norm = Math.hypot(...vec) || 1;
  return vec.map((x) => x / norm);
}

const base = process.env.VECTORDB_URL ?? "http://127.0.0.1:8080";
const client = new VectorDbClient(base, { apiKey: process.env.VECTORDB_API_KEY });
const rag = new RagPipeline(client, "rag_demo_js", DIM, fakeEmbed);

await rag.ingest([
  {
    id: "intro",
    text:
      "Vector databases store high-dimensional embeddings for similarity search. " +
      "They power RAG and recommendation systems.",
  },
  {
    id: "ops",
    text:
      "VectorDB supports HNSW indexing, metadata filters, hybrid BM25+dense search, " +
      "and Raft replication.",
  },
]);

const hits = await rag.query("hybrid search and replication", { topK: 3, rerank: true });
for (const hit of hits) {
  console.log(`${hit.score.toFixed(4)} [${hit.documentId}] ${hit.text.slice(0, 80)}...`);
}
