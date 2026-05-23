# VectorDB Node.js SDK

TypeScript/JavaScript REST client and RAG helpers for [VectorDB](../../README.md).

Requires **Node.js 18+** (native `fetch`).

## Install

```bash
cd sdks/nodejs
npm install
npm run build
```

## Quick start

```javascript
import { VectorDbClient } from "@vectordb/client";

const client = new VectorDbClient("http://127.0.0.1:8080", {
  apiKey: process.env.VECTORDB_API_KEY,
});

await client.createCollection("docs", 3);
await client.upsert("docs", [
  { id: "a", values: [1, 0, 0], payload: { text: "hello" } },
]);
const hits = await client.search("docs", [1, 0, 0], { topK: 5 });
console.log(hits);
```

## RAG pipeline

```javascript
import { VectorDbClient, RagPipeline } from "@vectordb/client";

const embed = async (text) => {
  /* OpenAI, Voyage, local model, etc. */
};

const client = new VectorDbClient("http://127.0.0.1:8080");
const rag = new RagPipeline(client, "kb", 1536, embed);
await rag.ingest([{ id: "doc1", text: "Vector databases store embeddings..." }]);
const hits = await rag.query("what is a vector database?", { topK: 3, rerank: true });
```

Run the demo (after gateway is up):

```bash
VECTORDB_URL=http://127.0.0.1:8080 node examples/rag-demo.mjs
```
