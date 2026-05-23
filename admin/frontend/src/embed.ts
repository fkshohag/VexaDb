// Deterministic, dependency-free text → vector embedder for the admin UI.
//
// This is *not* a semantic embedding model — it hashes unigrams, bigrams,
// and character 3-grams into a fixed-dim vector and L2-normalizes. Two
// texts with high token overlap will land near each other; two texts that
// merely *mean* the same thing won't. It's intended for demos, dev work,
// and as a useful zero-config option when paired with BM25.
//
// For production-quality semantic search use a real embedding model
// (OpenAI, sentence-transformers, BGE, Cohere, ...). The exact same
// `values` field accepts those vectors — just match the collection's dim.
//
// Algorithm (matches `scripts/rag_curl.sh` for parity):
//   - lowercase + split on word characters
//   - bump unigrams (weight 1.0), adjacent bigrams (0.7), char-3-grams (0.3)
//   - hash each feature with DJB2 mod dim → bucket index
//   - L2-normalize so cosine distance is well-behaved

function djb2(s: string): number {
  let h = 5381;
  for (let i = 0; i < s.length; i++) {
    // Math.imul gives proper 32-bit signed multiplication; the |0 keeps it i32.
    h = (Math.imul(h, 33) + s.charCodeAt(i)) | 0;
  }
  // Convert to unsigned so the modulo below is non-negative.
  return h >>> 0;
}

export function hashEmbed(text: string, dim: number): number[] {
  if (dim <= 0) throw new Error("dim must be positive");
  const v = new Array<number>(dim).fill(0);
  const bump = (key: string, weight: number) => {
    v[djb2(key) % dim] += weight;
  };

  const tokens = (text.toLowerCase().match(/\w+/g) ?? []).filter(Boolean);
  let prev = "";
  for (const t of tokens) {
    bump(`u\t${t}`, 1.0);
    if (prev) bump(`b\t${prev}\t${t}`, 0.7);
    prev = t;
    for (let i = 0; i + 3 <= t.length; i++) {
      bump(`c\t${t.slice(i, i + 3)}`, 0.3);
    }
  }

  let ss = 0;
  for (const x of v) ss += x * x;
  const norm = Math.sqrt(ss);
  if (norm === 0) return v;
  for (let i = 0; i < dim; i++) v[i] /= norm;
  return v;
}
