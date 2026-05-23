package dev.vectordb;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/** Minimal RAG demo (deterministic pseudo-embeddings for local testing). */
public final class RagDemo {
  private static final int DIM = 32;

  private RagDemo() {}

  public static void main(String[] args) throws Exception {
    String base = System.getenv().getOrDefault("VECTORDB_URL", "http://127.0.0.1:8080");
    String apiKey = System.getenv("VECTORDB_API_KEY");
    VectorDbClient client = new VectorDbClient(base, apiKey);
    RagPipeline rag = new RagPipeline(client, "rag_demo_java", DIM, RagDemo::fakeEmbed);

    rag.ingest(
        List.of(
            new RagDocument(
                "intro",
                "Vector databases store high-dimensional embeddings for similarity search. "
                    + "They power RAG and recommendation systems."),
            new RagDocument(
                "ops",
                "VectorDB supports HNSW indexing, metadata filters, hybrid BM25+dense search, "
                    + "and Raft replication.")),
        true);

    List<RagHit> hits =
        rag.query("hybrid search and replication", 3, null, "dense", 0.5, false, true);
    for (RagHit h : hits) {
      String snip = h.text.length() > 80 ? h.text.substring(0, 80) + "..." : h.text;
      System.out.printf("%.4f [%s] %s%n", h.score, h.documentId, snip);
    }
  }

  static List<Double> fakeEmbed(String text) throws Exception {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] digest = md.digest(text.getBytes(StandardCharsets.UTF_8));
    List<Double> vec = new ArrayList<>(DIM);
    double norm = 0;
    for (int i = 0; i < DIM; i++) {
      int b = digest[i % digest.length] & 0xff;
      double v = (b / 255.0) * 2 - 1;
      vec.add(v);
      norm += v * v;
    }
    norm = Math.sqrt(norm);
    if (norm == 0) norm = 1;
    List<Double> out = new ArrayList<>(DIM);
    for (double v : vec) out.add(v / norm);
    return out;
  }
}
