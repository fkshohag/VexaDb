package dev.vectordb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Chunk documents, embed, upsert, and query a VectorDB collection. */
public class RagPipeline {
  private final VectorDbClient client;
  private final String collection;
  private final int dimension;
  private final EmbedFunction embed;
  public String textField = "text";
  public int chunkSize = 512;
  public int overlap = 64;
  public String metric = "cosine";
  public String bm25TextField;
  public boolean sparseEnabled;
  private boolean collectionReady;

  public RagPipeline(
      VectorDbClient client, String collection, int dimension, EmbedFunction embed) {
    this.client = client;
    this.collection = collection;
    this.dimension = dimension;
    this.embed = embed;
  }

  public void ensureCollection() throws Exception {
    if (collectionReady) return;
    if (!client.listCollections().contains(collection)) {
      String bm25 = bm25TextField != null ? bm25TextField : textField;
      client.createCollection(collection, dimension, metric, null, sparseEnabled, bm25, false);
    }
    collectionReady = true;
  }

  public long ingest(List<RagDocument> documents, boolean bulk) throws Exception {
    ensureCollection();
    List<Map<String, Object>> points = new ArrayList<>();
    for (RagDocument doc : documents) {
      List<String> chunks = RagUtil.chunkText(doc.text, chunkSize, overlap);
      for (int i = 0; i < chunks.size(); i++) {
        String chunk = chunks.get(i);
        List<Double> vector = embed.embed(chunk);
        if (vector.size() != dimension) {
          throw new IllegalArgumentException(
              "embedding dimension " + vector.size() + " != " + dimension);
        }
        Map<String, Object> payload = new LinkedHashMap<>(doc.payload);
        payload.put(textField, chunk);
        payload.put("document_id", doc.id);
        payload.put("chunk_index", i);
        Map<String, Object> point = new LinkedHashMap<>();
        point.put("id", doc.id + "#" + i);
        point.put("values", vector);
        point.put("payload", payload);
        points.add(point);
      }
    }
    if (points.isEmpty()) return 0;
    return bulk ? client.bulkUpsert(collection, points, 500) : client.upsert(collection, points);
  }

  public List<RagHit> query(
      String queryText,
      int topK,
      Map<String, Object> filter,
      String searchMode,
      double hybridAlpha,
      boolean multiQuery,
      boolean rerank)
      throws Exception {
    ensureCollection();
    if (topK <= 0) topK = 5;
    List<String> queries = multiQuery ? RagUtil.expandQuery(queryText, 3) : List.of(queryText);
    Map<String, RagHit> byId = new HashMap<>();
    for (String q : queries) {
      List<Double> vector = embed.embed(q);
      String mode = searchMode != null ? searchMode : "dense";
      String textQ = "dense".equals(mode) ? null : q;
      List<Map<String, Object>> raw =
          client.search(collection, vector, topK, filter, textQ, mode, hybridAlpha);
      for (int rank = 0; rank < raw.size(); rank++) {
        Map<String, Object> row = raw.get(rank);
        String id = (String) row.get("id");
        Map<String, Object> point = client.getPoint(collection, id);
        Map<String, Object> payload =
            point != null && point.get("payload") instanceof Map
                ? (Map<String, Object>) point.get("payload")
                : Map.of();
        String text = String.valueOf(payload.getOrDefault(textField, ""));
        String docId = String.valueOf(payload.getOrDefault("document_id", id.split("#")[0]));
        int chunkIndex =
            payload.get("chunk_index") instanceof Number n ? n.intValue() : 0;
        double rrf = 1.0 / (60 + rank + 1);
        RagHit existing = byId.get(id);
        if (existing != null) {
          byId.put(
              id,
              new RagHit(
                  id,
                  existing.score + rrf,
                  text,
                  payload,
                  chunkIndex,
                  docId));
        } else {
          byId.put(id, new RagHit(id, rrf, text, payload, chunkIndex, docId));
        }
      }
    }
    List<RagHit> hits = new ArrayList<>(byId.values());
    hits.sort(Comparator.comparingDouble(h -> -h.score));
    if (hits.size() > topK) hits = hits.subList(0, topK);
    if (rerank) hits = RagUtil.rerankByOverlap(queryText, hits, topK);
    return hits;
  }
}
