package dev.vectordb;

import java.util.Map;

public class RagHit {
  public final String id;
  public final double score;
  public final String text;
  public final Map<String, Object> payload;
  public final int chunkIndex;
  public final String documentId;

  public RagHit(
      String id,
      double score,
      String text,
      Map<String, Object> payload,
      int chunkIndex,
      String documentId) {
    this.id = id;
    this.score = score;
    this.text = text;
    this.payload = payload;
    this.chunkIndex = chunkIndex;
    this.documentId = documentId;
  }
}
