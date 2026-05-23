package dev.vectordb;

import java.util.Map;

public class RagDocument {
  public final String id;
  public final String text;
  public final Map<String, Object> payload;

  public RagDocument(String id, String text) {
    this(id, text, Map.of());
  }

  public RagDocument(String id, String text, Map<String, Object> payload) {
    this.id = id;
    this.text = text;
    this.payload = payload != null ? payload : Map.of();
  }
}
