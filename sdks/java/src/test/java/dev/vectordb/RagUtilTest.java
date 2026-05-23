package dev.vectordb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RagUtilTest {
  @Test
  void chunkTextProducesMultipleChunks() {
    List<String> chunks =
        RagUtil.chunkText("one two three four five six seven eight nine ten eleven", 20, 5);
    assertTrue(chunks.size() >= 2);
  }

  @Test
  void expandQueryHasOriginal() {
    List<String> v = RagUtil.expandQuery("Vector Database", 3);
    assertTrue(v.contains("Vector Database"));
  }

  @Test
  void rerankPrefersOverlap() {
    List<RagHit> hits =
        List.of(
            new RagHit("a", 0.9, "cats and dogs", java.util.Map.of(), 0, "d1"),
            new RagHit("b", 0.95, "vector database storage", java.util.Map.of(), 1, "d2"));
    List<RagHit> ranked = RagUtil.rerankByOverlap("vector database", hits, 0);
    assertEquals("b", ranked.get(0).id);
  }
}
