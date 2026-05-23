package dev.vectordb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class RagUtil {
  private static final Pattern WORD = Pattern.compile("\\w+");

  private RagUtil() {}

  public static List<String> chunkText(String text, int chunkSize, int overlap) {
    if (chunkSize <= 0) chunkSize = 512;
    if (overlap < 0) overlap = 64;
    text = text.trim();
    if (text.isEmpty()) return List.of();
    if (text.length() <= chunkSize) return List.of(text);

    List<String> chunks = new ArrayList<>();
    int start = 0;
    while (start < text.length()) {
      int end = Math.min(start + chunkSize, text.length());
      if (end < text.length()) {
        int split = text.lastIndexOf(' ', start, end);
        if (split > start) end = split;
      }
      String piece = text.substring(start, end).trim();
      if (!piece.isEmpty()) chunks.add(piece);
      if (end >= text.length()) break;
      start = Math.max(end - overlap, start + 1);
    }
    return chunks;
  }

  public static List<String> expandQuery(String query, int maxVariants) {
    String q = query.trim();
    if (q.isEmpty()) return List.of();
    if (maxVariants <= 0) maxVariants = 3;
    List<String> variants = new ArrayList<>();
    variants.add(q);
    String lower = q.toLowerCase(Locale.ROOT);
    if (!lower.equals(q)) variants.add(lower);
    var words = WORD.matcher(q).results().map(m -> m.group()).filter(w -> w.length() > 2).toList();
    if (words.size() >= 2) variants.add(String.join(" ", words));
    Set<String> seen = new HashSet<>();
    List<String> out = new ArrayList<>();
    for (String v : variants) {
      if (seen.add(v)) {
        out.add(v);
        if (out.size() >= maxVariants) break;
      }
    }
    return out;
  }

  public static List<RagHit> rerankByOverlap(String query, List<RagHit> hits, int topK) {
    Set<String> qTokens = tokens(query.toLowerCase(Locale.ROOT));
    if (qTokens.isEmpty()) {
      return topK > 0 && hits.size() > topK ? hits.subList(0, topK) : hits;
    }
    List<RagHit> ranked =
        hits.stream()
            .map(
                h -> {
                  Set<String> tTokens = tokens(h.text.toLowerCase(Locale.ROOT));
                  int overlap = 0;
                  for (String t : qTokens) if (tTokens.contains(t)) overlap++;
                  double boost = (double) overlap / Math.max(qTokens.size(), 1);
                  return new RagHit(
                      h.id, h.score + boost, h.text, h.payload, h.chunkIndex, h.documentId);
                })
            .sorted(Comparator.comparingDouble((RagHit h) -> h.score).reversed())
            .toList();
    return topK > 0 && ranked.size() > topK ? ranked.subList(0, topK) : ranked;
  }

  private static Set<String> tokens(String s) {
    Set<String> out = new HashSet<>();
    for (String w : s.split("\\W+")) {
      if (!w.isEmpty()) out.add(w);
    }
    return out;
  }
}
