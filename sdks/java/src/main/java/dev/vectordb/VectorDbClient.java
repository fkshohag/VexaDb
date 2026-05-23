package dev.vectordb;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** HTTP/JSON client for the VectorDB REST gateway. */
public class VectorDbClient {
  private static final Gson GSON = new GsonBuilder().create();
  private static final Type LIST_STRING = new TypeToken<List<String>>() {}.getType();
  private static final Type MAP_STRING_OBJECT = new TypeToken<Map<String, Object>>() {}.getType();
  private static final Type LIST_MAP = new TypeToken<List<Map<String, Object>>>() {}.getType();

  private final String baseUrl;
  private final String apiKey;
  private final HttpClient http;

  public VectorDbClient(String baseUrl) {
    this(baseUrl, null);
  }

  public VectorDbClient(String baseUrl, String apiKey) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.apiKey = apiKey;
    this.http =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).build();
  }

  private HttpRequest.Builder newRequest(String method, String path) {
    HttpRequest.Builder b =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .timeout(Duration.ofSeconds(60));
    if (apiKey != null && !apiKey.isEmpty()) {
      b.header("x-api-key", apiKey).header("Authorization", "Bearer " + apiKey);
    }
    return b;
  }

  private <T> T doJson(String method, String path, Object body, Type type)
      throws IOException, InterruptedException {
    HttpRequest.Builder b = newRequest(method, path);
    if (body != null) {
      b.header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(GSON.toJson(body)));
    } else if ("GET".equals(method)) {
      b.GET();
    } else if ("DELETE".equals(method)) {
      b.DELETE();
    } else {
      b.method(method, HttpRequest.BodyPublishers.noBody());
    }
    HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
      throw new VectorDbException(resp.statusCode(), resp.body());
    }
    if (type == null || resp.body() == null || resp.body().isEmpty()) {
      return null;
    }
    return GSON.fromJson(resp.body(), type);
  }

  public Map<String, String> health() throws IOException, InterruptedException {
    return doJson("GET", "/health", null, new TypeToken<Map<String, String>>() {}.getType());
  }

  public boolean live() throws IOException, InterruptedException {
    HttpRequest req = newRequest("GET", "/live").GET().build();
    return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
  }

  public boolean ready() throws IOException, InterruptedException {
    HttpRequest req = newRequest("GET", "/ready").GET().build();
    return http.send(req, HttpResponse.BodyHandlers.ofString()).statusCode() == 200;
  }

  public List<String> listCollections() throws IOException, InterruptedException {
    List<String> out = doJson("GET", "/v1/collections", null, LIST_STRING);
    return out != null ? out : Collections.emptyList();
  }

  public void createCollection(
      String name,
      int dimension,
      String metric,
      List<Map<String, String>> payloadIndexes,
      boolean sparseEnabled,
      String bm25TextField,
      boolean scalarQuantization)
      throws IOException, InterruptedException {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("name", name);
    body.put("dimension", dimension);
    body.put("metric", metric != null ? metric : "cosine");
    body.put("sparse_enabled", sparseEnabled);
    body.put("scalar_quantization", scalarQuantization);
    if (payloadIndexes != null) {
      body.put("payload_indexes", payloadIndexes);
    }
    if (bm25TextField != null) {
      body.put("bm25_text_field", bm25TextField);
    }
    doJson("POST", "/v1/collections", body, null);
  }

  public void deleteCollection(String name) throws IOException, InterruptedException {
    doJson("DELETE", "/v1/collections/" + name, null, null);
  }

  public long upsert(String collection, List<Map<String, Object>> points)
      throws IOException, InterruptedException {
    Map<String, Object> body = Map.of("points", points);
    Map<String, Object> out =
        doJson("POST", "/v1/collections/" + collection + "/upsert", body, MAP_STRING_OBJECT);
    return ((Number) out.get("upserted")).longValue();
  }

  public long bulkUpsert(String collection, List<Map<String, Object>> points, int chunkSize)
      throws IOException, InterruptedException {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("points", points);
    body.put("chunk_size", chunkSize);
    Map<String, Object> out =
        doJson("POST", "/v1/collections/" + collection + "/bulk", body, MAP_STRING_OBJECT);
    return ((Number) out.get("upserted")).longValue();
  }

  public List<Map<String, Object>> search(
      String collection,
      List<Double> vector,
      int topK,
      Map<String, Object> filter,
      String textQuery,
      String searchMode,
      double hybridAlpha)
      throws IOException, InterruptedException {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("vector", vector);
    body.put("top_k", topK);
    body.put("search_mode", searchMode != null ? searchMode : "dense");
    body.put("hybrid_alpha", hybridAlpha);
    if (filter != null) {
      body.put("filter", filter);
    }
    if (textQuery != null && !textQuery.isEmpty()) {
      body.put("text_query", textQuery);
    }
    List<Map<String, Object>> hits =
        doJson("POST", "/v1/collections/" + collection + "/search", body, LIST_MAP);
    return hits != null ? hits : Collections.emptyList();
  }

  public long deletePoints(String collection, List<String> ids)
      throws IOException, InterruptedException {
    Map<String, Object> out =
        doJson(
            "DELETE",
            "/v1/collections/" + collection + "/points",
            Map.of("ids", ids),
            MAP_STRING_OBJECT);
    return ((Number) out.get("deleted")).longValue();
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getPoint(String collection, String id)
      throws IOException, InterruptedException {
    try {
      return doJson(
          "GET",
          "/v1/collections/" + collection + "/points/" + id,
          null,
          MAP_STRING_OBJECT);
    } catch (VectorDbException e) {
      if (e.getStatusCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  public Map<String, Object> compactWal(boolean snapshotFirst)
      throws IOException, InterruptedException {
    return doJson(
        "POST",
        "/v1/admin/compact-wal",
        Map.of("snapshot_first", snapshotFirst),
        MAP_STRING_OBJECT);
  }

  public long reindexCollection(String collection) throws IOException, InterruptedException {
    Map<String, Object> out =
        doJson("POST", "/v1/collections/" + collection + "/reindex", null, MAP_STRING_OBJECT);
    return ((Number) out.get("vectors_reindexed")).longValue();
  }
}
