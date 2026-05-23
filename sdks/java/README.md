# VectorDB Java SDK

REST client and RAG helpers for [VectorDB](../../README.md). Requires **Java 17+**.

## Build

```bash
cd sdks/java
mvn -q package
```

## Example

```java
VectorDbClient client = new VectorDbClient("http://127.0.0.1:8080", System.getenv("VECTORDB_API_KEY"));
client.createCollection("docs", 128, "cosine", null, false, null, false);

List<Map<String, Object>> points = List.of(
    Map.of("id", "a", "values", List.of(1.0, 0.0, 0.0), "payload", Map.of("text", "hello")));
client.upsert("docs", points);

List<Map<String, Object>> hits =
    client.search("docs", List.of(1.0, 0.0, 0.0), 5, null, null, "dense", 0.5);
```

## RAG

```java
RagPipeline rag = new RagPipeline(client, "kb", 1536, openAiEmbed::embed);
rag.ingest(List.of(new RagDocument("1", "Your document...")), true);
List<RagHit> hits = rag.query("your question", 5, null, "dense", 0.5, false, true);
```

Demo (gateway on `:8080`):

```bash
mvn -q exec:java -Dexec.mainClass="dev.vectordb.RagDemo"
```

Tests: `mvn test`
