# Filter DSL

VectorDB filters are JSON values evaluated against a point’s payload. Filters
can be used:

- In **REST** as the `filter` field of a search body.
- In **gRPC** as `filter_json` (a JSON string) on `SearchRequest`.

The DSL is implemented in
[`crates/vectordb-core/src/filter.rs`](../../crates/vectordb-core/src/filter.rs).

---

## Compound filters

A filter at the top level is a **compound** with three lists:

```json
{
  "must":     [ <condition>, ... ],
  "must_not": [ <condition>, ... ],
  "should":   [ <condition>, ... ]
}
```

- All `must` conditions must match (logical AND).
- No `must_not` condition may match.
- If `should` is non-empty, **at least one** must match.
- Conditions can themselves be compound filters (nested).

### Empty filter

`{}` — matches every point. Don’t set `filter` at all to skip filtering.

---

## Field conditions

Each condition has a `key` (dotted JSON path) and one operator:

| Operator | Example | Notes |
|----------|---------|-------|
| `match` | `{"key":"category","match":{"value":"books"}}` | exact equality (string/bool/number) |
| `any_of` | `{"key":"tags","any_of":{"any":["sale","fiction"]}}` | OR over candidate values, also handles array fields |
| `range` | `{"key":"price","range":{"gte":10,"lte":50,"gt":null,"lt":null}}` | numeric range; any subset of bounds |
| `exists` | `{"key":"sku","exists":{"exists":true}}` | presence check |

### Dotted paths

JSON payloads are nested. Paths use `.` to descend:

```json
{"key": "meta.author.name", "match": {"value": "Knuth"}}
```

Arrays are matched element-wise: `tags` containing `["a","b"]` matches
`{"key":"tags","match":{"value":"a"}}`.

---

## Examples

### AND of two fields
```json
{
  "must": [
    {"key": "category", "match": {"value": "books"}},
    {"key": "price",    "range": {"lte": 50}}
  ]
}
```

### Tag intersection (array field)
```json
{
  "must": [
    {"key": "tags", "any_of": {"any": ["sale", "new"]}}
  ]
}
```

### Exclude
```json
{
  "must_not": [
    {"key": "out_of_stock", "match": {"value": true}}
  ]
}
```

### OR with at least one match
```json
{
  "should": [
    {"key": "tags",  "any_of": {"any": ["fiction"]}},
    {"key": "price", "range":  {"gte": 10, "lte": 30}}
  ]
}
```

### Nested compound
```json
{
  "must": [
    {"key": "category", "match": {"value": "books"}},
    {
      "should": [
        {"key": "tags",  "any_of": {"any": ["sale"]}},
        {"key": "price", "range":  {"lte": 20}}
      ]
    }
  ]
}
```

### Existence check
```json
{ "must": [ {"key": "sku", "exists": {"exists": true}} ] }
```

---

## Performance

When a `must` condition references a payload-indexed field, the engine uses an
**indexed brute-force plan**: candidate ids come from the index, then distances
are computed only for those points. This wins when the filter is selective.

| `payload_indexes[].kind` | Useful for |
|--------------------------|------------|
| `keyword` | exact `match` / `any_of` over strings/booleans/integers |
| `numeric` | `range`, `match` over numeric fields |
| `bool` | `match` over booleans |

If the filter isn’t selective enough, the planner falls back to **HNSW
oversearch + post-filter**: it searches `k · 16` (capped at 1024) and discards
non-matching results.

> Always declare an index when you expect to filter often on a field. Indexes
> are maintained on every upsert / delete.

---

## Using from REST

```bash
curl -s -X POST :8080/v1/collections/books/search \
  -H 'Content-Type: application/json' \
  -d '{
    "vector":[1,0,0],
    "top_k":5,
    "filter":{
      "must":[
        {"key":"category","match":{"value":"books"}},
        {"key":"price","range":{"lte":50}}
      ]
    }
  }'
```

## Using from gRPC

`filter_json` is the same JSON, sent as a string field:

```rust
let filter = json!({
    "must":[{"key":"category","match":{"value":"books"}}]
});
client.search_with("books", vec, 5, vec![], filter.to_string()).await?;
```
