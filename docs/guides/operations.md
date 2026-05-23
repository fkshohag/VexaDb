# Operations

Day-to-day runtime operations: snapshots, WAL compaction, online reindex,
backup/restore, capacity planning.

---

## Snapshots

A snapshot is a labelled filesystem copy of `wal.log` + `meta/` (RocksDB).

```bash
# Create
curl -s -X POST :8080/v1/snapshots
# {"id":"snap-1716000000000","created_at_ms":1716000000000,"path":"./data/snapshots/snap-1716000000000"}

# List
curl -s :8080/v1/snapshots

# Delete
curl -s -X DELETE :8080/v1/snapshots/snap-1716000000000
```

gRPC: `CreateSnapshot`, `ListSnapshots`, `DeleteSnapshot`.

### Restore

Snapshots are physical copies — restoring is a stop / copy / start dance:

```bash
# 1. Stop the server
# 2. Replace data files
DATA=./data
rm -rf "$DATA/wal.log" "$DATA/meta"
cp "$DATA/snapshots/snap-XXXX/wal.log" "$DATA/wal.log"
cp -r "$DATA/snapshots/snap-XXXX/meta"  "$DATA/meta"
# 3. Start; WAL replay rebuilds in-memory state
```

For Raft, restore on **all** replicas before re-electing.

---

## WAL compaction

The WAL grows append-only. Compaction rewrites it from the **current
in-memory state**, discarding superseded entries.

```bash
# In-place compaction
curl -s -X POST :8080/v1/admin/compact-wal -H 'Content-Type: application/json' -d '{}'

# Snapshot first (recommended for production)
curl -s -X POST :8080/v1/admin/compact-wal -H 'Content-Type: application/json' \
  -d '{"snapshot_first":true}'
# {"entries_before":12834,"entries_after":215,"snapshot":{"id":"snap-...","path":"..."}}
```

Internals: every `Upsert/Delete` becomes one or more `BulkUpsert` records,
followed by a `Checkpoint` marker. New WAL entries continue appending after
that point.

When to run: after large bulk imports, before major maintenance, or as part of
a periodic operations cron.

---

## Online reindex

Rebuild the HNSW graph for a collection from stored vectors — useful after:

- Drift from many deletes
- Changing an HNSW parameter (you must `delete_collection` + recreate first today)

```bash
curl -s -X POST :8080/v1/collections/embeddings/reindex
# {"vectors_reindexed": 12345}
```

Reads continue against the old index until the swap (under a single write
lock). Writes during reindex are not blocked but the new graph reflects state
at the start of the operation; trailing writes are reapplied via the engine’s
write path.

---

## Bulk import & streaming

For large initial loads, use `BulkUpsert` (chunked) or `ImportStream`
(client-streaming).

REST:

```bash
curl -s -X POST :8080/v1/collections/embeddings/bulk \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"id":"a","values":[1,2,3]}, ...],"chunk_size":500}'
```

Python:

```python
from vectordb import VectorDbClient
c = VectorDbClient("http://127.0.0.1:8080")
c.bulk_upsert("embeddings", points=my_batch, chunk_size=500)
```

Throughput tips:

- Set `sync_wal = false` only for one-off backfills, then snapshot + compact.
- Run multiple gateways/routers in parallel against distinct shards.
- Pre-create collection with payload indexes — adding them later isn’t
  retroactive in this release.

---

## Backup & restore

A backup is a copy of `data/snapshots/snap-<ts>/` (or the entire `data/` dir
while the server is stopped). Snapshots are atomic and labelled.

Recommended flow:

```bash
# 1. Quiesce or accept inflight writes
# 2. Snapshot
curl -s -X POST :8080/v1/snapshots
# 3. Copy snap-<ts> to remote storage (S3 / GCS / NFS)
aws s3 cp ./data/snapshots/snap-XXXX s3://bucket/vectordb/snap-XXXX --recursive
# 4. Compact WAL to keep on-disk size reasonable
curl -s -X POST :8080/v1/admin/compact-wal -d '{}'
```

Restore: download snapshot dir, follow the **Restore** steps above.

---

## Common tasks

### Resize the cluster
1. Snapshot all shards.
2. Stand up new shards with the new `shard_count`.
3. Re-ingest data from your source-of-truth or a backup tool that re-keys
   point ids onto the new shard map.

(Live re-shard is on the future track.)

### Drain a follower
- Stop the process; quorum carries on with the remaining nodes.
- Bring it back; it catches up via Raft `AppendEntries` and WAL replay.

### Failover a leader
- Stop the leader.
- Followers run an election; new leader elected within `election_timeout_ms`.
- SDKs auto-redirect on next write via `x-vectordb-leader`.

### Clear a collection
```bash
curl -s -X DELETE :8080/v1/collections/embeddings
curl -s -X POST   :8080/v1/collections -d '{"name":"embeddings","dimension":1536}'
```

### Inspect storage size
```bash
du -sh data/wal.log data/meta data/snapshots/*
```

---

## Maintenance schedule (suggested)

| Frequency | Task |
|-----------|------|
| Hourly | Scrape Prometheus, alert on error rate / latency |
| Daily | Snapshot + compact WAL |
| Weekly | Verify a snapshot restores onto a staging instance |
| Monthly | Capacity check: RAM, disk, vector counts |
| As needed | Reindex when delete ratio is high |
