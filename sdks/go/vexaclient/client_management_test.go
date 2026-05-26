package vexaclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vectordb/vectordb/sdks/go/entity"
	"github.com/vectordb/vectordb/sdks/go/index"
)

// mgmtStubGateway emulates the management surface of the REST gateway:
// `/v1/collections/:n/indexes`, `/v1/collections/:n/load`,
// `/v1/collections/:n/flush`, `/v1/collections/:n/compact`,
// `/v1/compactions/:id`, and `/v1/collections/:n/segments`.
//
// State is intentionally minimal — we focus on round-tripping requests
// and responses so the SDK builders stay covered.
type mgmtStubGateway struct {
	mu                   sync.Mutex
	indexes              map[string][]storedIndex // collection -> indexes
	knownCollections     map[string]bool
	compactions          map[uint64]storedCompaction
	partitions           map[string][]string // collection -> partition names
	partitionRows        map[string]map[string]int
	createCalls          atomic.Int32
	dropCalls            atomic.Int32
	flushCalls           atomic.Int32
	compactCalls         atomic.Int32
	loadCalls            atomic.Int32
	releaseCalls         atomic.Int32
	createPartCalls      atomic.Int32
	dropPartCalls        atomic.Int32
	lastCreateBody       atomic.Pointer[map[string]any]
	lastAlterPropsBody   atomic.Pointer[map[string]any]
	lastDropPropsBody    atomic.Pointer[map[string]any]
	nextCompaction       atomic.Uint64
}

type storedIndex struct {
	Name   string            `json:"name"`
	Field  string            `json:"field"`
	Kind   string            `json:"kind"`
	Scope  string            `json:"scope"`
	Params map[string]string `json:"params"`
}

type storedCompaction struct {
	id            uint64
	collection    string
	state         string
	entriesBefore uint64
	entriesAfter  uint64
	startedMs     uint64
	finishedMs    uint64
}

func newMgmtStub() *mgmtStubGateway {
	s := &mgmtStubGateway{
		indexes:          map[string][]storedIndex{},
		knownCollections: map[string]bool{"docs": true},
		compactions:      map[uint64]storedCompaction{},
		partitions:       map[string][]string{"docs": {"_default"}},
		partitionRows:    map[string]map[string]int{"docs": {"_default": 0}},
	}
	s.nextCompaction.Store(1)
	return s
}

func (s *mgmtStubGateway) addCollection(name string) {
	s.mu.Lock()
	s.knownCollections[name] = true
	s.mu.Unlock()
}

func (s *mgmtStubGateway) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "0.1.0"})
	})

	mux.HandleFunc("/v1/collections/", s.handleCollection)
	mux.HandleFunc("/v1/compactions/", s.handleCompaction)

	return httptest.NewServer(mux)
}

func (s *mgmtStubGateway) handleCollection(w http.ResponseWriter, r *http.Request) {
	// path layout: /v1/collections/<name>/<segment>[/<rest...>]
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/collections/"), "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	name := parts[0]
	s.mu.Lock()
	known := s.knownCollections[name]
	s.mu.Unlock()
	if !known {
		http.Error(w, "collection not found", http.StatusNotFound)
		return
	}
	segment := parts[1]
	rest := parts[2:]

	switch segment {
	case "indexes":
		s.handleIndexes(w, r, name, rest)
	case "partitions":
		s.handlePartitions(w, r, name, rest)
	case "load":
		if r.Method == http.MethodPost {
			s.loadCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "Loaded", "progress": 100})
		}
	case "release":
		if r.Method == http.MethodPost {
			s.releaseCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		}
	case "load-state":
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "Loaded", "progress": 100})
	case "refresh-load":
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "Loaded", "progress": 100, "vectors_reindexed": 42})
	case "flush":
		s.flushCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"collection":          "default/" + name,
			"flush_ts_ms":         uint64(time.Now().UnixMilli()),
			"segment_ids":         []uint64{0xdead},
			"flushed_segment_ids": []uint64{0xdead},
			"wal_entries":         3,
		})
	case "compact":
		s.compactCalls.Add(1)
		id := s.nextCompaction.Add(1)
		s.mu.Lock()
		s.compactions[id] = storedCompaction{
			id:           id,
			collection:   "default/" + name,
			state:        "Completed",
			entriesAfter: 7,
			finishedMs:   uint64(time.Now().UnixMilli()),
		}
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"compaction_id": id})
	case "segments":
		segs := []map[string]any{
			{"id": 0x1234, "collection": "default/" + name, "num_rows": 12, "state": "Growing", "source": "wal"},
			{"id": 0x5678, "collection": "default/" + name, "num_rows": 0, "state": "Flushed", "source": "snapshot:abc"},
		}
		_ = json.NewEncoder(w).Encode(segs)
	default:
		http.NotFound(w, r)
	}
}

func (s *mgmtStubGateway) handleIndexes(w http.ResponseWriter, r *http.Request, name string, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		s.mu.Lock()
		out := append([]storedIndex(nil), s.indexes[name]...)
		s.mu.Unlock()
		out = append(out, storedIndex{
			Name: "vector", Field: "vector", Kind: "hnsw", Scope: "vector",
		})
		_ = json.NewEncoder(w).Encode(out)

	case len(rest) == 0 && r.Method == http.MethodPost:
		s.createCalls.Add(1)
		body := map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.lastCreateBody.Store(&body)
		field, _ := body["field"].(string)
		kind, _ := body["kind"].(string)
		idxName, _ := body["index_name"].(string)
		if idxName == "" {
			idxName = field
		}
		if kind != "hnsw" {
			s.mu.Lock()
			s.indexes[name] = append(s.indexes[name], storedIndex{
				Name: idxName, Field: field, Kind: kind, Scope: "scalar",
			})
			s.mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":  idxName,
			"field": field,
			"kind":  kind,
			"state": "Finished",
		})

	case len(rest) == 1 && r.Method == http.MethodGet:
		idxName := rest[0]
		s.mu.Lock()
		var found *storedIndex
		for i, idx := range s.indexes[name] {
			if idx.Name == idxName {
				found = &s.indexes[name][i]
				break
			}
		}
		s.mu.Unlock()
		if idxName == "vector" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name": "vector", "field": "vector", "kind": "hnsw", "scope": "vector",
				"state": "Finished", "total_rows": 100, "indexed_rows": 100, "pending_index_rows": 0,
			})
			return
		}
		if found == nil {
			http.Error(w, "index not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":  found.Name,
			"field": found.Field,
			"kind":  found.Kind,
			"scope": found.Scope,
			"state": "Finished", "total_rows": 100, "indexed_rows": 100, "pending_index_rows": 0,
		})

	case len(rest) == 1 && r.Method == http.MethodDelete:
		s.dropCalls.Add(1)
		idxName := rest[0]
		s.mu.Lock()
		kept := s.indexes[name][:0]
		for _, idx := range s.indexes[name] {
			if idx.Name != idxName {
				kept = append(kept, idx)
			}
		}
		s.indexes[name] = kept
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	case len(rest) == 2 && rest[1] == "properties" && r.Method == http.MethodPatch:
		body := map[string]any{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.lastAlterPropsBody.Store(&body)
		w.WriteHeader(http.StatusNoContent)

	case len(rest) == 2 && rest[1] == "properties" && r.Method == http.MethodDelete:
		body := map[string]any{}
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		s.lastDropPropsBody.Store(&body)
		w.WriteHeader(http.StatusNoContent)

	default:
		http.NotFound(w, r)
	}
}

func (s *mgmtStubGateway) handlePartitions(w http.ResponseWriter, r *http.Request, name string, rest []string) {
	switch {
	case len(rest) == 0 && r.Method == http.MethodGet:
		s.mu.Lock()
		parts := append([]string(nil), s.partitions[name]...)
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"partitions": parts})

	case len(rest) == 0 && r.Method == http.MethodPost:
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			PartitionName string `json:"partition_name"`
		}
		if err := json.Unmarshal(raw, &body); err != nil || body.PartitionName == "" {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		s.createPartCalls.Add(1)
		s.mu.Lock()
		for _, p := range s.partitions[name] {
			if p == body.PartitionName {
				s.mu.Unlock()
				http.Error(w, "partition exists", http.StatusBadRequest)
				return
			}
		}
		s.partitions[name] = append(s.partitions[name], body.PartitionName)
		if s.partitionRows[name] == nil {
			s.partitionRows[name] = map[string]int{}
		}
		s.partitionRows[name][body.PartitionName] = 0
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})

	case len(rest) == 1 && r.Method == http.MethodGet:
		partition := rest[0]
		s.mu.Lock()
		exists := false
		for _, p := range s.partitions[name] {
			if p == partition {
				exists = true
				break
			}
		}
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"exists": exists})

	case len(rest) == 1 && r.Method == http.MethodDelete:
		partition := rest[0]
		if partition == "_default" {
			http.Error(w, "cannot drop default partition", http.StatusBadRequest)
			return
		}
		s.dropPartCalls.Add(1)
		s.mu.Lock()
		found := false
		out := make([]string, 0, len(s.partitions[name]))
		for _, p := range s.partitions[name] {
			if p == partition {
				found = true
				continue
			}
			out = append(out, p)
		}
		s.partitions[name] = out
		delete(s.partitionRows[name], partition)
		s.mu.Unlock()
		if !found {
			http.Error(w, "partition not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})

	case len(rest) == 2 && rest[1] == "stats" && r.Method == http.MethodGet:
		partition := rest[0]
		s.mu.Lock()
		rows, ok := s.partitionRows[name][partition]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "partition not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"row_count":      itoa(rows),
			"partition_name": partition,
			"collection":     "default/" + name,
		})

	default:
		http.NotFound(w, r)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := make([]byte, 0, 16)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func (s *mgmtStubGateway) handleCompaction(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/compactions/")
	var n uint64
	for _, ch := range id {
		if ch < '0' || ch > '9' {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		n = n*10 + uint64(ch-'0')
	}
	s.mu.Lock()
	c, ok := s.compactions[n]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"compaction_id":  c.id,
		"collection":     c.collection,
		"state":          c.state,
		"entries_before": c.entriesBefore,
		"entries_after":  c.entriesAfter,
		"started_ms":     c.startedMs,
		"finished_ms":    c.finishedMs,
		"error":          nil,
	})
}

func newMgmtTestClient(t *testing.T, stub *mgmtStubGateway) (*Client, *httptest.Server) {
	t.Helper()
	srv := stub.server(t)
	cli, err := New(context.Background(), &ClientConfig{Address: srv.URL})
	if err != nil {
		srv.Close()
		t.Fatalf("New client: %v", err)
	}
	return cli, srv
}

// ---- Index ---------------------------------------------------------------

func TestCreateAndListIndexes(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	hnsw := index.NewHNSWIndex(index.COSINE, 16, 200)
	task, err := cli.CreateIndex(ctx, NewCreateIndexOption("docs", "vector", hnsw))
	if err != nil {
		t.Fatalf("CreateIndex hnsw: %v", err)
	}
	// HNSW path triggers reindex + opaque properties; the gateway returns
	// `Finished` state so Await should resolve immediately.
	c2, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := task.Await(c2); err != nil {
		t.Fatalf("Await hnsw: %v", err)
	}

	inv := index.NewInvertedIndex()
	if _, err := cli.CreateIndex(ctx, NewCreateIndexOption("docs", "category", inv).WithIndexName("cat_idx")); err != nil {
		t.Fatalf("CreateIndex inverted: %v", err)
	}

	names, err := cli.ListIndexes(ctx, NewListIndexOption("docs"))
	if err != nil {
		t.Fatalf("ListIndexes: %v", err)
	}
	wantContains := func(want string) {
		t.Helper()
		for _, n := range names {
			if n == want {
				return
			}
		}
		t.Fatalf("ListIndexes missing %q in %v", want, names)
	}
	wantContains("cat_idx")
	wantContains("vector")

	// Field filter narrows the result set.
	filtered, err := cli.ListIndexes(ctx, NewListIndexOption("docs").WithFieldName("category"))
	if err != nil {
		t.Fatalf("ListIndexes filtered: %v", err)
	}
	if len(filtered) != 1 || filtered[0] != "cat_idx" {
		t.Fatalf("filtered = %v, want [cat_idx]", filtered)
	}
}

func TestDescribeIndex(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	if _, err := cli.CreateIndex(ctx, NewCreateIndexOption("docs", "category", index.NewInvertedIndex()).WithIndexName("cat_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	desc, err := cli.DescribeIndex(ctx, NewDescribeIndexOption("docs", "cat_idx"))
	if err != nil {
		t.Fatalf("DescribeIndex: %v", err)
	}
	if desc.IndexName != "cat_idx" || desc.Field != "category" {
		t.Fatalf("desc = %+v", desc)
	}
	if desc.State != index.IndexStateFinished {
		t.Fatalf("state = %v", desc.State)
	}

	// Vector index shortcut.
	v, err := cli.DescribeIndex(ctx, NewDescribeIndexOption("docs", "vector"))
	if err != nil {
		t.Fatalf("DescribeIndex vector: %v", err)
	}
	if v.Scope != "vector" {
		t.Fatalf("vector scope = %q", v.Scope)
	}
}

func TestDropIndex(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()
	if _, err := cli.CreateIndex(ctx, NewCreateIndexOption("docs", "category", index.NewInvertedIndex()).WithIndexName("cat_idx")); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := cli.DropIndex(ctx, NewDropIndexOption("docs", "cat_idx")); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}
	if stub.dropCalls.Load() != 1 {
		t.Fatalf("dropCalls = %d", stub.dropCalls.Load())
	}
	names, _ := cli.ListIndexes(ctx, NewListIndexOption("docs"))
	for _, n := range names {
		if n == "cat_idx" {
			t.Fatalf("cat_idx still present after drop: %v", names)
		}
	}
}

func TestAlterAndDropIndexProperties(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()
	_, _ = cli.CreateIndex(ctx, NewCreateIndexOption("docs", "category", index.NewInvertedIndex()).WithIndexName("cat_idx"))

	if err := cli.AlterIndexProperties(ctx,
		NewAlterIndexPropertiesOption("docs", "cat_idx").WithProperty("mmap.enabled", true),
	); err != nil {
		t.Fatalf("AlterIndexProperties: %v", err)
	}
	if got := stub.lastAlterPropsBody.Load(); got == nil {
		t.Fatalf("AlterIndexProperties body not captured")
	} else {
		set, _ := (*got)["set"].(map[string]any)
		if v, _ := set["mmap.enabled"].(string); v != "true" {
			t.Fatalf("set = %v", set)
		}
	}
	if err := cli.DropIndexProperties(ctx, NewDropIndexPropertiesOption("docs", "cat_idx", "mmap.enabled")); err != nil {
		t.Fatalf("DropIndexProperties: %v", err)
	}
	if got := stub.lastDropPropsBody.Load(); got == nil {
		t.Fatalf("DropIndexProperties body not captured")
	} else {
		keys, _ := (*got)["keys"].([]any)
		if len(keys) != 1 || keys[0] != "mmap.enabled" {
			t.Fatalf("keys = %v", keys)
		}
	}
}

// ---- Load / Release ------------------------------------------------------

func TestLoadAndReleaseCollection(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	task, err := cli.LoadCollection(ctx, NewLoadCollectionOption("docs").WithReplica(2))
	if err != nil {
		t.Fatalf("LoadCollection: %v", err)
	}
	c2, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := task.Await(c2); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if stub.loadCalls.Load() != 1 {
		t.Fatalf("loadCalls = %d", stub.loadCalls.Load())
	}

	if err := cli.ReleaseCollection(ctx, NewReleaseCollectionOption("docs")); err != nil {
		t.Fatalf("ReleaseCollection: %v", err)
	}
	if stub.releaseCalls.Load() != 1 {
		t.Fatalf("releaseCalls = %d", stub.releaseCalls.Load())
	}

	state, err := cli.GetLoadState(ctx, NewGetLoadStateOption("docs"))
	if err != nil {
		t.Fatalf("GetLoadState: %v", err)
	}
	if state.State != entity.LoadStateLoaded || state.Progress != 100 {
		t.Fatalf("state = %+v", state)
	}
}

func TestLoadPartitionsAndReleasePartitions(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	if _, err := cli.LoadPartitions(ctx, NewLoadPartitionsOption("docs", "p1", "p2")); err != nil {
		t.Fatalf("LoadPartitions: %v", err)
	}
	// Aliases to LoadCollection — partitions are ignored on VexaDb.
	if stub.loadCalls.Load() != 1 {
		t.Fatalf("loadCalls = %d", stub.loadCalls.Load())
	}
	if err := cli.ReleasePartitions(ctx, NewReleasePartitionsOption("docs", "p1")); err != nil {
		t.Fatalf("ReleasePartitions: %v", err)
	}
	if stub.releaseCalls.Load() != 1 {
		t.Fatalf("releaseCalls = %d", stub.releaseCalls.Load())
	}
}

// ---- Flush / Compact / Segments -----------------------------------------

func TestFlushCollection(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	task, err := cli.Flush(ctx, NewFlushOption("docs"))
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := task.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}
	segs, flushed, ts, _ := task.GetFlushStats()
	if len(segs) == 0 || len(flushed) == 0 || ts == 0 {
		t.Fatalf("GetFlushStats=%v %v %v", segs, flushed, ts)
	}
	if stub.flushCalls.Load() != 1 {
		t.Fatalf("flushCalls = %d", stub.flushCalls.Load())
	}
}

func TestCompactAndPollState(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	id, err := cli.Compact(ctx, NewCompactOption("docs"))
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if id == 0 {
		t.Fatalf("compaction id = 0")
	}
	info, err := cli.GetCompactionState(ctx, NewGetCompactionStateOption(id))
	if err != nil {
		t.Fatalf("GetCompactionState: %v", err)
	}
	if info.State != entity.CompactionStateCompleted {
		t.Fatalf("state = %v", info.State)
	}
	if info.CompactionID != uint64(id) {
		t.Fatalf("id round-trip: got=%d want=%d", info.CompactionID, id)
	}
}

func TestPersistentSegmentInfo(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	segs, err := cli.GetPersistentSegmentInfo(ctx, NewGetPersistentSegmentInfoOption("docs"))
	if err != nil {
		t.Fatalf("GetPersistentSegmentInfo: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("len=%d, want 2", len(segs))
	}
	if !segs[1].Flushed() {
		t.Fatalf("expected last segment Flushed=true: %+v", segs[1])
	}
	if segs[0].State != entity.SegmentStateGrowing {
		t.Fatalf("first segment state = %v", segs[0].State)
	}
}

// ---- Refresh load --------------------------------------------------------

func TestRefreshLoad(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()
	task, err := cli.RefreshLoad(ctx, NewRefreshLoadOption("docs"))
	if err != nil {
		t.Fatalf("RefreshLoad: %v", err)
	}
	if err := task.Await(ctx); err != nil {
		t.Fatalf("Await: %v", err)
	}
}

// ---- Partitions ----------------------------------------------------------

func TestCreateListHasPartition(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	// Fresh collection starts with only _default.
	names, err := cli.ListPartitions(ctx, NewListPartitionOption("docs"))
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	if len(names) != 1 || names[0] != "_default" {
		t.Fatalf("expected only _default, got %v", names)
	}

	if err := cli.CreatePartition(ctx, NewCreatePartitionOption("docs", "hot")); err != nil {
		t.Fatalf("CreatePartition: %v", err)
	}
	if got := stub.createPartCalls.Load(); got != 1 {
		t.Fatalf("createPartCalls=%d want 1", got)
	}

	has, err := cli.HasPartition(ctx, NewHasPartitionOption("docs", "hot"))
	if err != nil {
		t.Fatalf("HasPartition: %v", err)
	}
	if !has {
		t.Fatalf("HasPartition(hot)=false")
	}

	has, err = cli.HasPartition(ctx, NewHasPartitionOption("docs", "cold"))
	if err != nil {
		t.Fatalf("HasPartition cold: %v", err)
	}
	if has {
		t.Fatalf("HasPartition(cold)=true")
	}

	names, _ = cli.ListPartitions(ctx, NewListPartitionOption("docs"))
	if len(names) != 2 {
		t.Fatalf("expected 2 partitions after create, got %v", names)
	}
}

func TestDropPartition(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	_ = cli.CreatePartition(ctx, NewCreatePartitionOption("docs", "warm"))
	if err := cli.DropPartition(ctx, NewDropPartitionOption("docs", "warm")); err != nil {
		t.Fatalf("DropPartition: %v", err)
	}
	if got := stub.dropPartCalls.Load(); got != 1 {
		t.Fatalf("dropPartCalls=%d want 1", got)
	}
	has, _ := cli.HasPartition(ctx, NewHasPartitionOption("docs", "warm"))
	if has {
		t.Fatalf("warm partition should be gone")
	}

	// Cannot drop default.
	if err := cli.DropPartition(ctx, NewDropPartitionOption("docs", "_default")); err == nil {
		t.Fatalf("expected error dropping _default")
	}
}

func TestGetPartitionStats(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()

	stub.mu.Lock()
	stub.partitionRows["docs"]["_default"] = 42
	stub.mu.Unlock()

	stats, err := cli.GetPartitionStats(ctx,
		NewGetPartitionStatsOption("docs", "_default"))
	if err != nil {
		t.Fatalf("GetPartitionStats: %v", err)
	}
	if stats["row_count"] != "42" {
		t.Fatalf("row_count = %q want 42", stats["row_count"])
	}
	if stats["partition_name"] != "_default" {
		t.Fatalf("partition_name = %q", stats["partition_name"])
	}
	if stats["collection"] != "default/docs" {
		t.Fatalf("collection = %q", stats["collection"])
	}
}

func TestPartitionOptionValidation(t *testing.T) {
	stub := newMgmtStub()
	cli, srv := newMgmtTestClient(t, stub)
	defer srv.Close()
	ctx := context.Background()
	if err := cli.CreatePartition(ctx, NewCreatePartitionOption("", "p")); err == nil {
		t.Fatal("expected error on empty collection")
	}
	if err := cli.CreatePartition(ctx, NewCreatePartitionOption("docs", "")); err == nil {
		t.Fatal("expected error on empty partition")
	}
	if _, err := cli.HasPartition(ctx, NewHasPartitionOption("docs", "")); err == nil {
		t.Fatal("expected error on empty partition for HasPartition")
	}
	if _, err := cli.ListPartitions(ctx, &ListPartitionsOption{}); err == nil {
		t.Fatal("expected error on empty collection for ListPartitions")
	}
}
