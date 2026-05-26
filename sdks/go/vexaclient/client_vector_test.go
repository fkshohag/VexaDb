package vexaclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// vecStubGateway is a focused HTTP stub for the Vector module
// (Insert/Upsert/Delete/Get/Query/Search/HybridSearch/Scroll/Analyze).
// Each handler captures the request body so the test can assert that
// the SDK forwards the expected Milvus-parity parameters.
type vecStubGateway struct {
	mu sync.Mutex

	upsertCalls atomic.Int32
	deleteCalls atomic.Int32

	lastUpsertBody atomic.Pointer[map[string]any]
	lastDeleteBody atomic.Pointer[map[string]any]
	lastSearchBody atomic.Pointer[map[string]any]
	lastQueryBody  atomic.Pointer[map[string]any]
	lastHybridBody atomic.Pointer[map[string]any]
	lastScrollBody atomic.Pointer[map[string]any]
	lastAnalyzeBody atomic.Pointer[map[string]any]

	// Scroll pages. Each call returns the page at `scrollCursor` and
	// advances the cursor.
	scrollPages []scrollPage
	scrollIdx   int
}

type scrollPage struct {
	points []map[string]any
	next   string
}

func newVecStub() *vecStubGateway {
	return &vecStubGateway{}
}

func (s *vecStubGateway) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "0.1.0"})
	})
	mux.HandleFunc("/v1/collections/", s.handleVector)
	mux.HandleFunc("/v1/admin/analyze", s.handleAnalyze)
	return httptest.NewServer(mux)
}

func decodeBody(r *http.Request) map[string]any {
	defer r.Body.Close()
	buf, _ := io.ReadAll(r.Body)
	out := map[string]any{}
	if len(buf) == 0 {
		return out
	}
	_ = json.Unmarshal(buf, &out)
	return out
}

func (s *vecStubGateway) handleVector(w http.ResponseWriter, r *http.Request) {
	// Routes we care about end with one of these segments. We use a
	// simple suffix match instead of a full router to keep the stub
	// compact.
	path := r.URL.Path
	switch {
	case endsWith(path, "/upsert") && r.Method == http.MethodPost:
		body := decodeBody(r)
		s.lastUpsertBody.Store(&body)
		s.upsertCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"upserted": 2})
	case endsWith(path, "/points") && r.Method == http.MethodDelete:
		body := decodeBody(r)
		s.lastDeleteBody.Store(&body)
		s.deleteCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"deleted": 3})
	case endsWith(path, "/search") && r.Method == http.MethodPost:
		body := decodeBody(r)
		s.lastSearchBody.Store(&body)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": "a", "score": 0.9, "payload": map[string]any{"color": "red"}},
			{"id": "b", "score": 0.8, "payload": map[string]any{"color": "blue"}},
		})
	case endsWith(path, "/hybrid-search") && r.Method == http.MethodPost:
		body := decodeBody(r)
		s.lastHybridBody.Store(&body)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": "h1", "score": 0.95},
			{"id": "h2", "score": 0.91},
		})
	case endsWith(path, "/query") && r.Method == http.MethodPost:
		body := decodeBody(r)
		s.lastQueryBody.Store(&body)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"points": []map[string]any{
				{"id": "q1", "payload": map[string]any{"color": "red"}},
			},
		})
	case endsWith(path, "/scroll") && r.Method == http.MethodPost:
		body := decodeBody(r)
		s.lastScrollBody.Store(&body)
		s.mu.Lock()
		page := scrollPage{}
		if s.scrollIdx < len(s.scrollPages) {
			page = s.scrollPages[s.scrollIdx]
			s.scrollIdx++
		}
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"points":      page.points,
			"next_cursor": page.next,
		})
	default:
		http.NotFound(w, r)
	}
}

func (s *vecStubGateway) handleAnalyze(w http.ResponseWriter, r *http.Request) {
	body := decodeBody(r)
	s.lastAnalyzeBody.Store(&body)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"results": []map[string]any{
			{
				"tokens": []map[string]any{
					{"token": "hello", "start_offset": 0, "end_offset": 5, "position": 0, "hash": 1},
					{"token": "world", "start_offset": 6, "end_offset": 11, "position": 1, "hash": 2},
				},
			},
		},
	})
}

func endsWith(path, suffix string) bool {
	if len(path) < len(suffix) {
		return false
	}
	return path[len(path)-len(suffix):] == suffix
}

func newVecTestClient(t *testing.T, stub *vecStubGateway) (*Client, *httptest.Server) {
	t.Helper()
	srv := stub.server(t)
	cli, err := New(context.Background(), &ClientConfig{Address: srv.URL})
	if err != nil {
		srv.Close()
		t.Fatalf("New client: %v", err)
	}
	return cli, srv
}

// ---- Tests ---------------------------------------------------------------

func TestUpsertWithPartitionForwardsPartition(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	opt := NewColumnBasedInsertOption("docs").
		WithIDs([]string{"a", "b"}).
		WithFloatVectorColumn("vector", 4, [][]float32{
			{1, 0, 0, 0}, {0, 1, 0, 0},
		}).
		WithPartition("hot")
	res, err := cli.Upsert(context.Background(), opt)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if res.Upserted != 2 {
		t.Fatalf("Upsert count: got %d, want 2", res.Upserted)
	}
	body := stub.lastUpsertBody.Load()
	if body == nil {
		t.Fatalf("upsert body not captured")
	}
	if (*body)["partition"] != "hot" {
		t.Fatalf("partition not forwarded: %v", (*body)["partition"])
	}
}

func TestDeleteWithExprAndPartition(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	opt := NewDeleteOption("docs").
		WithExpr("color == 'red'").
		WithPartition("hot")
	res, err := cli.Delete(context.Background(), opt)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if res.Deleted != 3 {
		t.Fatalf("Delete count: got %d, want 3", res.Deleted)
	}
	body := stub.lastDeleteBody.Load()
	if (*body)["filter"] != "color == 'red'" || (*body)["partition"] != "hot" {
		t.Fatalf("delete body missing fields: %v", *body)
	}
}

func TestDeleteWithInt64IDsFormatsIDs(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	opt := NewDeleteOption("docs").WithInt64IDs("id", []int64{1, 2, 3})
	if _, err := cli.Delete(context.Background(), opt); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	body := stub.lastDeleteBody.Load()
	ids, _ := (*body)["ids"].([]any)
	if len(ids) != 3 || ids[0] != "1" {
		t.Fatalf("ids not formatted: %v", (*body)["ids"])
	}
}

func TestSearchForwardsMilvusOptions(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	opt := NewSearchOption("docs", 5, []entity.Vector{entity.FloatVector{1, 0, 0, 0}}).
		WithFilter("color == 'red'").
		WithPartitions("hot", "warm").
		WithConsistencyLevel("Strong").
		WithOffset(10).
		WithGroupByField("color").
		WithGroupSize(2).
		WithAnnParam(map[string]any{"ef": 64}).
		WithSearchParam("metric_type", "L2").
		WithFunctionReranker("cosine_rerank")
	hits, err := cli.Search(context.Background(), opt)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("Search hits: got %d, want 2", len(hits))
	}
	body := stub.lastSearchBody.Load()
	for _, k := range []string{
		"partitions", "consistency_level", "offset", "group_by_field",
		"group_size", "ann_param", "search_params", "function_reranker",
	} {
		if _, ok := (*body)[k]; !ok {
			t.Fatalf("Search missing %s in body: %v", k, *body)
		}
	}
}

func TestHybridSearchSendsRequestsAndReranker(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	dense := entity.FloatVector{1, 0, 0, 0}
	sparse := &entity.SparseVector{Indices: []uint32{1, 5}, Values: []float32{0.5, 0.5}}
	opt := NewHybridSearchOption("docs", 3,
		NewAnnRequest("vector", 10, dense).WithFilter("a > 0"),
		NewAnnRequest("sparse", 10, sparse),
		NewAnnRequest("text", 10, "hello world"),
	).WithReranker(NewWeightedReranker(0.5, 0.3, 0.2)).
		WithPartitions("hot").
		WithOutputFields("color")
	rss, err := cli.HybridSearch(context.Background(), opt)
	if err != nil {
		t.Fatalf("HybridSearch: %v", err)
	}
	if len(rss) != 1 || rss[0].RowCount != 2 {
		t.Fatalf("HybridSearch result: %+v", rss)
	}
	body := stub.lastHybridBody.Load()
	if (*body)["limit"].(float64) != 3 {
		t.Fatalf("limit wrong: %v", (*body)["limit"])
	}
	reqs := (*body)["requests"].([]any)
	if len(reqs) != 3 {
		t.Fatalf("expected 3 AnnRequests, got %d", len(reqs))
	}
	rr, _ := (*body)["reranker"].(map[string]any)
	if rr["kind"] != "weighted" {
		t.Fatalf("expected weighted reranker, got %v", rr)
	}
}

func TestQueryIteratorPagesUntilEOF(t *testing.T) {
	stub := newVecStub()
	stub.scrollPages = []scrollPage{
		{
			points: []map[string]any{
				{"id": "a", "payload": map[string]any{"x": 1}},
				{"id": "b", "payload": map[string]any{"x": 2}},
			},
			next: "b",
		},
		{
			points: []map[string]any{
				{"id": "c", "payload": map[string]any{"x": 3}},
			},
			next: "",
		},
	}
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	it, err := cli.QueryIterator(context.Background(), NewQueryIteratorOption("docs").
		WithBatchSize(2).
		WithFilter("x > 0"))
	if err != nil {
		t.Fatalf("QueryIterator: %v", err)
	}
	defer it.Close()
	collected := 0
	for {
		rs, err := it.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		collected += rs.RowCount
	}
	if collected != 3 {
		t.Fatalf("iterator collected %d points, want 3", collected)
	}
	body := stub.lastScrollBody.Load()
	if (*body)["filter"] != "x > 0" {
		t.Fatalf("scroll body missing filter: %v", *body)
	}
}

func TestRunAnalyzerReturnsTokens(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	res, err := cli.RunAnalyzer(context.Background(), NewRunAnalyzerOption("hello world").
		WithAnalyzerParams(map[string]any{"tokenizer": "standard"}))
	if err != nil {
		t.Fatalf("RunAnalyzer: %v", err)
	}
	if len(res) != 1 || len(res[0].Tokens) != 2 {
		t.Fatalf("RunAnalyzer result: %+v", res)
	}
	if res[0].Tokens[0].Text != "hello" {
		t.Fatalf("first token: %v", res[0].Tokens[0])
	}
}

func TestGetUsesQueryEndpoint(t *testing.T) {
	stub := newVecStub()
	cli, srv := newVecTestClient(t, stub)
	defer srv.Close()
	opt := NewQueryOption("docs").WithStringIDs("id", []string{"q1"}).WithOutputFields("color")
	rs, err := cli.Get(context.Background(), opt)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rs.RowCount != 1 {
		t.Fatalf("Get rowcount: %d", rs.RowCount)
	}
}
