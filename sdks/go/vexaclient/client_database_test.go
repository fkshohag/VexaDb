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
)

// dbStubGateway is an in-memory implementation of the gateway's
// `/v1/databases/*` surface, used to exercise the SDK without spinning
// up the real Rust binary.
type dbStubGateway struct {
	mu  sync.Mutex
	dbs map[string]map[string]string // name -> properties
	// observed counters / queries
	listCalls      atomic.Int32
	createCalls    atomic.Int32
	dropCalls      atomic.Int32
	lastDropForce  atomic.Bool
	lastDropName   atomic.Pointer[string]
	lastAlterSet   atomic.Pointer[map[string]string]
	lastAlterUnset atomic.Pointer[[]string]
}

func newDBStub() *dbStubGateway {
	return &dbStubGateway{dbs: map[string]map[string]string{
		"default": {},
	}}
}

func (s *dbStubGateway) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"version": "0.1.0"})
	})

	// List + create
	mux.HandleFunc("/v1/databases", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.listCalls.Add(1)
			s.mu.Lock()
			out := make([]string, 0, len(s.dbs))
			for k := range s.dbs {
				out = append(out, k)
			}
			s.mu.Unlock()
			_ = json.NewEncoder(w).Encode(out)
		case http.MethodPost:
			s.createCalls.Add(1)
			var body struct {
				Name       string            `json:"name"`
				Properties map[string]string `json:"properties"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if body.Name == "" {
				http.Error(w, "name required", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			if _, ok := s.dbs[body.Name]; ok {
				s.mu.Unlock()
				http.Error(w, "exists", http.StatusConflict)
				return
			}
			if body.Properties == nil {
				body.Properties = map[string]string{}
			}
			s.dbs[body.Name] = body.Properties
			s.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	})

	// Per-database: describe / drop
	mux.HandleFunc("/v1/databases/", func(w http.ResponseWriter, r *http.Request) {
		// Path is /v1/databases/<name>[/properties]
		rest := strings.TrimPrefix(r.URL.Path, "/v1/databases/")
		name := rest
		isProps := false
		if i := strings.Index(rest, "/"); i >= 0 {
			name = rest[:i]
			tail := rest[i+1:]
			if tail == "properties" {
				isProps = true
			} else {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
		}

		switch {
		case !isProps && r.Method == http.MethodGet:
			s.mu.Lock()
			props, ok := s.dbs[name]
			s.mu.Unlock()
			if !ok {
				http.Error(w, "database not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":          name,
				"properties":    props,
				"created_at_ms": 1234,
			})

		case !isProps && r.Method == http.MethodDelete:
			s.dropCalls.Add(1)
			force := r.URL.Query().Get("force") == "true"
			s.lastDropForce.Store(force)
			s.lastDropName.Store(&name)
			s.mu.Lock()
			defer s.mu.Unlock()
			if _, ok := s.dbs[name]; !ok {
				http.Error(w, "database not found", http.StatusNotFound)
				return
			}
			delete(s.dbs, name)
			w.WriteHeader(http.StatusNoContent)

		case isProps && r.Method == http.MethodPatch:
			var body struct {
				Set   map[string]string `json:"set"`
				Unset []string          `json:"unset"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			set := body.Set
			s.lastAlterSet.Store(&set)
			unset := body.Unset
			s.lastAlterUnset.Store(&unset)
			s.mu.Lock()
			defer s.mu.Unlock()
			props, ok := s.dbs[name]
			if !ok {
				http.Error(w, "database not found", http.StatusNotFound)
				return
			}
			for k, v := range body.Set {
				props[k] = v
			}
			for _, k := range body.Unset {
				delete(props, k)
			}
			s.dbs[name] = props
			w.WriteHeader(http.StatusNoContent)

		case isProps && r.Method == http.MethodDelete:
			data, _ := io.ReadAll(r.Body)
			var body struct {
				Keys []string `json:"keys"`
			}
			if len(data) > 0 {
				if err := json.Unmarshal(data, &body); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
			}
			unset := body.Keys
			s.lastAlterUnset.Store(&unset)
			s.mu.Lock()
			defer s.mu.Unlock()
			props, ok := s.dbs[name]
			if !ok {
				http.Error(w, "database not found", http.StatusNotFound)
				return
			}
			for _, k := range body.Keys {
				delete(props, k)
			}
			s.dbs[name] = props
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func newDBClient(t *testing.T, srvURL string) *Client {
	t.Helper()
	c, err := New(context.Background(), &ClientConfig{Address: srvURL, APIKey: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestCreateAndListDatabase(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	if err := c.CreateDatabase(ctx,
		NewCreateDatabaseOption("analytics").
			WithProperty("database.replica.number", 2),
	); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if s.createCalls.Load() != 1 {
		t.Fatalf("createCalls=%d, want 1", s.createCalls.Load())
	}

	names, err := c.ListDatabase(ctx, NewListDatabaseOption())
	if err != nil {
		t.Fatalf("ListDatabase: %v", err)
	}
	if !containsString(names, "analytics") || !containsString(names, "default") {
		t.Fatalf("unexpected list: %v", names)
	}
}

func TestCreateDatabase_RequiresName(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	if err := c.CreateDatabase(context.Background(), NewCreateDatabaseOption("")); err == nil {
		t.Fatalf("expected error for empty name")
	}
}

func TestDescribeDatabase(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	if err := c.CreateDatabase(ctx,
		NewCreateDatabaseOption("billing").
			WithProperty("tier", "hot"),
	); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	db, err := c.DescribeDatabase(ctx, NewDescribeDatabaseOption("billing"))
	if err != nil {
		t.Fatalf("DescribeDatabase: %v", err)
	}
	if db.Name != "billing" || db.Property("tier") != "hot" {
		t.Fatalf("unexpected db: %+v", db)
	}
}

func TestDescribeDatabase_NotFound(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	_, err := c.DescribeDatabase(context.Background(), NewDescribeDatabaseOption("missing"))
	if err == nil {
		t.Fatalf("expected NotFound error")
	}
	ve, ok := err.(*VexaError)
	if !ok || ve.StatusCode != http.StatusNotFound {
		t.Fatalf("expected VexaError 404, got %v", err)
	}
}

func TestDropDatabase_PassesForce(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	_ = c.CreateDatabase(ctx, NewCreateDatabaseOption("temp"))

	if err := c.DropDatabase(ctx, NewDropDatabaseOption("temp").WithForce(true)); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if !s.lastDropForce.Load() {
		t.Fatalf("force not propagated")
	}
	if n := s.lastDropName.Load(); n == nil || *n != "temp" {
		t.Fatalf("dropped name = %v, want temp", n)
	}
}

func TestAlterDatabaseProperties(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	_ = c.CreateDatabase(ctx, NewCreateDatabaseOption("billing"))

	if err := c.AlterDatabaseProperties(ctx,
		NewAlterDatabasePropertiesOption("billing").
			WithProperty("ttl", "30d").
			WithProperty("tier", "hot"),
	); err != nil {
		t.Fatalf("AlterDatabaseProperties: %v", err)
	}
	got := s.lastAlterSet.Load()
	if got == nil {
		t.Fatalf("alter set not captured")
	}
	if (*got)["ttl"] != "30d" || (*got)["tier"] != "hot" {
		t.Fatalf("alter set unexpected: %+v", *got)
	}

	db, err := c.DescribeDatabase(ctx, NewDescribeDatabaseOption("billing"))
	if err != nil {
		t.Fatalf("DescribeDatabase: %v", err)
	}
	if db.Property("ttl") != "30d" || db.Property("tier") != "hot" {
		t.Fatalf("describe missing props: %+v", db.Properties)
	}
}

func TestDropDatabaseProperties(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	_ = c.CreateDatabase(ctx, NewCreateDatabaseOption("billing"))
	_ = c.AlterDatabaseProperties(context.Background(),
		NewAlterDatabasePropertiesOption("billing").
			WithProperty("ttl", "30d").
			WithProperty("tier", "hot"),
	)
	if err := c.DropDatabaseProperties(ctx,
		NewDropDatabasePropertiesOption("billing", "tier"),
	); err != nil {
		t.Fatalf("DropDatabaseProperties: %v", err)
	}

	got := s.lastAlterUnset.Load()
	if got == nil || len(*got) != 1 || (*got)[0] != "tier" {
		t.Fatalf("unset not propagated: %+v", got)
	}
}

func TestDropDatabaseProperties_EmptyKeysNoOp(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	// Should succeed without ever hitting the server.
	if err := c.DropDatabaseProperties(context.Background(),
		NewDropDatabasePropertiesOption("billing"),
	); err != nil {
		t.Fatalf("DropDatabaseProperties no-op should succeed: %v", err)
	}
	if got := s.lastAlterUnset.Load(); got != nil {
		t.Fatalf("expected no server call, got %+v", got)
	}
}

func TestUseDatabase_AliasOfUsingDatabase(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	ctx := context.Background()
	if err := c.UseDatabase(ctx, NewUseDatabaseOption("analytics")); err != nil {
		t.Fatalf("UseDatabase: %v", err)
	}
	if c.DBName() != "analytics" {
		t.Fatalf("DBName = %q, want analytics", c.DBName())
	}
	// Passing nil should be a no-op (Milvus parity).
	if err := c.UseDatabase(ctx, nil); err != nil {
		t.Fatalf("UseDatabase(nil): %v", err)
	}
	if c.DBName() != "analytics" {
		t.Fatalf("DBName clobbered by nil opt: %q", c.DBName())
	}
}

func TestDescribeDatabase_RequiresName(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	if _, err := c.DescribeDatabase(context.Background(), NewDescribeDatabaseOption("")); err == nil {
		t.Fatalf("expected error for empty name")
	}
}

func TestAlterDatabaseProperties_RequiresName(t *testing.T) {
	s := newDBStub()
	srv := s.server(t)
	defer srv.Close()
	c := newDBClient(t, srv.URL)

	if err := c.AlterDatabaseProperties(context.Background(),
		NewAlterDatabasePropertiesOption("")); err == nil {
		t.Fatalf("expected error for empty name")
	}
}

func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
