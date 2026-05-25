package vexaclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// fakeBackend stubs the meta endpoints exercised by collection mgmt tests.
type fakeBackend struct {
	mu          sync.Mutex
	collections map[string]map[string]any
	aliases     map[string]string
	lastBody    json.RawMessage
	lastPath    string
	lastMethod  string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		collections: map[string]map[string]any{
			"books": {
				"name":         "books",
				"dimension":    128,
				"metric":       "cosine",
				"vector_count": uint64(0),
			},
		},
		aliases: map[string]string{},
	}
}

func (f *fakeBackend) record(r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.lastBody = b
	f.lastPath = r.URL.Path
	f.lastMethod = r.Method
}

func (f *fakeBackend) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":"0.1.0"}`))
	})
	mux.HandleFunc("/v1/collections", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.record(r)
		switch r.Method {
		case http.MethodGet:
			names := make([]string, 0, len(f.collections))
			for n := range f.collections {
				names = append(names, n)
			}
			_ = json.NewEncoder(w).Encode(names)
		case http.MethodPost:
			var body map[string]any
			_ = json.Unmarshal(f.lastBody, &body)
			name, _ := body["name"].(string)
			f.collections[name] = body
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/collections/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.record(r)
		path := strings.TrimPrefix(r.URL.Path, "/v1/collections/")
		parts := strings.SplitN(path, "/", 2)
		name := parts[0]
		// resolve alias
		if target, ok := f.aliases[name]; ok {
			name = target
		}
		coll, exists := f.collections[name]
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				if !exists {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				// add aliases pointing at this collection
				aliases := []string{}
				for a, target := range f.aliases {
					if target == name {
						aliases = append(aliases, a)
					}
				}
				resp := map[string]any{}
				for k, v := range coll {
					resp[k] = v
				}
				resp["aliases"] = aliases
				_ = json.NewEncoder(w).Encode(resp)
			case http.MethodDelete:
				delete(f.collections, name)
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}
		sub := parts[1]
		switch sub {
		case "stats":
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         name,
				"vector_count": uint64(7),
				"dimension":    uint32(128),
				"metric":       "cosine",
			})
		case "rename":
			var body struct {
				NewName string `json:"new_name"`
			}
			_ = json.Unmarshal(f.lastBody, &body)
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.collections[body.NewName] = coll
			f.collections[body.NewName]["name"] = body.NewName
			delete(f.collections, name)
			for a, target := range f.aliases {
				if target == name {
					f.aliases[a] = body.NewName
				}
			}
			w.WriteHeader(http.StatusNoContent)
		case "properties":
			var body struct {
				Set   map[string]string `json:"set"`
				Unset []string          `json:"unset"`
			}
			_ = json.Unmarshal(f.lastBody, &body)
			if !exists {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			props, _ := coll["properties"].(map[string]any)
			if props == nil {
				props = map[string]any{}
			}
			for k, v := range body.Set {
				props[k] = v
			}
			for _, k := range body.Unset {
				delete(props, k)
			}
			coll["properties"] = props
			w.WriteHeader(http.StatusNoContent)
		case "aliases":
			out := []string{}
			for a, target := range f.aliases {
				if target == name {
					out = append(out, a)
				}
			}
			_ = json.NewEncoder(w).Encode(out)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/v1/aliases", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.record(r)
		switch r.Method {
		case http.MethodGet:
			rows := []entity.Alias{}
			for a, target := range f.aliases {
				rows = append(rows, entity.Alias{Alias: a, Collection: target})
			}
			_ = json.NewEncoder(w).Encode(rows)
		case http.MethodPost:
			var body struct {
				Alias      string `json:"alias"`
				Collection string `json:"collection"`
			}
			_ = json.Unmarshal(f.lastBody, &body)
			if _, ok := f.collections[body.Collection]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if _, exists := f.aliases[body.Alias]; exists {
				w.WriteHeader(http.StatusConflict)
				return
			}
			f.aliases[body.Alias] = body.Collection
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/v1/aliases/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.record(r)
		alias := strings.TrimPrefix(r.URL.Path, "/v1/aliases/")
		switch r.Method {
		case http.MethodGet:
			target, ok := f.aliases[alias]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(entity.Alias{Alias: alias, Collection: target})
		case http.MethodPut:
			var body struct {
				Collection string `json:"collection"`
			}
			_ = json.Unmarshal(f.lastBody, &body)
			if _, ok := f.aliases[alias]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.aliases[alias] = body.Collection
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(f.aliases, alias)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return httptest.NewServer(mux)
}

func mustNewClient(t *testing.T, url string) *Client {
	t.Helper()
	c, err := New(context.Background(), &ClientConfig{Address: url, APIKey: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestCreateAndDescribeCollection_WithProperties(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(64)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(32))

	opt := NewCreateCollectionOption("notes", schema).
		WithMetricType(entity.COSINE).
		WithProperty("collection.ttl.seconds", 3600).
		WithConsistencyLevel(entity.ClStrong).
		WithAutoID(false)

	if err := cli.CreateCollection(context.Background(), opt); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// verify props sent
	var sent map[string]any
	_ = json.Unmarshal(fb.lastBody, &sent)
	props, _ := sent["properties"].(map[string]any)
	if props == nil || props["collection.ttl.seconds"] != "3600" || props["consistency_level"] != "Strong" {
		t.Fatalf("expected properties to include TTL + consistency, got %v", sent)
	}
}

func TestHasCollection_BothOptionAndString(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())

	got, err := cli.HasCollection(context.Background(), NewHasCollectionOption("books"))
	if err != nil || !got {
		t.Fatalf("HasCollection(option): got=%v err=%v", got, err)
	}
	got, err = cli.HasCollection(context.Background(), "books")
	if err != nil || !got {
		t.Fatalf("HasCollection(string): got=%v err=%v", got, err)
	}
	got, err = cli.HasCollection(context.Background(), "missing")
	if err != nil || got {
		t.Fatalf("HasCollection(missing): expected false, got=%v err=%v", got, err)
	}
}

func TestDescribeCollection_ReturnsEntity(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())

	c, err := cli.DescribeCollection(context.Background(), NewDescribeCollectionOption("books"))
	if err != nil {
		t.Fatalf("DescribeCollection: %v", err)
	}
	if c.Name != "books" || c.Dimension != 128 {
		t.Fatalf("unexpected describe result: %+v", c)
	}
}

func TestRenameAndAliasRoundtrip(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())
	ctx := context.Background()

	// Create an alias on "books".
	if err := cli.CreateAlias(ctx, NewCreateAliasOption("books", "library")); err != nil {
		t.Fatalf("CreateAlias: %v", err)
	}
	aliases, err := cli.ListAliases(ctx, NewListAliasesOption("books"))
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if len(aliases) != 1 || aliases[0] != "library" {
		t.Fatalf("aliases for books = %v, want [library]", aliases)
	}

	// Resolve the alias.
	a, err := cli.DescribeAlias(ctx, NewDescribeAliasOption("library"))
	if err != nil {
		t.Fatalf("DescribeAlias: %v", err)
	}
	if a.Collection != "books" {
		t.Fatalf("alias library -> %q, want books", a.Collection)
	}

	// Rename and verify the alias follows the rename.
	if err := cli.RenameCollection(ctx, NewRenameCollectionOption("books", "tomes")); err != nil {
		t.Fatalf("RenameCollection: %v", err)
	}
	a, _ = cli.DescribeAlias(ctx, NewDescribeAliasOption("library"))
	if a.Collection != "tomes" {
		t.Fatalf("alias library -> %q after rename, want tomes", a.Collection)
	}

	// Reassign + drop.
	if err := cli.AlterAlias(ctx, NewAlterAliasOption("library", "tomes")); err != nil {
		t.Fatalf("AlterAlias: %v", err)
	}
	if err := cli.DropAlias(ctx, NewDropAliasOption("library")); err != nil {
		t.Fatalf("DropAlias: %v", err)
	}
}

func TestAlterAndDropProperties(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())
	ctx := context.Background()

	if err := cli.AlterCollectionProperties(ctx,
		NewAlterCollectionPropertiesOption("books").
			WithProperty("mmap.enabled", true).
			WithProperty("collection.ttl.seconds", 60),
	); err != nil {
		t.Fatalf("AlterCollectionProperties: %v", err)
	}
	if err := cli.DropCollectionProperties(ctx,
		NewDropCollectionPropertiesOption("books", "mmap.enabled"),
	); err != nil {
		t.Fatalf("DropCollectionProperties: %v", err)
	}
	// Last call recorded should be PATCH with unset=[mmap.enabled].
	if !strings.HasSuffix(fb.lastPath, "/properties") || fb.lastMethod != http.MethodPatch {
		t.Fatalf("expected PATCH .../properties, got %s %s", fb.lastMethod, fb.lastPath)
	}
}

func TestAddCollectionField_RejectsVectorAndPK(t *testing.T) {
	fb := newFakeBackend()
	srv := fb.server(t)
	defer srv.Close()
	cli := mustNewClient(t, srv.URL)
	defer cli.Close(context.Background())

	vecField := entity.NewField().WithName("v2").WithDataType(entity.FieldTypeFloatVector).WithDim(4)
	if err := cli.AddCollectionField(context.Background(), NewAddCollectionFieldOption("books", vecField)); err == nil {
		t.Fatal("expected vector-field rejection")
	}
	pkField := entity.NewField().WithName("id2").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	if err := cli.AddCollectionField(context.Background(), NewAddCollectionFieldOption("books", pkField)); err == nil {
		t.Fatal("expected PK rejection")
	}
	notNull := entity.NewField().WithName("x").WithDataType(entity.FieldTypeInt64)
	if err := cli.AddCollectionField(context.Background(), NewAddCollectionFieldOption("books", notNull)); err == nil {
		t.Fatal("expected nullable rejection")
	}
	ok := entity.NewField().WithName("notes").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256).WithNullable(true)
	if err := cli.AddCollectionField(context.Background(), NewAddCollectionFieldOption("books", ok)); err != nil {
		t.Fatalf("AddCollectionField (nullable scalar): %v", err)
	}
}
