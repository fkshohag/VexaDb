package vexaclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubGateway is a minimal /v1/version + /v1/auth/login + /health handler.
type stubGateway struct {
	versionCalls atomic.Int32
	loginCalls   atomic.Int32
	lastDB       atomic.Pointer[string]
	lastAuth     atomic.Pointer[string]
	transient    atomic.Int32 // # 503s to emit before serving /v1/version
}

func (s *stubGateway) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		s.loginCalls.Add(1)
		_, _ = w.Write([]byte(`{"token_id":"tid","token":"tid:secret","user":"alice"}`))
	})
	mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) {
		s.versionCalls.Add(1)
		if v := s.transient.Load(); v > 0 {
			s.transient.Add(-1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		db := r.Header.Get("x-vexa-db")
		s.lastDB.Store(&db)
		auth := r.Header.Get("authorization")
		s.lastAuth.Store(&auth)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"version":    "0.1.0",
			"server":     "vexadb-test",
			"git_commit": "deadbeef",
		})
	})
	return httptest.NewServer(mux)
}

func TestNew_WithAPIKey_PopulatesServerVersion(t *testing.T) {
	g := &stubGateway{}
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{Address: srv.URL, APIKey: "legacy-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(ctx)

	if c.ServerVersion() != "0.1.0" {
		t.Fatalf("ServerVersion = %q, want 0.1.0", c.ServerVersion())
	}
	if g.loginCalls.Load() != 0 {
		t.Fatalf("login should not be called when APIKey is set")
	}
	if g.versionCalls.Load() == 0 {
		t.Fatalf("expected /v1/version probe")
	}
	if auth := g.lastAuth.Load(); auth == nil || *auth != "Bearer legacy-key" {
		t.Fatalf("auth header = %v, want Bearer legacy-key", auth)
	}
}

func TestNew_WithUsernamePassword_LogsIn(t *testing.T) {
	g := &stubGateway{}
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{Address: srv.URL, Username: "alice", Password: "hunter2"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(ctx)

	if g.loginCalls.Load() != 1 {
		t.Fatalf("login calls = %d, want 1", g.loginCalls.Load())
	}
	if c.Token() != "tid:secret" {
		t.Fatalf("Token = %q, want tid:secret", c.Token())
	}
	if auth := g.lastAuth.Load(); auth == nil || *auth != "Bearer tid:secret" {
		t.Fatalf("auth header = %v, want Bearer tid:secret", auth)
	}
}

func TestNew_DisableConn_SkipsProbe(t *testing.T) {
	g := &stubGateway{}
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{Address: srv.URL, APIKey: "k", DisableConn: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(ctx)
	if g.versionCalls.Load() != 0 {
		t.Fatalf("expected no version probe, got %d", g.versionCalls.Load())
	}
	if c.ServerVersion() != "" {
		t.Fatalf("ServerVersion should be empty when DisableConn=true")
	}
}

func TestRetry_OnTransient503(t *testing.T) {
	g := &stubGateway{}
	g.transient.Store(2) // first two attempts fail, third succeeds
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{
		Address: srv.URL,
		APIKey:  "k",
		RetryRateLimit: &RetryRateLimitOption{
			MaxRetry:   3,
			MaxBackoff: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("New (with retries): %v", err)
	}
	defer c.Close(ctx)
	if g.versionCalls.Load() != 3 {
		t.Fatalf("version calls = %d, want 3", g.versionCalls.Load())
	}
}

func TestUsingDatabase_HeaderSet(t *testing.T) {
	g := &stubGateway{}
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{Address: srv.URL, APIKey: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(ctx)

	if err := c.UsingDatabase(ctx, NewUsingDatabaseOption("analytics")); err != nil {
		t.Fatalf("UsingDatabase: %v", err)
	}
	if _, err := c.GetServerVersion(ctx, NewGetServerVersionOption()); err != nil {
		t.Fatalf("GetServerVersion: %v", err)
	}
	if db := g.lastDB.Load(); db == nil || *db != "analytics" {
		t.Fatalf("x-vexa-db = %v, want analytics", db)
	}
}

func TestServerInfo_ReturnsFullBlock(t *testing.T) {
	g := &stubGateway{}
	srv := g.server(t)
	defer srv.Close()

	ctx := context.Background()
	c, err := New(ctx, &ClientConfig{Address: srv.URL, APIKey: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close(ctx)

	info, err := c.ServerInfo(ctx, NewServerInfoOption())
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if info.Version != "0.1.0" || info.GitCommit != "deadbeef" || info.Server != "vexadb-test" {
		t.Fatalf("ServerInfo unexpected: %+v", info)
	}
}

func TestNormalizeAddress(t *testing.T) {
	cases := []struct {
		in       string
		forceTLS bool
		want     string
		tls      bool
		wantErr  bool
	}{
		{"localhost:8080", false, "http://localhost:8080", false, false},
		{"localhost:8080", true, "https://localhost:8080", true, false},
		{"http://h:1", false, "http://h:1", false, false},
		{"https://h:1", false, "https://h:1", true, false},
		{"https://h:1/", false, "https://h:1", true, false},
		{"ftp://x", false, "", false, true},
		{"   ", false, "", false, true},
	}
	for _, tc := range cases {
		got, tls, err := normalizeAddress(tc.in, tc.forceTLS)
		if tc.wantErr {
			if err == nil {
				t.Errorf("normalize(%q): expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("normalize(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want || tls != tc.tls {
			t.Errorf("normalize(%q, force=%v) = (%q,%v), want (%q,%v)",
				tc.in, tc.forceTLS, got, tls, tc.want, tc.tls)
		}
	}
}

func TestNew_RequiresAddress(t *testing.T) {
	_, err := New(context.Background(), &ClientConfig{})
	if err == nil || !strings.Contains(err.Error(), "Address") {
		t.Fatalf("expected Address-required error, got %v", err)
	}
}
