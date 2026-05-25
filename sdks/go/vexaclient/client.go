// Package vexaclient is the official Go client for VexaDb (HTTP/JSON gateway).
//
// The API surface mirrors the Milvus Go SDK v2 (option-builder pattern):
// connect once with [New], then issue method calls with NewXxxOption helpers.
//
//	cli, err := vexaclient.New(ctx, &vexaclient.ClientConfig{
//	    Address:  "http://127.0.0.1:8080",
//	    Username: "root",
//	    Password: "hunter2",
//	})
//	defer cli.Close(ctx)
//
// Authentication priority during [New]:
//  1. APIKey (sent verbatim as `x-api-key` + `Authorization: Bearer`)
//  2. Username + Password (login at /v1/auth/login, the returned token replaces APIKey)
//  3. Anonymous (only works when the gateway has no RBAC configured)
package vexaclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ClientConfig configures a VexaDb client.
//
// Field semantics follow Milvus Go SDK v2 wherever it makes sense. Fields
// marked "populated by New" are zero on input and set by a successful
// [New] call.
type ClientConfig struct {
	// Address is the gateway base URL — REQUIRED.
	// Accepted formats: "host:port", "http://host:port", "https://host:port".
	// A bare "host:port" is interpreted as http://; pass an explicit
	// https:// to enable TLS (see also EnableTLSAuth).
	Address string

	// Username / Password — when both are set and APIKey is empty, New
	// calls POST /v1/auth/login and uses the returned token afterward.
	Username string
	Password string

	// APIKey is an API token (`tokenid:secret`) or a legacy superuser key.
	// Wins over Username/Password when both are provided.
	APIKey string

	// DBName scopes operations to a named database (sent as `x-vexa-db`
	// header). Reserved for future use — currently a single default DB.
	DBName string

	// EnableTLSAuth forces TLS even when Address has no scheme.
	// Automatically true when Address starts with `https://`.
	EnableTLSAuth bool

	// InsecureSkipVerify disables certificate verification (dev/self-signed).
	// Ignored when HTTPClient is set.
	InsecureSkipVerify bool

	// DisableConn skips the post-construction Health/Version probe.
	// Useful in tests or for deferred connections.
	DisableConn bool

	// Timeout is the per-request HTTP timeout (default 60s).
	// Ignored when HTTPClient is set.
	Timeout time.Duration

	// HTTPClient overrides the default *http.Client (advanced).
	// When set, Timeout, EnableTLSAuth, and InsecureSkipVerify are ignored.
	HTTPClient *http.Client

	// UserAgent sets the User-Agent header (default "vexadb-go/<version>").
	UserAgent string

	// RetryRateLimit configures retry-with-backoff for 429/503 responses.
	// `nil` uses sensible defaults.
	RetryRateLimit *RetryRateLimitOption

	// ServerVersion is populated by [New] from `/v1/version`.
	ServerVersion string
}

// RetryRateLimitOption configures automatic retry on rate-limit / transient
// failures.
type RetryRateLimitOption struct {
	// MaxRetry is the maximum retry attempts (default 5).
	MaxRetry uint
	// MaxBackoff caps the per-attempt delay (default 3s).
	MaxBackoff time.Duration
}

// SDKVersion is sent in the default User-Agent and helpful in server logs.
const SDKVersion = "0.1.0"

// Client talks to the VexaDb HTTP/JSON gateway. Safe for concurrent use.
type Client struct {
	cfg        ClientConfig
	baseURL    string
	httpClient *http.Client
	retry      RetryRateLimitOption
	userAgent  string

	mu    sync.RWMutex
	token string // resolved auth token (APIKey or login-issued)

	serverVersion string
}

// New creates a client, optionally logs in, and verifies connectivity.
//
// On success the caller is responsible for [Client.Close] to release idle
// connections.
func New(ctx context.Context, cfg *ClientConfig) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("vexadb: ClientConfig is required")
	}
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, errors.New("vexadb: ClientConfig.Address is required")
	}

	baseURL, tls, err := normalizeAddress(cfg.Address, cfg.EnableTLSAuth)
	if err != nil {
		return nil, err
	}

	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 60 * time.Second
		}
		hc = buildHTTPClient(timeout, tls, cfg.InsecureSkipVerify)
	}

	retry := RetryRateLimitOption{MaxRetry: 5, MaxBackoff: 3 * time.Second}
	if cfg.RetryRateLimit != nil {
		if cfg.RetryRateLimit.MaxRetry > 0 {
			retry.MaxRetry = cfg.RetryRateLimit.MaxRetry
		}
		if cfg.RetryRateLimit.MaxBackoff > 0 {
			retry.MaxBackoff = cfg.RetryRateLimit.MaxBackoff
		}
	}

	ua := cfg.UserAgent
	if ua == "" {
		ua = "vexadb-go/" + SDKVersion
	}

	c := &Client{
		cfg:        *cfg,
		baseURL:    baseURL,
		httpClient: hc,
		retry:      retry,
		userAgent:  ua,
		token:      cfg.APIKey,
	}

	// Username+Password login (only when no APIKey is provided).
	if c.token == "" && cfg.Username != "" && cfg.Password != "" {
		info, err := c.Login(ctx, NewLoginOption(cfg.Username, cfg.Password))
		if err != nil {
			return nil, fmt.Errorf("vexadb: login failed: %w", err)
		}
		c.mu.Lock()
		c.token = info.Token
		c.mu.Unlock()
	}

	if !cfg.DisableConn {
		ver, err := c.GetServerVersion(ctx, NewGetServerVersionOption())
		if err != nil {
			return nil, fmt.Errorf("vexadb: gateway unreachable at %s: %w", baseURL, err)
		}
		c.serverVersion = ver
		c.cfg.ServerVersion = ver
	}

	return c, nil
}

// Close releases idle connections.
//
// The HTTP transport is otherwise lazy; calling Close is optional but
// recommended in long-running services that recreate clients.
func (c *Client) Close(ctx context.Context) error {
	_ = ctx
	if t, ok := c.httpClient.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

// ServerVersion returns the cached version reported by the gateway during
// [New]. Empty when [ClientConfig.DisableConn] was set.
func (c *Client) ServerVersion() string {
	return c.serverVersion
}

// Address returns the normalized gateway base URL.
func (c *Client) Address() string {
	return c.baseURL
}

// SetToken updates the auth token at runtime. Useful after rotating a token
// or when a long-running process refreshes credentials.
func (c *Client) SetToken(token string) {
	c.mu.Lock()
	c.token = token
	c.mu.Unlock()
}

// Token returns the currently configured auth token (legacy key or
// `tokenid:secret`). Returns an empty string when no auth is set.
func (c *Client) Token() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

func (c *Client) auth(req *http.Request) {
	c.mu.RLock()
	tok := c.token
	c.mu.RUnlock()
	if tok == "" {
		return
	}
	req.Header.Set("x-api-key", tok)
	req.Header.Set("Authorization", "Bearer "+tok)
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var raw []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		raw = b
	}

	var lastErr error
	for attempt := uint(0); attempt <= c.retry.MaxRetry; attempt++ {
		var r io.Reader
		if raw != nil {
			r = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, r)
		if err != nil {
			return err
		}
		c.auth(req)
		req.Header.Set("User-Agent", c.userAgent)
		c.mu.RLock()
		dbName := c.cfg.DBName
		c.mu.RUnlock()
		if dbName != "" {
			req.Header.Set("x-vexa-db", dbName)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if !c.shouldRetry(attempt, ctx) {
				return err
			}
			c.sleepBackoff(ctx, attempt)
			continue
		}

		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if !c.shouldRetry(attempt, ctx) {
				return readErr
			}
			c.sleepBackoff(ctx, attempt)
			continue
		}

		// Retry on transient server errors / rate-limit.
		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusGatewayTimeout {
			lastErr = &VexaError{StatusCode: resp.StatusCode, Message: string(data)}
			if !c.shouldRetry(attempt, ctx) {
				return lastErr
			}
			c.sleepBackoff(ctx, attempt)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return &VexaError{StatusCode: resp.StatusCode, Message: string(data)}
		}
		if out == nil || len(data) == 0 || resp.StatusCode == http.StatusNoContent {
			return nil
		}
		return json.Unmarshal(data, out)
	}
	return lastErr
}

func (c *Client) shouldRetry(attempt uint, ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	return attempt < c.retry.MaxRetry
}

func (c *Client) sleepBackoff(ctx context.Context, attempt uint) {
	// Exponential backoff with jitter, capped at MaxBackoff.
	base := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
	if base > c.retry.MaxBackoff {
		base = c.retry.MaxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(base / 4))) // up to 25% jitter
	d := base + jitter
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Health returns gateway health metadata (unauthenticated probe).
func (c *Client) Health(ctx context.Context) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodGet, "/health", nil, &out)
	return out, err
}

// Live is the liveness probe.
func (c *Client) Live(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/live", nil)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// Ready returns true when the gateway reports the cluster is serving.
func (c *Client) Ready(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/ready", nil)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// ---- helpers ----

func normalizeAddress(addr string, forceTLS bool) (baseURL string, tlsEnabled bool, err error) {
	a := strings.TrimSpace(addr)
	if !strings.Contains(a, "://") {
		scheme := "http"
		if forceTLS {
			scheme = "https"
		}
		a = scheme + "://" + a
	}
	u, perr := url.Parse(a)
	if perr != nil {
		return "", false, fmt.Errorf("vexadb: invalid Address %q: %w", addr, perr)
	}
	if u.Host == "" {
		return "", false, fmt.Errorf("vexadb: Address %q is missing host", addr)
	}
	switch u.Scheme {
	case "http":
		tlsEnabled = forceTLS
	case "https":
		tlsEnabled = true
	default:
		return "", false, fmt.Errorf("vexadb: unsupported scheme %q (use http/https)", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return strings.TrimRight(u.String(), "/"), tlsEnabled, nil
}

func buildHTTPClient(timeout time.Duration, tlsEnabled, insecureSkipVerify bool) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	if tlsEnabled || insecureSkipVerify {
		t.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify} //nolint:gosec
	}
	return &http.Client{Timeout: timeout, Transport: t}
}
