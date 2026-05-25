// Package vexaclient is the official Go client for VexaDb (REST gateway).
//
// API shape follows the Milvus Go SDK v2 option-builder pattern; see
// sdks/go/milvus-sdk-go/ for reference docs only (not a dependency).
package vexaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ClientConfig configures a VexaDb client.
type ClientConfig struct {
	// Address is the gateway base URL (e.g. http://127.0.0.1:8080).
	Address string
	// APIKey is sent as x-api-key and Authorization: Bearer (optional).
	APIKey string
	// Timeout for each HTTP request (default 60s).
	Timeout time.Duration
}

// Client talks to the VexaDb HTTP/JSON gateway.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// New creates a client and verifies the gateway is reachable.
func New(ctx context.Context, cfg *ClientConfig) (*Client, error) {
	if cfg == nil || strings.TrimSpace(cfg.Address) == "" {
		return nil, fmt.Errorf("vexadb: Address is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	c := &Client{
		baseURL: strings.TrimRight(cfg.Address, "/"),
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
	if _, err := c.Health(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Close releases resources (HTTP client is shared; no-op for REST).
func (c *Client) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func (c *Client) auth(req *http.Request) {
	if c.apiKey == "" {
		return
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, r)
	if err != nil {
		return err
	}
	c.auth(req)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &VexaError{StatusCode: resp.StatusCode, Message: string(data)}
	}
	if out == nil || len(data) == 0 || resp.StatusCode == 204 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// Health returns gateway health metadata.
func (c *Client) Health(ctx context.Context) (map[string]string, error) {
	var out map[string]string
	err := c.do(ctx, http.MethodGet, "/health", nil, &out)
	return out, err
}

func (c *Client) Live(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/live", nil)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

func (c *Client) Ready(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/ready", nil)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}
