package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

type embedRequest struct {
	Text     string `json:"text"`
	Provider string `json:"provider"` // "openai" (LM Studio, OpenAI, etc.) or "ollama"
	BaseURL  string `json:"base_url"`
	Model    string `json:"model"`
	APIKey   string `json:"api_key"`
}

type embedResponse struct {
	Embedding  []float64 `json:"embedding"`
	Dimensions int       `json:"dimensions"`
	Model      string    `json:"model"`
	Provider   string    `json:"provider"`
}

func handleEmbed(c echo.Context) error {
	var req embedRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
	}
	req.Text = strings.TrimSpace(req.Text)
	if req.Text == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "text is required"})
	}
	if req.Provider == "" {
		req.Provider = "openai"
	}
	if req.Model == "" {
		req.Model = "text-embedding-nomic-embed-text-v1.5"
	}
	if req.BaseURL == "" {
		req.BaseURL = "http://127.0.0.1:1234/v1"
	}

	var (
		vec []float64
		err error
	)
	switch strings.ToLower(req.Provider) {
	case "ollama":
		vec, err = embedOllama(req.BaseURL, req.Model, req.Text)
	default:
		vec, err = embedOpenAI(req.BaseURL, req.Model, req.APIKey, req.Text)
	}
	if err != nil {
		return c.JSON(http.StatusBadGateway, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, embedResponse{
		Embedding:  vec,
		Dimensions: len(vec),
		Model:      req.Model,
		Provider:   req.Provider,
	})
}

func embedOpenAI(baseURL, model, apiKey, text string) ([]float64, error) {
	base := strings.TrimRight(baseURL, "/")
	url := base + "/embeddings"
	body, _ := json.Marshal(map[string]any{
		"model": model,
		"input": text,
	})
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("embedding server unreachable at %s: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("embeddings API %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var parsed struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Embedding []float64 `json:"embedding"` // some servers flatten this
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse embeddings response: %w", err)
	}
	if len(parsed.Data) > 0 && len(parsed.Data[0].Embedding) > 0 {
		return parsed.Data[0].Embedding, nil
	}
	if len(parsed.Embedding) > 0 {
		return parsed.Embedding, nil
	}
	return nil, fmt.Errorf("empty embedding in response from %s", url)
}

func embedOllama(baseURL, model, text string) ([]float64, error) {
	base := strings.TrimRight(baseURL, "/")
	url := base + "/api/embeddings"
	body, _ := json.Marshal(map[string]string{
		"model":  model,
		"prompt": text,
	})
	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("ollama unreachable at %s: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ollama %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var parsed struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse ollama response: %w", err)
	}
	if len(parsed.Embedding) == 0 {
		return nil, fmt.Errorf("empty embedding from ollama")
	}
	return parsed.Embedding, nil
}
