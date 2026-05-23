// VectorDB admin panel backend.
//
// - Reverse-proxies /api/* to the VectorDB REST gateway (default :8080).
// - Forwards (or injects) the API key configured at startup.
// - Serves the React build from ./frontend/dist with SPA fallback.
//
// Flags / env:
//   --listen   ($ADMIN_LISTEN)        bind address                (default :8090)
//   --upstream ($VECTORDB_URL)        gateway URL                 (default http://127.0.0.1:8080)
//   --static   ($ADMIN_STATIC)        path to React build         (default ../frontend/dist)
//   --api-key  ($VECTORDB_API_KEY)    default API key forwarded   (optional)
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	listen := flag.String("listen", env("ADMIN_LISTEN", ":8090"), "bind address")
	upstream := flag.String("upstream", env("VECTORDB_URL", "http://127.0.0.1:8080"), "VectorDB gateway URL")
	staticDir := flag.String("static", env("ADMIN_STATIC", "../frontend/dist"), "Path to React build (set empty to disable)")
	apiKey := flag.String("api-key", os.Getenv("VECTORDB_API_KEY"), "Default API key to forward (optional)")
	flag.Parse()

	upstreamURL, err := url.Parse(*upstream)
	if err != nil {
		log.Fatalf("invalid upstream URL %q: %v", *upstream, err)
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	// Permissive CORS so the React dev server (vite on :5173) can call /api directly.
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{"*"},
	}))

	proxy := httputil.NewSingleHostReverseProxy(upstreamURL)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api")
		if req.URL.Path == "" {
			req.URL.Path = "/"
		}
		req.Host = upstreamURL.Host
		req.Header.Set("X-Forwarded-Host", req.Header.Get("Host"))
		// Inject the configured API key only if the caller didn't supply one.
		if *apiKey != "" && req.Header.Get("x-api-key") == "" && req.Header.Get("Authorization") == "" {
			req.Header.Set("x-api-key", *apiKey)
			req.Header.Set("Authorization", "Bearer "+*apiKey)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy error: %s %s -> %s: %v", r.Method, r.URL.Path, upstreamURL, err)
		http.Error(w, fmt.Sprintf(`{"error":"upstream unreachable: %s"}`, err), http.StatusBadGateway)
	}

	e.Any("/api", echo.WrapHandler(proxy))
	e.Any("/api/*", echo.WrapHandler(proxy))

	// Tiny config endpoint the frontend reads on boot.
	e.GET("/config.json", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{
			"upstream":   *upstream,
			"hasApiKey":  *apiKey != "",
			"serverTime": time.Now().Format(time.RFC3339),
		})
	})

	// Static React build with SPA fallback. Order matters: more-specific routes
	// (assets, root file) are registered first so the catch-all only handles
	// unknown paths (client-side routing) and falls back to index.html.
	if *staticDir != "" {
		abs, _ := filepath.Abs(*staticDir)
		if info, err := os.Stat(abs); err == nil && info.IsDir() {
			indexFile := filepath.Join(abs, "index.html")
			e.Static("/assets", filepath.Join(abs, "assets"))
			e.File("/", indexFile)
			e.File("/favicon.ico", filepath.Join(abs, "favicon.ico"))
			e.File("/favicon.svg", filepath.Join(abs, "favicon.svg"))
			// Catch-all for client-side routes (anything not /api, /config.json,
			// /assets, or an explicit file falls through to here).
			e.GET("/*", func(c echo.Context) error {
				return c.File(indexFile)
			})
			log.Printf("serving static files from %s", abs)
		} else {
			log.Printf("static dir %q not found; serving placeholder page", abs)
			e.GET("/", func(c echo.Context) error {
				return c.HTML(http.StatusOK, devPage(*upstream))
			})
		}
	}

	log.Printf("vectordb-admin listening on %s, proxying /api -> %s", *listen, *upstream)
	if err := e.Start(*listen); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func devPage(upstream string) string {
	return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>VectorDB Admin — backend running</title>
<style>
body{font:14px/1.5 ui-monospace,Menlo,monospace;max-width:680px;margin:6em auto;padding:1.5em;
     background:#0a0e1a;color:#c9d1d9;border:1px solid #1f2937;border-radius:12px}
h1{color:#06b6d4;margin-top:0}
code,pre{background:#131929;padding:.15em .35em;border-radius:4px}
pre{padding:1em;overflow:auto}
a{color:#8b5cf6}
</style>
</head>
<body>
<h1>VectorDB Admin — backend running</h1>
<p>The React frontend hasn't been built yet. From <code>admin/frontend/</code> run:</p>
<pre>npm install
npm run dev      # dev server with HMR (separate Vite on :5173)
# or
npm run build    # writes ./dist which this Go binary will then serve</pre>
<p>Proxy is live: <a href="/api/health">/api/health</a> &middot; upstream: <code>` + upstream + `</code></p>
</body>
</html>`
}
