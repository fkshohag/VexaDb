// Load tester for VectorDB using the Go SDK.
//
// Three phases run sequentially against a single collection:
//
//   1. SINGLE upsert  — many concurrent workers, small batches
//      (simulates online, low-latency writes).
//   2. BULK   upsert  — fewer workers, large chunks via /bulk
//      (simulates batch ingestion).
//   3. QUERY  search  — concurrent dense top-k searches.
//
// Tunable via env vars (defaults in parentheses):
//
//	VECTORDB_URL          (http://127.0.0.1:8080)
//	VECTORDB_API_KEY      (unset)
//	LT_COLLECTION         (loadtest)
//	LT_DIM                (768)         vector dimension
//	LT_TOTAL              (50000)       total points to insert per phase
//	LT_SINGLE_CONCURRENCY (16)          concurrent workers in single-upsert phase
//	LT_SINGLE_BATCH       (32)          points per /upsert request
//	LT_BULK_CONCURRENCY   (4)           concurrent workers in bulk phase
//	LT_BULK_CHUNK         (256)         points per /bulk request (server may sub-chunk)
//	                                    NOTE: at dim=768 each point is ~6KB JSON,
//	                                    so 256 points ≈ 1.5MB — safely under the
//	                                    gateway's default 2MB body limit. Raise
//	                                    only if your gateway's BodyLimit is larger.
//	LT_QUERY_CONCURRENCY  (32)
//	LT_QUERY_TOTAL        (10000)       number of search calls
//	LT_QUERY_TOPK         (20)
//	LT_HTTP_TIMEOUT_S     (60)
//	LT_KEEP_COLLECTION    (0)           1 = leave the collection in place at end
//	LT_PROGRESS_S         (5)           seconds between live progress lines
//
// Run:
//   cd sdks/go && go run ./examples/loadtest
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	vectordb "github.com/vectordb/vectordb/sdks/go"
)

type config struct {
	url          string
	apiKey       string
	collection   string
	dim          int
	total        int
	singleConc   int
	singleBatch  int
	bulkConc     int
	bulkChunk    int
	queryConc    int
	queryTotal   int
	queryTopK    int
	httpTimeoutS int
	keep         bool
	progressSecs int
}

func loadConfig() config {
	return config{
		url:          getenv("VECTORDB_URL", "http://127.0.0.1:8080"),
		apiKey:       os.Getenv("VECTORDB_API_KEY"),
		collection:   getenv("LT_COLLECTION", "loadtest"),
		dim:          getenvInt("LT_DIM", 768),
		total:        getenvInt("LT_TOTAL", 50000),
		singleConc:   getenvInt("LT_SINGLE_CONCURRENCY", 16),
		singleBatch:  getenvInt("LT_SINGLE_BATCH", 32),
		bulkConc:     getenvInt("LT_BULK_CONCURRENCY", 4),
		bulkChunk:    getenvInt("LT_BULK_CHUNK", 256),
		queryConc:    getenvInt("LT_QUERY_CONCURRENCY", 32),
		queryTotal:   getenvInt("LT_QUERY_TOTAL", 10000),
		queryTopK:    getenvInt("LT_QUERY_TOPK", 20),
		httpTimeoutS: getenvInt("LT_HTTP_TIMEOUT_S", 60),
		keep:         getenvInt("LT_KEEP_COLLECTION", 0) == 1,
		progressSecs: getenvInt("LT_PROGRESS_S", 5),
	}
}

func main() {
	cfg := loadConfig()
	log.Printf("config: %+v", cfg)

	c := vectordb.NewClient(cfg.url, cfg.apiKey)
	c.HTTPClient.Timeout = time.Duration(cfg.httpTimeoutS) * time.Second

	ctx := context.Background()
	if h, err := c.Health(ctx); err != nil {
		log.Fatalf("health check: %v", err)
	} else {
		log.Printf("connected: %s (%v)", cfg.url, h)
	}

	if !cfg.keep {
		_ = c.DeleteCollection(ctx, cfg.collection)
	}

	if err := c.CreateCollection(ctx, cfg.collection, cfg.dim, vectordb.CreateCollectionOpts{
		Metric: "cosine",
		PayloadIndexes: []vectordb.PayloadIndex{
			{Field: "shard_hint", Kind: "keyword"},
			{Field: "i", Kind: "numeric"},
		},
	}); err != nil {
		log.Fatalf("create collection: %v", err)
	}
	log.Printf("created %q (dim=%d)", cfg.collection, cfg.dim)

	// Phase 1: single upsert
	singleStats := runSingleUpsert(ctx, c, cfg)
	singleStats.print("SINGLE upsert", cfg.total)

	// Phase 2: bulk upsert (fresh ID range so we don't overwrite)
	bulkStats := runBulkUpsert(ctx, c, cfg)
	bulkStats.print("BULK upsert", cfg.total)

	// Phase 3: query
	queryStats := runQuery(ctx, c, cfg)
	queryStats.print("QUERY search", cfg.queryTotal)

	// Final cluster + collection state
	if cs, err := c.ClusterStatus(ctx); err == nil {
		log.Printf("cluster: %d shards x RF=%d, %d nodes", cs.ShardCount, cs.ReplicationFactor, len(cs.Nodes))
	}
	// describe is best-effort — don't fail the test if it 502s
	_, _ = c.Health(ctx)

	if !cfg.keep {
		log.Printf("dropping collection %q (LT_KEEP_COLLECTION=1 to keep)", cfg.collection)
		if err := c.DeleteCollection(ctx, cfg.collection); err != nil {
			log.Printf("delete collection: %v", err)
		}
	}
	log.Printf("done.")
}

// ---- workload: single upsert ----

func runSingleUpsert(ctx context.Context, c *vectordb.Client, cfg config) *stats {
	log.Printf("[single] %d points across %d workers, batch=%d",
		cfg.total, cfg.singleConc, cfg.singleBatch)
	st := newStats()

	// Worker queue: each task is one batch of point indices [start, end).
	tasks := make(chan [2]int, 2*cfg.singleConc)
	go func() {
		for start := 0; start < cfg.total; start += cfg.singleBatch {
			end := start + cfg.singleBatch
			if end > cfg.total {
				end = cfg.total
			}
			tasks <- [2]int{start, end}
		}
		close(tasks)
	}()

	stop := startProgress(st, cfg.progressSecs, "[single]")
	var wg sync.WaitGroup
	for w := 0; w < cfg.singleConc; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 0xfeedface))
			for r := range tasks {
				batch := makePoints(rng, cfg.dim, "s", r[0], r[1])
				t0 := time.Now()
				_, err := c.Upsert(ctx, cfg.collection, batch)
				st.record(time.Since(t0), len(batch), err)
			}
		}(uint64(w + 1))
	}
	wg.Wait()
	close(stop)
	return st
}

// ---- workload: bulk upsert ----

func runBulkUpsert(ctx context.Context, c *vectordb.Client, cfg config) *stats {
	log.Printf("[bulk] %d points across %d workers, chunk=%d",
		cfg.total, cfg.bulkConc, cfg.bulkChunk)
	st := newStats()

	tasks := make(chan [2]int, 2*cfg.bulkConc)
	go func() {
		for start := 0; start < cfg.total; start += cfg.bulkChunk {
			end := start + cfg.bulkChunk
			if end > cfg.total {
				end = cfg.total
			}
			tasks <- [2]int{start, end}
		}
		close(tasks)
	}()

	stop := startProgress(st, cfg.progressSecs, "[bulk]")
	var wg sync.WaitGroup
	for w := 0; w < cfg.bulkConc; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed+1000, 0xcafebabe))
			for r := range tasks {
				batch := makePoints(rng, cfg.dim, "b", r[0], r[1])
				t0 := time.Now()
				_, err := c.BulkUpsert(ctx, cfg.collection, batch, 256)
				st.record(time.Since(t0), len(batch), err)
			}
		}(uint64(w + 1))
	}
	wg.Wait()
	close(stop)
	return st
}

// ---- workload: query ----

func runQuery(ctx context.Context, c *vectordb.Client, cfg config) *stats {
	log.Printf("[query] %d searches across %d workers, top_k=%d",
		cfg.queryTotal, cfg.queryConc, cfg.queryTopK)
	st := newStats()

	tasks := make(chan int, 2*cfg.queryConc)
	go func() {
		for i := 0; i < cfg.queryTotal; i++ {
			tasks <- i
		}
		close(tasks)
	}()

	stop := startProgress(st, cfg.progressSecs, "[query]")
	var wg sync.WaitGroup
	for w := 0; w < cfg.queryConc; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed+2000, 0xbadc0ffee))
			vec := make([]float32, cfg.dim)
			for range tasks {
				for j := range vec {
					vec[j] = float32(rng.NormFloat64())
				}
				t0 := time.Now()
				_, err := c.Search(ctx, cfg.collection, vec, vectordb.SearchOpts{TopK: cfg.queryTopK})
				st.record(time.Since(t0), 1, err)
			}
		}(uint64(w + 1))
	}
	wg.Wait()
	close(stop)
	return st
}

// ---- helpers ----

func makePoints(rng *rand.Rand, dim int, prefix string, start, end int) []vectordb.Point {
	out := make([]vectordb.Point, end-start)
	for i := range out {
		idx := start + i
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		out[i] = vectordb.Point{
			ID:     fmt.Sprintf("%s-%07d", prefix, idx),
			Values: v,
			Payload: map[string]any{
				"shard_hint": []string{"a", "b", "c", "d", "e"}[idx%5],
				"i":          idx,
			},
		}
	}
	return out
}

func startProgress(st *stats, everySecs int, tag string) chan struct{} {
	stop := make(chan struct{})
	if everySecs <= 0 {
		go func() { <-stop }()
		return stop
	}
	t0 := time.Now()
	go func() {
		tick := time.NewTicker(time.Duration(everySecs) * time.Second)
		defer tick.Stop()
		var lastOps, lastItems uint64
		lastT := t0
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				ops := atomic.LoadUint64(&st.ops)
				items := atomic.LoadUint64(&st.items)
				errs := atomic.LoadUint64(&st.errs)
				now := time.Now()
				dt := now.Sub(lastT).Seconds()
				dops := float64(ops - lastOps)
				ditems := float64(items - lastItems)
				log.Printf("%s ops=%d items=%d errs=%d  | last %.0fs: %.1f op/s, %.0f items/s",
					tag, ops, items, errs, dt, dops/dt, ditems/dt)
				lastOps, lastItems, lastT = ops, items, now
			}
		}
	}()
	return stop
}

// ---- stats ----

type stats struct {
	ops    uint64 // requests completed
	items  uint64 // points (or searches) accounted
	errs   uint64
	latMu  sync.Mutex
	lat    []time.Duration
	t0     time.Time
	tDone  time.Time
	errMap map[string]uint64
}

func newStats() *stats {
	return &stats{
		t0:     time.Now(),
		lat:    make([]time.Duration, 0, 65536),
		errMap: make(map[string]uint64),
	}
}

func (s *stats) record(d time.Duration, n int, err error) {
	atomic.AddUint64(&s.ops, 1)
	if err != nil {
		atomic.AddUint64(&s.errs, 1)
		s.latMu.Lock()
		// Truncate error key to avoid map blow-up on long messages.
		key := err.Error()
		if len(key) > 80 {
			key = key[:80]
		}
		s.errMap[key]++
		s.latMu.Unlock()
		return
	}
	atomic.AddUint64(&s.items, uint64(n))
	s.latMu.Lock()
	s.lat = append(s.lat, d)
	s.latMu.Unlock()
}

func (s *stats) print(label string, expected int) {
	s.tDone = time.Now()
	dur := s.tDone.Sub(s.t0).Seconds()
	ops := atomic.LoadUint64(&s.ops)
	items := atomic.LoadUint64(&s.items)
	errs := atomic.LoadUint64(&s.errs)

	s.latMu.Lock()
	lat := append([]time.Duration(nil), s.lat...)
	errMap := make(map[string]uint64, len(s.errMap))
	for k, v := range s.errMap {
		errMap[k] = v
	}
	s.latMu.Unlock()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		i := int(float64(len(lat)-1) * q)
		return lat[i]
	}
	mean := time.Duration(0)
	if len(lat) > 0 {
		var sum time.Duration
		for _, d := range lat {
			sum += d
		}
		mean = sum / time.Duration(len(lat))
	}

	log.Printf("---- %s ----", label)
	log.Printf("  duration:       %.2fs", dur)
	log.Printf("  requests:       %d ok, %d err  (success=%.2f%%)",
		ops-errs, errs, 100.0*float64(ops-errs)/float64(max1(ops)))
	log.Printf("  items:          %d / %d expected", items, expected)
	log.Printf("  throughput:     %.1f req/s, %.0f items/s",
		float64(ops)/dur, float64(items)/dur)
	log.Printf("  latency p50:    %v", p(0.50))
	log.Printf("  latency p95:    %v", p(0.95))
	log.Printf("  latency p99:    %v", p(0.99))
	log.Printf("  latency max:    %v", p(1.0))
	log.Printf("  latency mean:   %v", mean)

	if len(errMap) > 0 {
		log.Printf("  error breakdown (top 5):")
		type kv struct {
			k string
			v uint64
		}
		es := make([]kv, 0, len(errMap))
		for k, v := range errMap {
			es = append(es, kv{k, v})
		}
		sort.Slice(es, func(i, j int) bool { return es[i].v > es[j].v })
		if len(es) > 5 {
			es = es[:5]
		}
		for _, e := range es {
			log.Printf("    %5d  %s", e.v, e.k)
		}
	}
}

func max1(x uint64) uint64 {
	if x == 0 {
		return 1
	}
	return x
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
