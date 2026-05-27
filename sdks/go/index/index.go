// Package index mirrors the surface of Milvus's `milvus-io/milvus/client/v2/index`
// for VexaDb's Go SDK. The interface and constructors keep the same names so
// existing Milvus call sites compile against vexaclient with the same `index`
// import.
//
// VexaDb's backend implements a strict subset of the algorithms Milvus exposes:
//   - HNSW (the always-on vector index)
//   - AUTOINDEX (alias for HNSW)
//   - keyword / numeric / bool inverted scalar indexes (Milvus "INVERTED",
//     "STL_SORT", "BITMAP")
//   - sparse inverted index for sparse vectors
//
// Constructors for index types VexaDb does not implement (IVF_*, DiskANN, GPU
// variants, MinHashLSH, R-Tree, MaxSim, ...) are kept for source-level
// compatibility — they build the same `Index` value, and the server validates
// the kind at create time so unsupported choices fail loudly instead of
// silently corrupting state.
package index

// MetricType enumerates the distance metrics. Values mirror Milvus's
// "L2" / "IP" / "COSINE" / "HAMMING" / "JACCARD" string constants so
// Milvus-style code compiles unchanged.
type MetricType string

const (
	L2            MetricType = "L2"
	IP            MetricType = "IP"
	COSINE        MetricType = "COSINE"
	HAMMING       MetricType = "HAMMING"
	JACCARD       MetricType = "JACCARD"
	TANIMOTO      MetricType = "TANIMOTO"
	SUBSTRUCTURE  MetricType = "SUBSTRUCTURE"
	SUPERSTRUCTURE MetricType = "SUPERSTRUCTURE"
	BM25          MetricType = "BM25"
	MHJACCARD     MetricType = "MHJACCARD"
	MaxSim        MetricType = "MAX_SIM"
	MaxSimCosine  MetricType = "MAX_SIM_COSINE"
	MaxSimL2      MetricType = "MAX_SIM_L2"
	MaxSimIP      MetricType = "MAX_SIM_IP"
	MaxSimHamming MetricType = "MAX_SIM_HAMMING"
	MaxSimJaccard MetricType = "MAX_SIM_JACCARD"
)

// MetricTypeL2/IP/COSINE provide the same aliases Milvus's docs use, so
// `index.MetricTypeL2` etc. resolve cleanly.
const (
	MetricTypeL2     = L2
	MetricTypeIP     = IP
	MetricTypeCOSINE = COSINE
)

// IndexType enumerates the supported index algorithms. Stringly-typed to
// stay forward-compatible with future Milvus additions.
type IndexType string

const (
	Flat           IndexType = "FLAT"
	BinFlat        IndexType = "BIN_FLAT"
	IvfFlat        IndexType = "IVF_FLAT"
	BinIvfFlat     IndexType = "BIN_IVF_FLAT"
	IvfPQ          IndexType = "IVF_PQ"
	IvfSQ8         IndexType = "IVF_SQ8"
	IvfRabitQ      IndexType = "IVF_RABITQ"
	HNSW           IndexType = "HNSW"
	IvfHNSW        IndexType = "IVF_HNSW"
	AUTOINDEX      IndexType = "AUTOINDEX"
	DISKANN        IndexType = "DISKANN"
	SCANN          IndexType = "SCANN"
	MinHashLSH     IndexType = "MINHASH_LSH"
	SparseInverted IndexType = "SPARSE_INVERTED_INDEX"
	SparseWAND     IndexType = "SPARSE_WAND"
	GPUIvfFlat     IndexType = "GPU_IVF_FLAT"
	GPUIvfPQ       IndexType = "GPU_IVF_PQ"
	GPUCagra       IndexType = "GPU_CAGRA"
	GPUBruteForce  IndexType = "GPU_BRUTE_FORCE"
	Trie           IndexType = "Trie"
	Sorted         IndexType = "STL_SORT"
	Inverted       IndexType = "INVERTED"
	BITMAP         IndexType = "BITMAP"
	RTREE          IndexType = "RTREE"
)

// IndexState mirrors Milvus's lifecycle. VexaDb builds indexes synchronously,
// so `Finished` is reported for every successful Create.
type IndexState int32

const (
	IndexStateUnspecified IndexState = 0
	IndexStateInProgress  IndexState = 1
	IndexStateFinished    IndexState = 2
	IndexStateFailed      IndexState = 3
	IndexStateRetry       IndexState = 4
)

func (s IndexState) String() string {
	switch s {
	case IndexStateInProgress:
		return "InProgress"
	case IndexStateFinished:
		return "Finished"
	case IndexStateFailed:
		return "Failed"
	case IndexStateRetry:
		return "Retry"
	default:
		return "Unspecified"
	}
}

// Index is the interface CreateIndex consumes. Use the constructors below
// (NewHNSWIndex, NewAutoIndex, NewInvertedIndex, …) to produce values.
type Index interface {
	Name() string
	IndexType() IndexType
	Params() map[string]string
}

// genericIndex is a concrete implementation that backs every constructor.
// Kept private so users always go through `NewXxxIndex` and we can layer
// validation later.
type genericIndex struct {
	name   string
	idxKind IndexType
	params map[string]string
}

func (g *genericIndex) Name() string             { return g.name }
func (g *genericIndex) IndexType() IndexType     { return g.idxKind }
func (g *genericIndex) Params() map[string]string {
	if g.params == nil {
		return map[string]string{}
	}
	// Defensive copy so callers can't mutate our internal map.
	out := make(map[string]string, len(g.params))
	for k, v := range g.params {
		out[k] = v
	}
	return out
}

// GenericIndex is the exported alias Milvus uses for NewGenericIndex's return.
type GenericIndex = *genericIndex

func makeIndex(kind IndexType, params map[string]string) *genericIndex {
	if params == nil {
		params = map[string]string{}
	}
	params["index_type"] = string(kind)
	return &genericIndex{idxKind: kind, params: params}
}

// ---- Constructors --------------------------------------------------------

// NewAutoIndex returns an AUTOINDEX configuration. VexaDb maps AUTOINDEX to
// HNSW under the hood.
func NewAutoIndex(metric MetricType) Index {
	return makeIndex(AUTOINDEX, map[string]string{"metric_type": string(metric)})
}

// NewHNSWIndex returns an HNSW configuration with the given M and
// efConstruction. Use [vectordb-core defaults] when in doubt: M=16,
// efConstruction=200.
func NewHNSWIndex(metric MetricType, m, efConstruction int) Index {
	return makeIndex(HNSW, map[string]string{
		"metric_type":     string(metric),
		"M":               itoa(m),
		"efConstruction":  itoa(efConstruction),
	})
}

// NewFlatIndex returns a brute-force (FLAT) configuration. VexaDb's backend
// transparently falls back to HNSW for FLAT today; the constructor is here
// for source compat.
func NewFlatIndex(metric MetricType) Index {
	return makeIndex(Flat, map[string]string{"metric_type": string(metric)})
}

// NewBinFlatIndex returns a binary FLAT configuration.
func NewBinFlatIndex(metric MetricType) Index {
	return makeIndex(BinFlat, map[string]string{"metric_type": string(metric)})
}

// NewIvfFlatIndex returns an IVF_FLAT configuration. VexaDb's backend will
// reject this with InvalidArgument until IVF lands; kept for source compat.
func NewIvfFlatIndex(metric MetricType, nlist int) Index {
	return makeIndex(IvfFlat, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewBinIvfFlatIndex — IVF_FLAT for binary vectors.
func NewBinIvfFlatIndex(metric MetricType, nlist int) Index {
	return makeIndex(BinIvfFlat, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewIvfPQIndex — IVF with product quantization.
func NewIvfPQIndex(metric MetricType, nlist, m, nbits int) Index {
	return makeIndex(IvfPQ, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
		"m":           itoa(m),
		"nbits":       itoa(nbits),
	})
}

// NewIvfSQ8Index — IVF with 8-bit scalar quantization.
func NewIvfSQ8Index(metric MetricType, nlist int) Index {
	return makeIndex(IvfSQ8, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewIvfRabitQIndex — IVF with RaBitQ quantization.
func NewIvfRabitQIndex(metric MetricType, nlist int) Index {
	return makeIndex(IvfRabitQ, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewDiskANNIndex — disk-based ANN. Compat constructor only.
func NewDiskANNIndex(metric MetricType) Index {
	return makeIndex(DISKANN, map[string]string{"metric_type": string(metric)})
}

// NewSCANNIndex — Google ScaNN. Compat constructor only.
func NewSCANNIndex(metric MetricType, nlist int) Index {
	return makeIndex(SCANN, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewMinHashLSHIndex — MinHash LSH. Compat constructor only.
func NewMinHashLSHIndex(metric MetricType) Index {
	return makeIndex(MinHashLSH, map[string]string{"metric_type": string(metric)})
}

// NewSparseInvertedIndex — VexaDb-supported sparse vector index.
func NewSparseInvertedIndex(metric MetricType, dropRatio float64) Index {
	return makeIndex(SparseInverted, map[string]string{
		"metric_type":      string(metric),
		"drop_ratio_build": ftoa(dropRatio),
	})
}

// NewSparseWANDIndex — sparse WAND index. Compat constructor only.
func NewSparseWANDIndex(metric MetricType, dropRatio float64) Index {
	return makeIndex(SparseWAND, map[string]string{
		"metric_type":      string(metric),
		"drop_ratio_build": ftoa(dropRatio),
	})
}

// NewGPUBruteForceIndex — GPU FLAT. Compat constructor only.
func NewGPUBruteForceIndex(metric MetricType) Index {
	return makeIndex(GPUBruteForce, map[string]string{"metric_type": string(metric)})
}

// NewGPUCagraIndex — GPU CAGRA. Compat constructor only.
func NewGPUCagraIndex(metric MetricType) Index {
	return makeIndex(GPUCagra, map[string]string{"metric_type": string(metric)})
}

// NewGPUIVPFlatIndex — GPU IVF_FLAT. Compat constructor only.
func NewGPUIVPFlatIndex(metric MetricType, nlist int) Index {
	return makeIndex(GPUIvfFlat, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
	})
}

// NewGPUIVPPQIndex — GPU IVF_PQ. Compat constructor only.
func NewGPUIVPPQIndex(metric MetricType, nlist, m, nbits int) Index {
	return makeIndex(GPUIvfPQ, map[string]string{
		"metric_type": string(metric),
		"nlist":       itoa(nlist),
		"m":           itoa(m),
		"nbits":       itoa(nbits),
	})
}

// NewInvertedIndex — scalar inverted index. Maps to VexaDb's `keyword`
// payload index at the engine boundary.
func NewInvertedIndex() Index {
	return makeIndex(Inverted, nil)
}

// NewSortedIndex — STL_SORT scalar index. Maps to VexaDb's `numeric` payload
// index at the engine boundary.
func NewSortedIndex() Index {
	return makeIndex(Sorted, nil)
}

// NewBitmapIndex — bitmap scalar index. Maps to VexaDb's `bool` payload index.
func NewBitmapIndex() Index {
	return makeIndex(BITMAP, nil)
}

// NewTrieIndex — string trie. Compat constructor only.
func NewTrieIndex() Index {
	return makeIndex(Trie, nil)
}

// NewJSONPathIndex — JSON path scalar index. Maps to `keyword` for parity.
func NewJSONPathIndex(path string) Index {
	return makeIndex(Inverted, map[string]string{"json_path": path})
}

// NewRTreeIndex — R-tree. Compat constructor only.
func NewRTreeIndex() Index {
	return makeIndex(RTREE, nil)
}

// NewGenericIndex returns a fully custom configuration. `name` becomes the
// stored index name; pass an empty string to default to the field name at
// CreateIndex time.
func NewGenericIndex(name string, params map[string]string) GenericIndex {
	idx := makeIndex(IndexType(params["index_type"]), copyParams(params))
	idx.name = name
	return idx
}

func copyParams(p map[string]string) map[string]string {
	if p == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

// VexaIndexKind maps a Milvus IndexType to VexaDb's engine kind string
// (`keyword`/`numeric`/`bool`/`hnsw`). Returns the second value `true`
// when the mapping is known; callers should validate before sending an
// unknown IndexType to the backend.
func VexaIndexKind(t IndexType) (string, bool) {
	switch t {
	case HNSW, AUTOINDEX, Flat:
		return "hnsw", true
	case Inverted, BITMAP, Trie:
		return "keyword", true
	case Sorted:
		return "numeric", true
	case SparseInverted, SparseWAND:
		return "sparse", true
	default:
		return "", false
	}
}

// ---- tiny helpers (avoid pulling in strconv at the top to keep the file
// readable when scanning for the public API surface).

func itoa(i int) string {
	// Mirrors strconv.Itoa without the runtime allocation discussion; we
	// use strconv here for clarity.
	return strconvItoa(i)
}

func ftoa(f float64) string {
	return strconvFormat(f)
}
