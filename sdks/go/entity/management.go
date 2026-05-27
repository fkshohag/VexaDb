package entity

import (
	"github.com/vectordb/vectordb/sdks/go/index"
)

// ---- Load state (Milvus parity) -----------------------------------------

// LoadStateCode mirrors Milvus's commonpb LoadStateCode. VexaDb keeps every
// collection memory-resident, so the only states a caller will observe are
// `Loaded` and `NotExist` (returned as an error, not a code).
type LoadStateCode int32

const (
	LoadStateUnspecified LoadStateCode = 0
	LoadStateNotExist    LoadStateCode = 1
	LoadStateNotLoad     LoadStateCode = 2
	LoadStateLoading     LoadStateCode = 3
	LoadStateLoaded      LoadStateCode = 4
)

func (c LoadStateCode) String() string {
	switch c {
	case LoadStateNotExist:
		return "NotExist"
	case LoadStateNotLoad:
		return "NotLoad"
	case LoadStateLoading:
		return "Loading"
	case LoadStateLoaded:
		return "Loaded"
	default:
		return "Unspecified"
	}
}

// LoadState reports the load progress of a collection. Progress is a
// percentage in `[0, 100]`.
type LoadState struct {
	State    LoadStateCode `json:"state"`
	Progress int64         `json:"progress"`
}

// ---- Compaction (Milvus parity) -----------------------------------------

// CompactionState mirrors Milvus's commonpb CompactionState.
type CompactionState int32

const (
	CompactionStateUnspecified CompactionState = 0
	CompactionStateRunning     CompactionState = 1
	CompactionStateCompleted   CompactionState = 2
	CompactionStateFailed      CompactionState = 3
)

const (
	// Aliases matching Milvus's exported constant names.
	CompactionStateExecuting = CompactionStateRunning
)

func (c CompactionState) String() string {
	switch c {
	case CompactionStateRunning:
		return "Running"
	case CompactionStateCompleted:
		return "Completed"
	case CompactionStateFailed:
		return "Failed"
	default:
		return "Unspecified"
	}
}

// CompactionInfo carries the full server-side picture of a compaction job.
// Returned by `Client.GetCompactionState`.
type CompactionInfo struct {
	CompactionID  uint64          `json:"compaction_id"`
	Collection    string          `json:"collection"`
	State         CompactionState `json:"state"`
	EntriesBefore uint64          `json:"entries_before"`
	EntriesAfter  uint64          `json:"entries_after"`
	StartedMs     uint64          `json:"started_ms"`
	FinishedMs    uint64          `json:"finished_ms"`
	Error         string          `json:"error,omitempty"`
}

// ---- Segments (Milvus parity) -------------------------------------------

// SegmentState mirrors Milvus's commonpb SegmentState. VexaDb only emits
// `Growing` (live WAL) and `Flushed` (snapshot-captured) segments today.
type SegmentState int32

const (
	SegmentStateUnspecified SegmentState = 0
	SegmentStateGrowing     SegmentState = 1
	SegmentStateSealed      SegmentState = 2
	SegmentStateFlushed     SegmentState = 3
)

func (s SegmentState) String() string {
	switch s {
	case SegmentStateGrowing:
		return "Growing"
	case SegmentStateSealed:
		return "Sealed"
	case SegmentStateFlushed:
		return "Flushed"
	default:
		return "Unspecified"
	}
}

// Segment describes a single persistent segment. `Flushed()` matches
// Milvus's helper and reports whether the segment has been written to
// stable storage.
type Segment struct {
	ID           int64        `json:"id"`
	CollectionID int64        `json:"collection_id"`
	PartitionID  int64        `json:"partition_id"`
	NumRows      int64        `json:"num_rows"`
	State        SegmentState `json:"state"`
	// Source is VexaDb-specific: the producer of this segment (`wal`,
	// `snapshot:<id>`). Milvus does not expose this — populated for parity
	// with its more granular segment table without breaking callers that
	// only look at the standard fields.
	Source string `json:"source,omitempty"`
}

// Flushed reports whether the segment has been persisted to durable storage.
func (s *Segment) Flushed() bool {
	if s == nil {
		return false
	}
	return s.State == SegmentStateFlushed
}

// ---- Index description (Milvus parity) ----------------------------------

// IndexDescription bundles the static index configuration (via the embedded
// `index.Index`) with runtime build-progress counters. Server-side row
// counters are best-effort; VexaDb builds indexes synchronously so
// `IndexedRows == TotalRows` after a successful CreateIndex.
type IndexDescription struct {
	index.Index `json:"-"`

	// Field is the collection field this index is attached to.
	Field string `json:"field"`
	// IndexName is the user-facing name; matches Field when not customized.
	IndexName string `json:"name"`
	// Scope is "vector" for the HNSW index, "scalar" or "sparse" otherwise.
	Scope string `json:"scope,omitempty"`
	// State is the current build state.
	State index.IndexState `json:"state"`
	// PendingIndexRows is the number of rows queued for indexing.
	PendingIndexRows int64 `json:"pending_index_rows"`
	// TotalRows is the total number of rows the collection currently holds.
	TotalRows int64 `json:"total_rows"`
	// IndexedRows is the number of rows already indexed.
	IndexedRows int64 `json:"indexed_rows"`
	// Params is the opaque parameter map carried with the index.
	Params map[string]string `json:"params,omitempty"`
}

// ---- Flush task return shape (Milvus parity) ----------------------------

// FlushStats mirrors the (segIDs, flushSegIDs, flushTs, channelCheckpoints)
// tuple Milvus's FlushTask.GetFlushStats returns. VexaDb has no per-channel
// checkpoint concept, so `ChannelCheckpoints` is always nil — kept on the
// struct for source compat.
type FlushStats struct {
	SegmentIDs         []uint64
	FlushedSegmentIDs  []uint64
	FlushTs            uint64
	ChannelCheckpoints map[string]uint64
}
