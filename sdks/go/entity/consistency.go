package entity

// ConsistencyLevel mirrors Milvus's [entity.ConsistencyLevel]. VexaDb
// is strongly consistent through Raft for writes, but the SDK keeps the
// enum for API parity and forward compatibility — search/query callers
// may pass a level to express their freshness expectations.
type ConsistencyLevel int32

const (
	// ClStrong: all operations are immediately visible (VexaDb default).
	ClStrong ConsistencyLevel = 0
	// ClBounded: bounded staleness (~5s window).
	ClBounded ConsistencyLevel = 1
	// ClSession: session consistency (reads see same-session writes).
	ClSession ConsistencyLevel = 2
	// ClEventually: best query performance, possibly stale reads.
	ClEventually ConsistencyLevel = 3
	// ClCustomized: user-supplied guarantee timestamp.
	ClCustomized ConsistencyLevel = 4
)

// String returns a stable name suitable for headers / JSON.
func (c ConsistencyLevel) String() string {
	switch c {
	case ClStrong:
		return "Strong"
	case ClBounded:
		return "Bounded"
	case ClSession:
		return "Session"
	case ClEventually:
		return "Eventually"
	case ClCustomized:
		return "Customized"
	default:
		return "Strong"
	}
}
