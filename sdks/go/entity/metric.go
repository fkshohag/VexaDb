package entity

// MetricType is the distance metric for vector similarity.
type MetricType int

const (
	MetricUndefined MetricType = iota
	COSINE
	L2
	IP // inner product (dot)
)

func (m MetricType) String() string {
	switch m {
	case COSINE:
		return "cosine"
	case L2:
		return "euclidean"
	case IP:
		return "dot"
	default:
		return "cosine"
	}
}
