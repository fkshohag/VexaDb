package entity

// Vector is a query or stored embedding (Milvus-compatible).
type Vector interface {
	Dim() int
	ToFloat32Slice() []float32
}

// FloatVector is a dense float32 embedding.
type FloatVector []float32

func (v FloatVector) Dim() int { return len(v) }

func (v FloatVector) ToFloat32Slice() []float32 { return []float32(v) }

// SparseVector holds sparse components (optional on upsert).
type SparseVector struct {
	Indices []uint32
	Values  []float32
}
