package vexaclient

import (
	"fmt"
	"strconv"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// ColumnBasedInsertOption inserts entities as columns (Milvus-style).
type ColumnBasedInsertOption struct {
	Collection string
	// idColumn holds primary keys (string or int64 slice).
	idStrings []string
	idInt64s  []int64
	columns   map[string]columnData
	// Partition is the optional target partition (Milvus parity).
	// Empty string means the collection's `_default` partition.
	Partition string
	// PartialUpdate enables Milvus-style partial upsert: existing fields not
	// in the payload are preserved instead of being cleared. Only honored by
	// `Client.Upsert`; ignored on plain `Insert`.
	PartialUpdate bool
}

type columnData struct {
	strings   []string
	int64s    []int64
	int32s    []int32
	int16s    []int16
	int8s     []int8
	floats    []float64
	bools     []bool
	vectors   [][]float32
	binaryVec [][]byte
	int8Vec   [][]int8
	vectorDim int
	sparse    []*entity.SparseVector
}

func NewColumnBasedInsertOption(collection string) *ColumnBasedInsertOption {
	return &ColumnBasedInsertOption{
		Collection: collection,
		columns:    map[string]columnData{},
	}
}

func (o *ColumnBasedInsertOption) WithVarcharColumn(name string, data []string) *ColumnBasedInsertOption {
	o.columns[name] = columnData{strings: data}
	return o
}

func (o *ColumnBasedInsertOption) WithInt64Column(name string, data []int64) *ColumnBasedInsertOption {
	if name == "id" || name == "pk" {
		o.idInt64s = data
		return o
	}
	o.columns[name] = columnData{int64s: data}
	return o
}

func (o *ColumnBasedInsertOption) WithFloatVectorColumn(name string, dim int, data [][]float32) *ColumnBasedInsertOption {
	o.columns[name] = columnData{vectors: data, vectorDim: dim}
	return o
}

func (o *ColumnBasedInsertOption) WithBoolColumn(name string, data []bool) *ColumnBasedInsertOption {
	o.columns[name] = columnData{bools: data}
	return o
}

func (o *ColumnBasedInsertOption) WithFloatColumn(name string, data []float64) *ColumnBasedInsertOption {
	o.columns[name] = columnData{floats: data}
	return o
}

// WithIDs sets primary keys as strings (varchar PK).
func (o *ColumnBasedInsertOption) WithIDs(ids []string) *ColumnBasedInsertOption {
	o.idStrings = ids
	return o
}

// WithInt8Column / WithInt16Column / WithInt32Column store narrow ints
// in the payload as int64 (VexaDb has one numeric type at rest).
func (o *ColumnBasedInsertOption) WithInt8Column(name string, data []int8) *ColumnBasedInsertOption {
	o.columns[name] = columnData{int8s: data}
	return o
}
func (o *ColumnBasedInsertOption) WithInt16Column(name string, data []int16) *ColumnBasedInsertOption {
	o.columns[name] = columnData{int16s: data}
	return o
}
func (o *ColumnBasedInsertOption) WithInt32Column(name string, data []int32) *ColumnBasedInsertOption {
	o.columns[name] = columnData{int32s: data}
	return o
}

// WithBinaryVectorColumn stores a binary vector column. VexaDb decodes the
// byte stream to float32 dimensions on the gateway by `byte >> bit`
// expansion so the rest of the engine stays single-typed.
func (o *ColumnBasedInsertOption) WithBinaryVectorColumn(name string, dim int, data [][]byte) *ColumnBasedInsertOption {
	o.columns[name] = columnData{binaryVec: data, vectorDim: dim}
	return o
}

// WithFloat16VectorColumn / WithBFloat16VectorColumn accept the caller's
// float32 representation. VexaDb stores float32 internally; the explicit
// builders exist purely for Milvus parity so callers do not have to
// rewrite their setup code.
func (o *ColumnBasedInsertOption) WithFloat16VectorColumn(name string, dim int, data [][]float32) *ColumnBasedInsertOption {
	o.columns[name] = columnData{vectors: data, vectorDim: dim}
	return o
}
func (o *ColumnBasedInsertOption) WithBFloat16VectorColumn(name string, dim int, data [][]float32) *ColumnBasedInsertOption {
	o.columns[name] = columnData{vectors: data, vectorDim: dim}
	return o
}

// WithInt8VectorColumn stores int8 vector data (e.g. quantized embeddings).
// The payload is sent as raw int8 values; VexaDb expands them to float32 on
// the server side.
func (o *ColumnBasedInsertOption) WithInt8VectorColumn(name string, dim int, data [][]int8) *ColumnBasedInsertOption {
	o.columns[name] = columnData{int8Vec: data, vectorDim: dim}
	return o
}

// WithSparseColumn attaches a slice of optional sparse vectors aligned with
// the row order.
func (o *ColumnBasedInsertOption) WithSparseColumn(name string, data []*entity.SparseVector) *ColumnBasedInsertOption {
	o.columns[name] = columnData{sparse: data}
	return o
}

// WithColumns accepts pre-built typed columns (Milvus parity).
func (o *ColumnBasedInsertOption) WithColumns(cols ...entity.Column) *ColumnBasedInsertOption {
	for _, col := range cols {
		switch c := col.(type) {
		case entity.StringColumn:
			o.columns[c.Field] = columnData{strings: c.Data}
		case entity.Int64Column:
			o.columns[c.Field] = columnData{int64s: c.Data}
		case entity.FloatVectorColumn:
			o.columns[c.Field] = columnData{vectors: c.Data, vectorDim: c.Dim}
		}
	}
	return o
}

// WithPartition targets a specific partition (Milvus parity).
func (o *ColumnBasedInsertOption) WithPartition(name string) *ColumnBasedInsertOption {
	o.Partition = name
	return o
}

// WithPartialUpdate switches the upsert to Milvus's partial-update mode:
// existing payload fields not present in the new payload are preserved.
func (o *ColumnBasedInsertOption) WithPartialUpdate(on bool) *ColumnBasedInsertOption {
	o.PartialUpdate = on
	return o
}

// Row is one entity for row-based insert.
type Row struct {
	ID     any // string or int64
	Fields map[string]any
}

// RowBasedInsertOption inserts entities row-by-row.
type RowBasedInsertOption struct {
	Collection  string
	Rows        []Row
	VectorField string
	Partition   string
}

func (o *RowBasedInsertOption) WithPartition(name string) *RowBasedInsertOption {
	o.Partition = name
	return o
}

func NewRowBasedInsertOption(collection string, rows ...Row) *RowBasedInsertOption {
	return &RowBasedInsertOption{Collection: collection, Rows: rows, VectorField: "vector"}
}

func (o *RowBasedInsertOption) WithVectorField(name string) *RowBasedInsertOption {
	o.VectorField = name
	return o
}

func rowCountFromColumn(o *ColumnBasedInsertOption) int {
	if len(o.idStrings) > 0 {
		return len(o.idStrings)
	}
	if len(o.idInt64s) > 0 {
		return len(o.idInt64s)
	}
	for _, c := range o.columns {
		if len(c.strings) > 0 {
			return len(c.strings)
		}
		if len(c.int64s) > 0 {
			return len(c.int64s)
		}
		if len(c.vectors) > 0 {
			return len(c.vectors)
		}
	}
	return 0
}

func (o *ColumnBasedInsertOption) buildPoints() ([]gatewayPoint, error) {
	n := rowCountFromColumn(o)
	if n == 0 {
		return nil, fmt.Errorf("no rows in insert option")
	}
	points := make([]gatewayPoint, n)
	for i := 0; i < n; i++ {
		var id string
		switch {
		case len(o.idStrings) > 0:
			id = o.idStrings[i]
		case len(o.idInt64s) > 0:
			id = strconv.FormatInt(o.idInt64s[i], 10)
		default:
			id = fmt.Sprintf("%d", i)
		}
		payload := map[string]any{}
		var values []float32
		var sparse *entity.SparseVector
		for name, col := range o.columns {
			switch {
			case len(col.vectors) > 0:
				values = col.vectors[i]
			case len(col.binaryVec) > 0:
				values = bytesToFloat32(col.binaryVec[i])
			case len(col.int8Vec) > 0:
				values = int8ToFloat32(col.int8Vec[i])
			case len(col.strings) > 0:
				payload[name] = col.strings[i]
			case len(col.int64s) > 0:
				payload[name] = col.int64s[i]
			case len(col.int32s) > 0:
				payload[name] = int64(col.int32s[i])
			case len(col.int16s) > 0:
				payload[name] = int64(col.int16s[i])
			case len(col.int8s) > 0:
				payload[name] = int64(col.int8s[i])
			case len(col.floats) > 0:
				payload[name] = col.floats[i]
			case len(col.bools) > 0:
				payload[name] = col.bools[i]
			case len(col.sparse) > 0 && col.sparse[i] != nil:
				sparse = col.sparse[i]
			}
		}
		points[i] = gatewayPoint{ID: id, Values: values, Payload: payload, Sparse: sparse}
	}
	return points, nil
}

func (o *RowBasedInsertOption) buildPoints() ([]gatewayPoint, error) {
	out := make([]gatewayPoint, 0, len(o.Rows))
	for _, row := range o.Rows {
		id, err := formatID(row.ID)
		if err != nil {
			return nil, err
		}
		payload := map[string]any{}
		var values []float32
		var sparse *entity.SparseVector
		for k, v := range row.Fields {
			if k == o.VectorField {
				switch vec := v.(type) {
				case []float32:
					values = vec
				case entity.FloatVector:
					values = vec
				case []float64:
					values = make([]float32, len(vec))
					for i, x := range vec {
						values[i] = float32(x)
					}
				default:
					return nil, fmt.Errorf("field %q: expected []float32 vector", k)
				}
				continue
			}
			if sv, ok := v.(*entity.SparseVector); ok {
				sparse = sv
				continue
			}
			payload[k] = v
		}
		out = append(out, gatewayPoint{ID: id, Values: values, Payload: payload, Sparse: sparse})
	}
	return out, nil
}

func formatID(id any) (string, error) {
	if id == nil {
		return "", fmt.Errorf("row ID is required")
	}
	switch v := id.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	default:
		return "", fmt.Errorf("unsupported ID type %T (use string or int64)", id)
	}
}

type gatewayPoint struct {
	ID      string               `json:"id"`
	Values  []float32            `json:"values"`
	Payload map[string]any       `json:"payload,omitempty"`
	Sparse  *entity.SparseVector `json:"sparse,omitempty"`
}

// bytesToFloat32 expands a Milvus-style binary vector (1 bit per dimension,
// MSB first) to one float32 per bit so VexaDb can store it as a regular
// dense vector. Each output value is either 0.0 or 1.0.
func bytesToFloat32(b []byte) []float32 {
	out := make([]float32, 0, len(b)*8)
	for _, byteVal := range b {
		for i := 7; i >= 0; i-- {
			bit := (byteVal >> uint(i)) & 1
			out = append(out, float32(bit))
		}
	}
	return out
}

// int8ToFloat32 widens an int8 vector to float32 with no scaling. Callers
// that quantize on the client side keep the original semantics by
// normalizing before sending.
func int8ToFloat32(v []int8) []float32 {
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(x)
	}
	return out
}

