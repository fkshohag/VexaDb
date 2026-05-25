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
}

type columnData struct {
	strings   []string
	int64s    []int64
	floats    []float64
	bools     []bool
	vectors   [][]float32
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

// Row is one entity for row-based insert.
type Row struct {
	ID     any // string or int64
	Fields map[string]any
}

// RowBasedInsertOption inserts entities row-by-row.
type RowBasedInsertOption struct {
	Collection string
	Rows       []Row
	VectorField string
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
			case len(col.strings) > 0:
				payload[name] = col.strings[i]
			case len(col.int64s) > 0:
				payload[name] = col.int64s[i]
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
	ID      string                 `json:"id"`
	Values  []float32              `json:"values"`
	Payload map[string]any         `json:"payload,omitempty"`
	Sparse  *entity.SparseVector   `json:"sparse,omitempty"`
}

