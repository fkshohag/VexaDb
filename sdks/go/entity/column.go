package entity

import "fmt"

// Column is a typed column in a ResultSet (subset of Milvus column API).
type Column interface {
	Name() string
	Len() int
}

// StringColumn holds string values (IDs, varchar fields).
type StringColumn struct {
	Field string
	Data  []string
}

func (c StringColumn) Name() string { return c.Field }
func (c StringColumn) Len() int     { return len(c.Data) }

// Int64Column holds int64 values.
type Int64Column struct {
	Field string
	Data  []int64
}

func (c Int64Column) Name() string { return c.Field }
func (c Int64Column) Len() int     { return len(c.Data) }

// FloatVectorColumn holds [][]float32 embeddings.
type FloatVectorColumn struct {
	Field string
	Data  [][]float32
	Dim   int
}

func (c FloatVectorColumn) Name() string { return c.Field }
func (c FloatVectorColumn) Len() int     { return len(c.Data) }

// JSONFieldColumn holds per-row JSON payloads as map[string]any.
type JSONFieldColumn struct {
	Field string
	Data  []map[string]any
}

func (c JSONFieldColumn) Name() string { return c.Field }
func (c JSONFieldColumn) Len() int     { return len(c.Data) }

// ResultSet groups columns returned from Search or Query.
type ResultSet struct {
	Scores  []float32
	Fields  map[string]Column
	RowCount int
}

// GetColumn returns a column by field name.
func (rs *ResultSet) GetColumn(name string) (Column, error) {
	c, ok := rs.Fields[name]
	if !ok {
		return nil, fmt.Errorf("column %q not found", name)
	}
	return c, nil
}

// IDs returns the primary-key column as strings (int64 IDs are formatted).
func (rs *ResultSet) IDs() ([]string, error) {
	for _, c := range rs.Fields {
		switch col := c.(type) {
		case StringColumn:
			if col.Field == "id" || col.Len() == rs.RowCount {
				return col.Data, nil
			}
		}
	}
	if c, err := rs.GetColumn("id"); err == nil {
		if sc, ok := c.(StringColumn); ok {
			return sc.Data, nil
		}
	}
	return nil, fmt.Errorf("id column not found")
}
