package entity

// FieldType describes a collection field (Milvus-compatible subset).
type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeInt64
	FieldTypeVarChar
	FieldTypeFloatVector
	FieldTypeBool
	FieldTypeFloat
)

// Field is one column in a collection schema.
type Field struct {
	Name       string
	DataType   FieldType
	IsPrimary  bool
	AutoID     bool
	Dim        int64   // for FloatVector
	MaxLength  int     // for VarChar
	Nullable   bool
}

// NewField returns a field builder.
func NewField() *Field {
	return &Field{}
}

func (f *Field) WithName(name string) *Field {
	f.Name = name
	return f
}

func (f *Field) WithDataType(t FieldType) *Field {
	f.DataType = t
	return f
}

func (f *Field) WithIsPrimaryKey(pk bool) *Field {
	f.IsPrimary = pk
	return f
}

func (f *Field) WithIsAutoID(auto bool) *Field {
	f.AutoID = auto
	return f
}

func (f *Field) WithDim(dim int64) *Field {
	f.Dim = dim
	return f
}

func (f *Field) WithMaxLength(n int) *Field {
	f.MaxLength = n
	return f
}

// Schema is a collection schema (field list + options).
type Schema struct {
	Fields        []*Field
	DynamicFields bool
}

// NewSchema creates an empty schema.
func NewSchema() *Schema {
	return &Schema{}
}

func (s *Schema) WithField(field *Field) *Schema {
	s.Fields = append(s.Fields, field)
	return s
}

func (s *Schema) WithDynamicFieldEnabled(on bool) *Schema {
	s.DynamicFields = on
	return s
}

// PKFieldName returns the primary-key field name, or "".
func (s *Schema) PKFieldName() string {
	for _, f := range s.Fields {
		if f.IsPrimary {
			return f.Name
		}
	}
	return ""
}

// VectorFieldName returns the first float-vector field name.
func (s *Schema) VectorFieldName() string {
	for _, f := range s.Fields {
		if f.DataType == FieldTypeFloatVector {
			return f.Name
		}
	}
	return ""
}

// Dimension returns the vector dimension from the schema.
func (s *Schema) Dimension() int64 {
	for _, f := range s.Fields {
		if f.DataType == FieldTypeFloatVector {
			return f.Dim
		}
	}
	return 0
}

// PayloadIndexKind for filter pushdown (maps to gateway payload_indexes).
type PayloadIndexKind string

const (
	IndexKeyword PayloadIndexKind = "keyword"
	IndexNumeric PayloadIndexKind = "numeric"
	IndexBool    PayloadIndexKind = "bool"
)

// PayloadIndex describes an indexed payload field.
type PayloadIndex struct {
	Field string
	Kind  PayloadIndexKind
}
