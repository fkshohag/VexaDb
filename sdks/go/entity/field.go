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
	FieldTypeInt8
	FieldTypeInt16
	FieldTypeInt32
	FieldTypeDouble
	FieldTypeString
	FieldTypeArray
	FieldTypeJSON
	FieldTypeGeometry
	FieldTypeTimestamptz
	FieldTypeBinaryVector
	FieldTypeFloat16Vector
	FieldTypeBFloat16Vector
	FieldTypeSparseVector
	FieldTypeInt8Vector
)

// Field is one column in a collection schema.
type Field struct {
	Name            string
	Description     string
	DataType        FieldType
	IsPrimary       bool
	AutoID          bool
	Dim             int64 // for FloatVector
	MaxLength       int   // for VarChar
	Nullable        bool
	IsPartitionKey  bool
	IsClusteringKey bool
	IsDynamic       bool
	ElementType     FieldType
	MaxCapacity     int64
	EnableAnalyzer  bool
	EnableMatch     bool
	DefaultValue    any
	TypeParams      map[string]string
	AnalyzerParams  map[string]any
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

func (f *Field) WithDescription(desc string) *Field { f.Description = desc; return f }
func (f *Field) WithNullable(on bool) *Field        { f.Nullable = on; return f }
func (f *Field) WithIsPartitionKey(on bool) *Field  { f.IsPartitionKey = on; return f }
func (f *Field) WithIsClusteringKey(on bool) *Field { f.IsClusteringKey = on; return f }
func (f *Field) WithIsDynamic(on bool) *Field       { f.IsDynamic = on; return f }
func (f *Field) WithElementType(t FieldType) *Field { f.ElementType = t; return f }
func (f *Field) WithMaxCapacity(n int64) *Field     { f.MaxCapacity = n; return f }
func (f *Field) WithEnableAnalyzer(on bool) *Field  { f.EnableAnalyzer = on; return f }
func (f *Field) WithEnableMatch(on bool) *Field     { f.EnableMatch = on; return f }
func (f *Field) WithDefaultValueString(v string) *Field { f.DefaultValue = v; return f }
func (f *Field) WithDefaultValueInt(v int32) *Field     { f.DefaultValue = v; return f }
func (f *Field) WithDefaultValueLong(v int64) *Field    { f.DefaultValue = v; return f }
func (f *Field) WithDefaultValueFloat(v float32) *Field { f.DefaultValue = v; return f }
func (f *Field) WithDefaultValueDouble(v float64) *Field { f.DefaultValue = v; return f }
func (f *Field) WithDefaultValueBool(v bool) *Field      { f.DefaultValue = v; return f }

func (f *Field) WithTypeParams(key, value string) *Field {
	if f.TypeParams == nil {
		f.TypeParams = make(map[string]string)
	}
	f.TypeParams[key] = value
	return f
}

func (f *Field) WithAnalyzerParams(params map[string]any) *Field {
	f.AnalyzerParams = params
	return f
}

// Schema is a collection schema (field list + options).
type Schema struct {
	CollectionName string
	Description    string
	AutoID         bool
	Fields         []*Field
	DynamicFields  bool
	Functions      []*Function
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

func (s *Schema) WithName(name string) *Schema {
	s.CollectionName = name
	return s
}

func (s *Schema) WithDescription(desc string) *Schema {
	s.Description = desc
	return s
}

func (s *Schema) WithAutoID(on bool) *Schema {
	s.AutoID = on
	return s
}

// WithFunction attaches a built-in function (BM25, text embedding, rerank).
// Currently VexaDb consumes the first FunctionTypeBM25 function and routes
// it to the collection's bm25_text_field; other variants are forwarded as
// opaque metadata for future server-side support.
func (s *Schema) WithFunction(fn *Function) *Schema {
	if fn != nil {
		s.Functions = append(s.Functions, fn)
	}
	return s
}

// PKField returns the primary-key field, or nil.
func (s *Schema) PKField() *Field {
	for _, f := range s.Fields {
		if f.IsPrimary {
			return f
		}
	}
	return nil
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
