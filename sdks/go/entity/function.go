package entity

// FunctionType mirrors Milvus's function types. VexaDb implements BM25 today;
// other variants are accepted by the SDK for forward compatibility and may
// be rejected by the server.
type FunctionType string

const (
	FunctionTypeUnknown       FunctionType = ""
	FunctionTypeBM25          FunctionType = "BM25"
	FunctionTypeTextEmbedding FunctionType = "TextEmbedding"
	FunctionTypeRerank        FunctionType = "Rerank"
)

// Function attaches a built-in function (BM25, text embedding, rerank, …)
// to a collection schema. Today VexaDb consumes `FunctionTypeBM25` to set
// the collection's `bm25_text_field` from the function's first input field.
type Function struct {
	Name            string         `json:"name"`
	Description     string         `json:"description,omitempty"`
	Type            FunctionType   `json:"type"`
	InputFieldNames []string       `json:"input_field_names,omitempty"`
	OutputFieldNames []string      `json:"output_field_names,omitempty"`
	Params          map[string]any `json:"params,omitempty"`
}

// NewFunction returns a builder.
func NewFunction() *Function { return &Function{Type: FunctionTypeUnknown} }

func (f *Function) WithName(name string) *Function { f.Name = name; return f }
func (f *Function) WithDescription(d string) *Function { f.Description = d; return f }
func (f *Function) WithType(t FunctionType) *Function { f.Type = t; return f }

// WithFunctionType is the Milvus alias.
func (f *Function) WithFunctionType(t FunctionType) *Function { f.Type = t; return f }

func (f *Function) WithInputFields(names ...string) *Function {
	f.InputFieldNames = names
	return f
}

func (f *Function) WithOutputFields(names ...string) *Function {
	f.OutputFieldNames = names
	return f
}

func (f *Function) WithParam(key string, value any) *Function {
	if f.Params == nil {
		f.Params = make(map[string]any)
	}
	f.Params[key] = value
	return f
}
