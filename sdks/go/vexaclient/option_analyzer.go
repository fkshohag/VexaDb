package vexaclient

// RunAnalyzerOption configures RunAnalyzer (Milvus parity).
type RunAnalyzerOption struct {
	Text             []string
	AnalyzerParams   map[string]any
	AnalyzerParamsStr string
	AnalyzerNames    []string
	Collection       string
	Field            string
	Detail            bool
	Hash              bool
}

// NewRunAnalyzerOption accepts one or more input strings.
func NewRunAnalyzerOption(text ...string) *RunAnalyzerOption {
	return &RunAnalyzerOption{Text: text, Detail: true, Hash: true}
}

// WithAnalyzerParams accepts a structured params map (e.g. tokenizer,
// stopword filter). The map is JSON-encoded by the client.
func (o *RunAnalyzerOption) WithAnalyzerParams(params map[string]any) *RunAnalyzerOption {
	o.AnalyzerParams = params
	return o
}

func (o *RunAnalyzerOption) WithAnalyzerParamsStr(params string) *RunAnalyzerOption {
	o.AnalyzerParamsStr = params
	return o
}

func (o *RunAnalyzerOption) WithDetail() *RunAnalyzerOption {
	o.Detail = true
	return o
}

func (o *RunAnalyzerOption) WithHash() *RunAnalyzerOption {
	o.Hash = true
	return o
}

func (o *RunAnalyzerOption) WithField(collection, field string) *RunAnalyzerOption {
	o.Collection = collection
	o.Field = field
	return o
}

func (o *RunAnalyzerOption) WithAnalyzerName(names ...string) *RunAnalyzerOption {
	o.AnalyzerNames = names
	return o
}
