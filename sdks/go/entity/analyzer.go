package entity

// AnalyzerToken is one token produced by `Client.RunAnalyzer` (Milvus
// parity). The hash is a 64-bit FNV-1a value over the token text and is
// deterministic across runs.
type AnalyzerToken struct {
	Text        string `json:"token"`
	StartOffset uint64 `json:"start_offset"`
	EndOffset   uint64 `json:"end_offset"`
	Position    uint64 `json:"position"`
	Hash        uint64 `json:"hash"`
}

// AnalyzerResult is the per-input output of `Client.RunAnalyzer`.
type AnalyzerResult struct {
	Tokens []AnalyzerToken `json:"tokens"`
}
