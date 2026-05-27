package vexaclient

import "fmt"

// stringify converts an arbitrary value to its canonical string form, used
// when serializing properties into the string-keyed map the server stores.
//
// Mirrors the conversion Milvus performs on `WithProperty(key, value any)`.
func stringify(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool, int, int32, int64, uint, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
