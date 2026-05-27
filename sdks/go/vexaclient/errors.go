package vexaclient

import "fmt"

// VexaError is returned when the gateway responds with a non-success status.
type VexaError struct {
	StatusCode int
	Message    string
}

func (e *VexaError) Error() string {
	return fmt.Sprintf("vexadb: %d %s", e.StatusCode, e.Message)
}
