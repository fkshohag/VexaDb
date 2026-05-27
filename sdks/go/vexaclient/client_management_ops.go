package vexaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/vectordb/vectordb/sdks/go/entity"
	"github.com/vectordb/vectordb/sdks/go/index"
)

// ---- Index lifecycle (Milvus parity) ------------------------------------

// CreateIndex builds an index on `option.FieldName`. For vector fields
// (HNSW / AUTOINDEX / FLAT) VexaDb triggers a reindex; for scalar fields
// (Inverted / Sorted / Bitmap / Trie) the engine adds a payload index.
// Returns a CreateIndexTask whose Await blocks until the build reports
// `Finished`.
func (c *Client) CreateIndex(ctx context.Context, option *CreateIndexOption) (*CreateIndexTask, error) {
	if option == nil {
		return nil, fmt.Errorf("nil CreateIndexOption")
	}
	if option.CollectionName == "" || option.FieldName == "" || option.Index == nil {
		return nil, fmt.Errorf("CreateIndexOption requires collection, field and index")
	}
	kind, ok := index.VexaIndexKind(option.Index.IndexType())
	if !ok {
		// Forward-compat: send the raw index type as `kind` so the gateway
		// can reject it cleanly. Stops us from silently dropping unknown
		// indexes on the client.
		kind = string(option.Index.IndexType())
	}
	body := map[string]any{
		"field":  option.FieldName,
		"kind":   kind,
		"params": option.Index.Params(),
	}
	if option.IndexName != "" {
		body["index_name"] = option.IndexName
	}
	if m, ok := option.Index.Params()["metric_type"]; ok {
		body["metric"] = m
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/indexes"
	if err := c.do(ctx, http.MethodPost, path, body, nil); err != nil {
		return nil, err
	}
	idxName := option.IndexName
	if idxName == "" {
		idxName = option.FieldName
	}
	return &CreateIndexTask{
		client:     c,
		collection: option.CollectionName,
		indexName:  idxName,
		field:      option.FieldName,
	}, nil
}

// DropIndex removes an index by name. Idempotent on unknown names.
func (c *Client) DropIndex(ctx context.Context, option *DropIndexOption) error {
	if option == nil || option.CollectionName == "" || option.IndexName == "" {
		return fmt.Errorf("DropIndexOption requires collection and indexName")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/indexes/" + url.PathEscape(option.IndexName)
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

// DescribeIndex returns the build state, row counts, and parameter map
// for an index.
func (c *Client) DescribeIndex(ctx context.Context, option *DescribeIndexOption) (entity.IndexDescription, error) {
	if option == nil || option.CollectionName == "" || option.IndexName == "" {
		return entity.IndexDescription{}, fmt.Errorf("DescribeIndexOption requires collection and indexName")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/indexes/" + url.PathEscape(option.IndexName)
	var raw struct {
		Name             string            `json:"name"`
		Field            string            `json:"field"`
		Kind             string            `json:"kind"`
		Scope            string            `json:"scope"`
		Metric           string            `json:"metric"`
		Params           map[string]string `json:"params"`
		State            string            `json:"state"`
		TotalRows        int64             `json:"total_rows"`
		IndexedRows      int64             `json:"indexed_rows"`
		PendingIndexRows int64             `json:"pending_index_rows"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return entity.IndexDescription{}, err
	}
	params := raw.Params
	if params == nil {
		params = map[string]string{}
	}
	if raw.Metric != "" {
		params["metric_type"] = raw.Metric
	}
	return entity.IndexDescription{
		Index:            index.NewGenericIndex(raw.Name, map[string]string{"index_type": raw.Kind}),
		Field:            raw.Field,
		IndexName:        raw.Name,
		Scope:            raw.Scope,
		State:            parseIndexState(raw.State),
		PendingIndexRows: raw.PendingIndexRows,
		TotalRows:        raw.TotalRows,
		IndexedRows:      raw.IndexedRows,
		Params:           params,
	}, nil
}

// ListIndexes returns the index names defined on a collection. When
// `WithFieldName` is set only indexes attached to that field are
// returned. Milvus parity: `([]string, error)`.
func (c *Client) ListIndexes(ctx context.Context, option *ListIndexOption) ([]string, error) {
	if option == nil || option.CollectionName == "" {
		return nil, fmt.Errorf("ListIndexOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/indexes"
	var raw []struct {
		Name  string `json:"name"`
		Field string `json:"field"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if option.FieldName != "" && r.Field != option.FieldName {
			continue
		}
		out = append(out, r.Name)
	}
	return out, nil
}

// AlterIndexProperties merges custom properties into an index.
func (c *Client) AlterIndexProperties(ctx context.Context, option *AlterIndexPropertiesOption) error {
	if option == nil || option.CollectionName == "" || option.IndexName == "" {
		return fmt.Errorf("AlterIndexPropertiesOption requires collection and indexName")
	}
	if len(option.Properties) == 0 {
		return nil
	}
	body := map[string]any{"set": option.Properties}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/indexes/" + url.PathEscape(option.IndexName) + "/properties"
	return c.do(ctx, http.MethodPatch, path, body, nil)
}

// DropIndexProperties removes specified property keys from an index.
func (c *Client) DropIndexProperties(ctx context.Context, option *DropIndexPropertiesOption) error {
	if option == nil || option.CollectionName == "" || option.IndexName == "" {
		return fmt.Errorf("DropIndexPropertiesOption requires collection and indexName")
	}
	if len(option.Keys) == 0 {
		return nil
	}
	body := map[string]any{"keys": option.Keys}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) +
		"/indexes/" + url.PathEscape(option.IndexName) + "/properties"
	return c.do(ctx, http.MethodDelete, path, body, nil)
}

// ---- Load / Release / Refresh (Milvus parity) ---------------------------

// LoadCollection ensures `option.CollectionName` is resident in memory.
// VexaDb keeps collections always-loaded, so the call returns a LoadTask
// that completes immediately.
func (c *Client) LoadCollection(ctx context.Context, option *LoadCollectionOption) (LoadTask, error) {
	if option == nil || option.CollectionName == "" {
		return LoadTask{}, fmt.Errorf("LoadCollectionOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/load"
	if err := c.do(ctx, http.MethodPost, path, nil, nil); err != nil {
		return LoadTask{}, err
	}
	if option.Refresh {
		// "Load with refresh" → trigger a reindex so the HNSW reflects
		// any pending writes before Await returns.
		_ = c.do(ctx, http.MethodPost,
			"/v1/collections/"+url.PathEscape(option.CollectionName)+"/refresh-load",
			nil, nil)
	}
	return LoadTask{client: c, collection: option.CollectionName}, nil
}

// LoadPartitions — Milvus parity. VexaDb does not partition; the
// partition names are accepted and the call is treated as
// LoadCollection.
func (c *Client) LoadPartitions(ctx context.Context, option *LoadPartitionsOption) (LoadTask, error) {
	if option == nil || option.CollectionName == "" {
		return LoadTask{}, fmt.Errorf("LoadPartitionsOption.CollectionName is required")
	}
	return c.LoadCollection(ctx, &LoadCollectionOption{
		CollectionName:       option.CollectionName,
		Replica:              option.Replica,
		ResourceGroups:       option.ResourceGroups,
		LoadFields:           option.LoadFields,
		SkipLoadDynamicField: option.SkipLoadDynamicField,
		Refresh:              option.Refresh,
	})
}

// ReleaseCollection — Milvus parity. No-op on VexaDb but validates the
// collection exists so callers see a 404 on bad input.
func (c *Client) ReleaseCollection(ctx context.Context, option *ReleaseCollectionOption) error {
	if option == nil || option.CollectionName == "" {
		return fmt.Errorf("ReleaseCollectionOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/release"
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// ReleasePartitions — Milvus parity. Falls back to ReleaseCollection.
func (c *Client) ReleasePartitions(ctx context.Context, option *ReleasePartitionsOption) error {
	if option == nil || option.CollectionName == "" {
		return fmt.Errorf("ReleasePartitionsOption.CollectionName is required")
	}
	return c.ReleaseCollection(ctx, &ReleaseCollectionOption{CollectionName: option.CollectionName})
}

// GetLoadState — Milvus parity.
func (c *Client) GetLoadState(ctx context.Context, option *GetLoadStateOption) (entity.LoadState, error) {
	if option == nil || option.CollectionName == "" {
		return entity.LoadState{}, fmt.Errorf("GetLoadStateOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/load-state"
	var raw struct {
		State    string `json:"state"`
		Progress int64  `json:"progress"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return entity.LoadState{}, err
	}
	return entity.LoadState{
		State:    parseLoadState(raw.State),
		Progress: raw.Progress,
	}, nil
}

// RefreshLoad — Milvus parity. Triggers a reindex so newly inserted
// data becomes searchable, and returns a LoadTask that completes once
// the reindex finishes.
func (c *Client) RefreshLoad(ctx context.Context, option *RefreshLoadOption) (LoadTask, error) {
	if option == nil || option.CollectionName == "" {
		return LoadTask{}, fmt.Errorf("RefreshLoadOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/refresh-load"
	if err := c.do(ctx, http.MethodPost, path, nil, nil); err != nil {
		return LoadTask{}, err
	}
	return LoadTask{client: c, collection: option.CollectionName}, nil
}

// ---- Flush / Compact (Milvus parity) ------------------------------------

// Flush — Milvus parity. VexaDb fsyncs the WAL inline so the returned
// FlushTask is already complete; callers can still invoke Await for
// API-shape symmetry.
func (c *Client) Flush(ctx context.Context, option *FlushOption) (*FlushTask, error) {
	if option == nil || option.CollectionName == "" {
		return nil, fmt.Errorf("FlushOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/flush"
	var raw struct {
		Collection        string   `json:"collection"`
		FlushTsMs         uint64   `json:"flush_ts_ms"`
		SegmentIDs        []uint64 `json:"segment_ids"`
		FlushedSegmentIDs []uint64 `json:"flushed_segment_ids"`
		WALEntries        uint64   `json:"wal_entries"`
	}
	if err := c.do(ctx, http.MethodPost, path, nil, &raw); err != nil {
		return nil, err
	}
	return &FlushTask{
		collection: raw.Collection,
		stats: entity.FlushStats{
			SegmentIDs:        raw.SegmentIDs,
			FlushedSegmentIDs: raw.FlushedSegmentIDs,
			FlushTs:           raw.FlushTsMs,
		},
	}, nil
}

// Compact — Milvus parity. Returns the compaction ID as int64 (Milvus's
// type); poll with [Client.GetCompactionState].
func (c *Client) Compact(ctx context.Context, option *CompactOption) (int64, error) {
	if option == nil || option.CollectionName == "" {
		return 0, fmt.Errorf("CompactOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/compact"
	var raw struct {
		CompactionID uint64 `json:"compaction_id"`
	}
	if err := c.do(ctx, http.MethodPost, path, nil, &raw); err != nil {
		return 0, err
	}
	return int64(raw.CompactionID), nil
}

// GetCompactionState — Milvus parity.
func (c *Client) GetCompactionState(ctx context.Context, option *GetCompactionStateOption) (entity.CompactionInfo, error) {
	if option == nil {
		return entity.CompactionInfo{}, fmt.Errorf("nil GetCompactionStateOption")
	}
	path := "/v1/compactions/" + strconv.FormatUint(option.CompactionID, 10)
	var raw struct {
		CompactionID  uint64          `json:"compaction_id"`
		Collection    string          `json:"collection"`
		State         string          `json:"state"`
		EntriesBefore uint64          `json:"entries_before"`
		EntriesAfter  uint64          `json:"entries_after"`
		StartedMs     uint64          `json:"started_ms"`
		FinishedMs    uint64          `json:"finished_ms"`
		Error         json.RawMessage `json:"error"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return entity.CompactionInfo{}, err
	}
	var errStr string
	if len(raw.Error) > 0 && string(raw.Error) != "null" {
		_ = json.Unmarshal(raw.Error, &errStr)
	}
	return entity.CompactionInfo{
		CompactionID:  raw.CompactionID,
		Collection:    raw.Collection,
		State:         parseCompactionState(raw.State),
		EntriesBefore: raw.EntriesBefore,
		EntriesAfter:  raw.EntriesAfter,
		StartedMs:     raw.StartedMs,
		FinishedMs:    raw.FinishedMs,
		Error:         errStr,
	}, nil
}

// ---- Segments (Milvus parity) -------------------------------------------

// GetPersistentSegmentInfo — Milvus parity.
func (c *Client) GetPersistentSegmentInfo(ctx context.Context, option *GetPersistentSegmentInfoOption) ([]*entity.Segment, error) {
	if option == nil || option.CollectionName == "" {
		return nil, fmt.Errorf("GetPersistentSegmentInfoOption.CollectionName is required")
	}
	path := "/v1/collections/" + url.PathEscape(option.CollectionName) + "/segments"
	var raw []struct {
		ID         uint64 `json:"id"`
		Collection string `json:"collection"`
		NumRows    uint64 `json:"num_rows"`
		State      string `json:"state"`
		Source     string `json:"source"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &raw); err != nil {
		return nil, err
	}
	out := make([]*entity.Segment, 0, len(raw))
	for _, r := range raw {
		out = append(out, &entity.Segment{
			ID:      int64(r.ID),
			NumRows: int64(r.NumRows),
			State:   parseSegmentState(r.State),
			Source:  r.Source,
		})
	}
	return out, nil
}

// ---- enum parsing helpers -----------------------------------------------

func parseIndexState(s string) index.IndexState {
	switch s {
	case "InProgress":
		return index.IndexStateInProgress
	case "Finished":
		return index.IndexStateFinished
	case "Failed":
		return index.IndexStateFailed
	case "Retry":
		return index.IndexStateRetry
	default:
		return index.IndexStateUnspecified
	}
}

func parseLoadState(s string) entity.LoadStateCode {
	switch s {
	case "Loaded":
		return entity.LoadStateLoaded
	case "Loading":
		return entity.LoadStateLoading
	case "NotLoad":
		return entity.LoadStateNotLoad
	case "NotExist":
		return entity.LoadStateNotExist
	default:
		return entity.LoadStateUnspecified
	}
}

func parseCompactionState(s string) entity.CompactionState {
	switch s {
	case "Running":
		return entity.CompactionStateRunning
	case "Completed":
		return entity.CompactionStateCompleted
	case "Failed":
		return entity.CompactionStateFailed
	default:
		return entity.CompactionStateUnspecified
	}
}

func parseSegmentState(s string) entity.SegmentState {
	switch s {
	case "Growing":
		return entity.SegmentStateGrowing
	case "Sealed":
		return entity.SegmentStateSealed
	case "Flushed":
		return entity.SegmentStateFlushed
	default:
		return entity.SegmentStateUnspecified
	}
}
