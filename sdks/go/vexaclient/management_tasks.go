package vexaclient

import (
	"context"
	"fmt"
	"time"

	"github.com/vectordb/vectordb/sdks/go/entity"
)

// awaitTask is the shared poll helper used by every async management task
// in this file. It calls `poll` at `interval` until it reports done==true,
// an error occurs, or the context is cancelled.
//
// Polling is exposed as a strategy parameter so tests can drive the task
// to completion deterministically (`interval=0`).
func awaitTask(ctx context.Context, interval time.Duration, poll func(context.Context) (bool, error)) error {
	if interval <= 0 {
		// One immediate check, then return whatever the strategy says.
		done, err := poll(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// Fall through to the timed loop with a sane default cadence.
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		done, err := poll(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// ---- CreateIndexTask (Milvus parity) ------------------------------------

// CreateIndexTask is returned by Client.CreateIndex. Call Await to block
// until the index build reaches a terminal state. VexaDb builds indexes
// synchronously, so Await returns immediately for a freshly-created task.
type CreateIndexTask struct {
	client     *Client
	collection string
	indexName  string
	field      string
}

// Await blocks until the index reaches `Finished` state or the context
// is cancelled. Returns an error when the build transitions to `Failed`.
func (t *CreateIndexTask) Await(ctx context.Context) error {
	if t == nil || t.client == nil {
		return fmt.Errorf("nil CreateIndexTask")
	}
	return awaitTask(ctx, 200*time.Millisecond, func(ctx context.Context) (bool, error) {
		desc, err := t.client.DescribeIndex(ctx, NewDescribeIndexOption(t.collection, t.indexName))
		if err != nil {
			return false, err
		}
		switch desc.State {
		case 2: // index.IndexStateFinished
			return true, nil
		case 3: // index.IndexStateFailed
			return false, fmt.Errorf("index %q on %q failed to build", t.indexName, t.collection)
		default:
			return false, nil
		}
	})
}

// ---- LoadTask (Milvus parity) -------------------------------------------

// LoadTask is returned by LoadCollection / LoadPartitions / RefreshLoad.
// VexaDb keeps collections memory-resident, so Await is effectively a
// no-op success when the collection exists.
type LoadTask struct {
	client     *Client
	collection string
}

// Await blocks until the collection reports `Loaded`. Cancellable via ctx.
func (t LoadTask) Await(ctx context.Context) error {
	if t.client == nil {
		return fmt.Errorf("nil LoadTask")
	}
	return awaitTask(ctx, 100*time.Millisecond, func(ctx context.Context) (bool, error) {
		state, err := t.client.GetLoadState(ctx, NewGetLoadStateOption(t.collection))
		if err != nil {
			return false, err
		}
		switch state.State {
		case entity.LoadStateLoaded:
			return true, nil
		case entity.LoadStateNotExist:
			return false, fmt.Errorf("collection %q does not exist", t.collection)
		default:
			return false, nil
		}
	})
}

// ---- FlushTask (Milvus parity) ------------------------------------------

// FlushTask is returned by Client.Flush. Because VexaDb fsyncs the WAL
// inline during Flush, the task is created in a Completed state.
type FlushTask struct {
	collection string
	stats      entity.FlushStats
}

// Await is a no-op for VexaDb: the WAL has already been fsync'd when the
// task was created. Honours ctx cancellation for symmetry with Milvus.
func (t *FlushTask) Await(ctx context.Context) error {
	if t == nil {
		return fmt.Errorf("nil FlushTask")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// GetFlushStats returns the segment IDs and timestamp captured by the
// flush. Matches Milvus's tuple-return shape; `channelCheckpoints` is
// always nil for VexaDb.
func (t *FlushTask) GetFlushStats() (segIDs []uint64, flushSegIDs []uint64, flushTs uint64, channelCheckpoints map[string]uint64) {
	if t == nil {
		return nil, nil, 0, nil
	}
	return t.stats.SegmentIDs, t.stats.FlushedSegmentIDs, t.stats.FlushTs, t.stats.ChannelCheckpoints
}

// Collection returns the FQ collection name the flush targeted.
func (t *FlushTask) Collection() string {
	if t == nil {
		return ""
	}
	return t.collection
}
