//! WAL compaction: rewrite the log from current in-memory state.

use crate::wal::{BulkPoint, WalEntry};

/// Build a minimal WAL that recreates the current engine state.
pub fn export_state_to_wal(
    collections: &[(vectordb_core::CollectionConfig, Vec<BulkPoint>)],
) -> Vec<WalEntry> {
    let mut entries = Vec::new();
    for (config, points) in collections {
        entries.push(WalEntry::CreateCollection {
            config: config.clone(),
        });
        if points.is_empty() {
            continue;
        }
        for chunk in points.chunks(500) {
            entries.push(WalEntry::BulkUpsert {
                collection: config.name.clone(),
                points: chunk.to_vec(),
            });
        }
    }
    entries
}
