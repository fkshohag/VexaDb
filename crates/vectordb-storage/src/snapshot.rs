//! Filesystem snapshots: copy WAL + RocksDB metadata to a labelled directory.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::engine::{EngineError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: String,
    pub created_at_ms: u128,
    pub data_dir: PathBuf,
}

pub struct SnapshotManager {
    data_dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    pub fn snapshots_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    pub fn create(&self) -> Result<SnapshotMeta> {
        fs::create_dir_all(self.snapshots_dir())?;
        let id = format!(
            "snap-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );
        let dst = self.snapshots_dir().join(&id);
        fs::create_dir_all(&dst)?;

        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == "snapshots" {
                continue;
            }
            let from = entry.path();
            let to = dst.join(&name);
            copy_recursive(&from, &to)?;
        }

        let meta = SnapshotMeta {
            id: id.clone(),
            created_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            data_dir: dst.clone(),
        };
        fs::write(
            dst.join("manifest.json"),
            serde_json::to_vec_pretty(&meta).map_err(|e| EngineError::Rocks(e.to_string()))?,
        )?;
        Ok(meta)
    }

    pub fn list(&self) -> Result<Vec<SnapshotMeta>> {
        let dir = self.snapshots_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let manifest = entry.path().join("manifest.json");
            if let Ok(bytes) = fs::read(&manifest) {
                if let Ok(meta) = serde_json::from_slice::<SnapshotMeta>(&bytes) {
                    out.push(meta);
                }
            }
        }
        out.sort_by_key(|m| std::cmp::Reverse(m.created_at_ms));
        Ok(out)
    }

    pub fn delete(&self, id: &str) -> Result<()> {
        let path = self.snapshots_dir().join(id);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }
}

/// Relative paths of durable shard files inside a snapshot directory
/// (`wal.log`, files under `meta/`). Skips `manifest.json` and `snapshots/`.
pub fn collect_payload_files(snapshot_root: &Path) -> Result<Vec<(PathBuf, String)>> {
    let mut out = Vec::new();
    let wal = snapshot_root.join("wal.log");
    if wal.is_file() {
        out.push((wal, "wal.log".into()));
    }
    let meta = snapshot_root.join("meta");
    if meta.is_dir() {
        collect_files_recursive(&meta, &meta, &mut out)?;
    }
    Ok(out)
}

fn collect_files_recursive(
    base: &Path,
    dir: &Path,
    out: &mut Vec<(PathBuf, String)>,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(base, &path, out)?;
        } else {
            let rel = path
                .strip_prefix(base)
                .map_err(|_| EngineError::Rocks("path prefix".into()))?;
            let name = format!("meta/{}", rel.to_string_lossy().replace('\\', "/"));
            out.push((path, name));
        }
    }
    Ok(())
}

/// Copy `wal.log` and `meta/` from a snapshot payload tree into a live data
/// directory (overwriting existing).
pub fn install_payload_into_data_dir(payload_root: &Path, data_dir: &Path) -> Result<()> {
    let wal_src = payload_root.join("wal.log");
    if wal_src.is_file() {
        copy_recursive(&wal_src, &data_dir.join("wal.log"))?;
    }
    let meta_src = payload_root.join("meta");
    if meta_src.is_dir() {
        let meta_dst = data_dir.join("meta");
        if meta_dst.exists() {
            fs::remove_dir_all(&meta_dst)?;
        }
        copy_recursive(&meta_src, &meta_dst)?;
    }
    Ok(())
}

pub(crate) fn copy_recursive(src: &Path, dst: &Path) -> Result<()> {
    if src.is_dir() {
        fs::create_dir_all(dst)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            copy_recursive(&entry.path(), &dst.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src, dst)?;
    }
    Ok(())
}
