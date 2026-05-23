use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use vectordb_core::{SparseVector, Vector};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] bincode::Error),
}

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    CreateCollection {
        config: vectordb_core::CollectionConfig,
    },
    DeleteCollection {
        name: String,
    },
    Upsert {
        collection: String,
        id: String,
        vector: Vector,
        #[serde(default)]
        payload: Option<Vec<u8>>,
        #[serde(default)]
        sparse: Option<SparseVector>,
    },
    Delete {
        collection: String,
        id: String,
    },
    /// Batch upsert for bulk import (single WAL record).
    BulkUpsert {
        collection: String,
        points: Vec<BulkPoint>,
    },
    /// Marker written after snapshot + compaction.
    Checkpoint {
        snapshot_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkPoint {
    pub id: String,
    pub vector: Vector,
    #[serde(default)]
    pub payload: Option<Vec<u8>>,
    #[serde(default)]
    pub sparse: Option<SparseVector>,
}

/// Append-only write-ahead log for crash recovery.
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let bytes = bincode::serialize(entry)?;
        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            entries.push(bincode::deserialize(&buf)?);
        }
        Ok(entries)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replace the WAL file with a compacted sequence of entries.
    pub fn rewrite(&mut self, entries: &[WalEntry]) -> Result<()> {
        let path = self.path.clone();
        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&path)?,
            ),
        ));
        for entry in entries {
            self.append(entry)?;
        }
        Ok(())
    }

    pub fn append_checkpoint(&mut self, snapshot_id: impl Into<String>) -> Result<()> {
        self.append(&WalEntry::Checkpoint {
            snapshot_id: snapshot_id.into(),
        })
    }
}
