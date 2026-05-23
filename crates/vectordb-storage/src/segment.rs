use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use vectordb_core::types::Vector;

#[derive(Debug, Error)]
pub enum SegmentError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid segment: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, SegmentError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentHeader {
    pub dimension: usize,
    pub count: usize,
}

/// Memory-mapped dense vector segment for efficient bulk reads.
pub struct VectorSegment {
    path: PathBuf,
    dimension: usize,
    count: usize,
    mmap: MmapMut,
}

impl VectorSegment {
    pub fn create(path: impl AsRef<Path>, dimension: usize, capacity: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes_per_vector = dimension * std::mem::size_of::<f32>();
        let file_size = std::mem::size_of::<SegmentHeader>() + capacity * bytes_per_vector;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(file_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let header = SegmentHeader {
            dimension,
            count: 0,
        };
        write_header(&mut mmap, &header)?;

        Ok(Self {
            path,
            dimension,
            count: 0,
            mmap,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let header = read_header(&mmap)?;
        Ok(Self {
            path,
            dimension: header.dimension,
            count: header.count,
            mmap,
        })
    }

    pub fn append_vector(&mut self, vector: &Vector) -> Result<usize> {
        if vector.dim() != self.dimension {
            return Err(SegmentError::Invalid(format!(
                "dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.dim()
            )));
        }
        let offset = vector_offset(self.dimension, self.count);
        let bytes = unsafe {
            std::slice::from_raw_parts(
                vector.values.as_ptr() as *const u8,
                vector.dim() * std::mem::size_of::<f32>(),
            )
        };
        if offset + bytes.len() > self.mmap.len() {
            return Err(SegmentError::Invalid("segment capacity exceeded".into()));
        }
        self.mmap[offset..offset + bytes.len()].copy_from_slice(bytes);
        self.count += 1;
        let header = SegmentHeader {
            dimension: self.dimension,
            count: self.count,
        };
        write_header(&mut self.mmap, &header)?;
        Ok(self.count - 1)
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn vector_offset(dimension: usize, index: usize) -> usize {
    std::mem::size_of::<SegmentHeader>() + index * dimension * std::mem::size_of::<f32>()
}

fn read_header(mmap: &[u8]) -> Result<SegmentHeader> {
    if mmap.len() < std::mem::size_of::<SegmentHeader>() {
        return Err(SegmentError::Invalid("segment too small".into()));
    }
    let header: SegmentHeader = bincode::deserialize(&mmap[..std::mem::size_of::<SegmentHeader>()])
        .map_err(|e| SegmentError::Invalid(e.to_string()))?;
    Ok(header)
}

fn write_header(mmap: &mut MmapMut, header: &SegmentHeader) -> Result<()> {
    let bytes = bincode::serialize(header).map_err(|e| SegmentError::Invalid(e.to_string()))?;
    mmap[..bytes.len()].copy_from_slice(&bytes);
    Ok(())
}
