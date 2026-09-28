use std::future::Future;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::ContentError;
use crate::hashing::ContentHasher;
use crate::identity::ContentId;

/// Default nominal chunk size: 256 KiB (262,144 bytes).
///
/// **Why 256 KiB is optimal for FIELDNET MVP:**
/// 1. **Resource Constraints:** Allows low-memory nodes (embedded devices, Android phones)
///    to verify, buffer, and relay chunks without triggering OOM or excessive heap churn.
/// 2. **Mesh Wireless Resilience:** In lossy, high-latency peer-to-peer ad-hoc topologies,
///    a dropped packet only requires retransmitting 256 KiB rather than several megabytes.
/// 3. **Sub-millisecond Cryptographic Verification:** Hashing 256 KiB via SHA-256 takes
///    a fraction of a millisecond on modern arm64/x86_64 CPUs.
/// 4. **Merkle Fan-Out:** Generates compact Merkle trees (e.g. 4 chunks for 1 MiB, 40 for 10 MiB,
///    400 for 100 MiB) that easily fit inside DTN bundle payloads and transport control frames.
pub const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Minimum allowable chunk size: 1 byte (or 0 for empty content).
pub const MIN_CHUNK_SIZE: usize = 1;

/// Maximum allowable chunk size: 1 MiB (1,048,576 bytes).
pub const MAX_CHUNK_SIZE: usize = 1024 * 1024;

/// Authoritative chunking algorithm identifier string for fixed 256KB chunking.
pub const CHUNKING_ALGORITHM_FIXED_256K: &str = "FIXED_256KB";

/// Extensible abstraction for splitting raw data streams into discrete content chunks.
pub trait Chunker: Send + Sync {
    /// Nominal chunk size in bytes produced by this chunker.
    fn chunk_size(&self) -> usize;

    /// Algorithm name descriptor.
    fn algorithm_name(&self) -> &'static str;

    /// Chunks an in-memory byte slice into a list of `(chunk_index, byte_offset, chunk_bytes)`.
    fn chunk_slice(&self, data: &[u8]) -> Vec<(u32, u64, Vec<u8>)>;
}

/// Fixed-size chunking engine implementing the `Chunker` trait.
#[derive(Clone, Copy, Debug)]
pub struct FixedSizeChunker {
    chunk_size: usize,
}

impl Default for FixedSizeChunker {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl FixedSizeChunker {
    /// Constructs a `FixedSizeChunker` with a customized chunk size within `[MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]`.
    pub fn new(chunk_size: usize) -> Result<Self, ContentError> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(ContentError::StorageFailure(format!(
                "Invalid chunk size {}: must be between {} and {} bytes",
                chunk_size, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE
            )));
        }
        Ok(Self { chunk_size })
    }

    /// Returns the nominal chunk size in bytes.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Streams data from an async reader without loading the entire object into memory.
    ///
    /// For each completed chunk, invokes `handler(chunk_index, byte_offset, chunk_bytes)`.
    /// Simultaneously calculates the unchunked overall SHA-256 `ContentId`.
    ///
    /// Returns `(content_id, total_bytes, total_chunks)`.
    pub async fn chunk_stream<R, F, Fut>(
        &self,
        reader: &mut R,
        mut handler: F,
    ) -> Result<(ContentId, u64, u32), ContentError>
    where
        R: AsyncRead + Unpin,
        F: FnMut(u32, u64, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), ContentError>>,
    {
        let mut overall_hasher = ContentHasher::new();
        let mut total_bytes: u64 = 0;
        let mut chunk_index: u32 = 0;

        let mut chunk_buf = vec![0u8; self.chunk_size];
        let mut chunk_fill = 0;

        loop {
            let n = reader
                .read(&mut chunk_buf[chunk_fill..])
                .await
                .map_err(|e| {
                    ContentError::IOError(format!("Read failure during streaming chunking: {}", e))
                })?;

            if n == 0 {
                // EOF reached
                if chunk_fill > 0 || total_bytes == 0 {
                    let chunk_slice = &chunk_buf[..chunk_fill];
                    overall_hasher.update(chunk_slice);
                    let offset = total_bytes;
                    total_bytes += chunk_fill as u64;

                    handler(chunk_index, offset, chunk_slice.to_vec()).await?;
                    chunk_index += 1;
                }
                break;
            }

            chunk_fill += n;
            if chunk_fill == self.chunk_size {
                let chunk_slice = &chunk_buf[..chunk_fill];
                overall_hasher.update(chunk_slice);
                let offset = total_bytes;
                total_bytes += chunk_fill as u64;

                handler(chunk_index, offset, chunk_slice.to_vec()).await?;
                chunk_index += 1;
                chunk_fill = 0;
            }
        }

        let content_id = overall_hasher.finalize();
        Ok((content_id, total_bytes, chunk_index))
    }
}

impl Chunker for FixedSizeChunker {
    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn algorithm_name(&self) -> &'static str {
        CHUNKING_ALGORITHM_FIXED_256K
    }

    fn chunk_slice(&self, data: &[u8]) -> Vec<(u32, u64, Vec<u8>)> {
        if data.is_empty() {
            return vec![(0, 0, Vec::new())];
        }

        let mut chunks = Vec::new();
        let mut offset = 0;
        let mut index = 0;

        while offset < data.len() {
            let end = (offset + self.chunk_size).min(data.len());
            chunks.push((index, offset as u64, data[offset..end].to_vec()));
            offset = end;
            index += 1;
        }

        chunks
    }
}
