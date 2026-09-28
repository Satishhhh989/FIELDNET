use sha2::{Digest, Sha256};
use std::io::Read;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ContentError;
use crate::identity::ContentId;

/// Default I/O buffer size (64 KiB) for streaming hashing operations.
pub const STREAM_BUFFER_SIZE: usize = 64 * 1024;

/// Authoritative streaming SHA-256 hasher for FIELDNET content identity.
#[derive(Clone)]
pub struct ContentHasher {
    hasher: Sha256,
}

impl Default for ContentHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentHasher {
    /// Creates a new instance of the content hasher.
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    /// Appends arbitrary byte chunks to the running digest.
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Consumes the hasher and returns the finalized `ContentId`.
    pub fn finalize(self) -> ContentId {
        let digest = self.hasher.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&digest);
        ContentId::from_bytes(bytes)
    }

    /// Finalizes the current digest, resets state, and returns the computed `ContentId`.
    pub fn finalize_reset(&mut self) -> ContentId {
        let digest = self.hasher.finalize_reset();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&digest);
        ContentId::from_bytes(bytes)
    }
}

/// Computes the `ContentId` of an in-memory byte slice using SHA-256.
pub fn hash_bytes(data: &[u8]) -> ContentId {
    let mut hasher = ContentHasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Computes the `ContentId` of a synchronous `std::io::Read` source without loading it entirely into memory.
pub fn hash_reader<R: Read>(reader: &mut R) -> Result<ContentId, ContentError> {
    let mut hasher = ContentHasher::new();
    let mut buffer = vec![0u8; STREAM_BUFFER_SIZE];

    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => hasher.update(&buffer[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(ContentError::IOError(format!(
                    "Read failure during hashing: {}",
                    e
                )))
            }
        }
    }

    Ok(hasher.finalize())
}

/// Computes the `ContentId` of an asynchronous `tokio::io::AsyncRead` stream.
pub async fn hash_stream<R: AsyncRead + Unpin>(reader: &mut R) -> Result<ContentId, ContentError> {
    let mut hasher = ContentHasher::new();
    let mut buffer = vec![0u8; STREAM_BUFFER_SIZE];

    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => hasher.update(&buffer[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(ContentError::IOError(format!(
                    "Async read failure during hashing: {}",
                    e
                )))
            }
        }
    }

    Ok(hasher.finalize())
}

/// Streams data from `reader` into `writer`, concurrently computing the SHA-256 `ContentId`
/// and returning `(ContentId, total_bytes_written)`.
pub async fn hash_and_copy_stream<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
) -> Result<(ContentId, u64), ContentError> {
    let mut hasher = ContentHasher::new();
    let mut buffer = vec![0u8; STREAM_BUFFER_SIZE];
    let mut total_bytes: u64 = 0;

    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                hasher.update(&buffer[..n]);
                writer.write_all(&buffer[..n]).await.map_err(|e| {
                    ContentError::IOError(format!("Write failure during staging: {}", e))
                })?;
                total_bytes += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                return Err(ContentError::IOError(format!(
                    "Async read failure during staging: {}",
                    e
                )))
            }
        }
    }

    writer
        .flush()
        .await
        .map_err(|e| ContentError::IOError(format!("Flush failure during staging: {}", e)))?;

    Ok((hasher.finalize(), total_bytes))
}
