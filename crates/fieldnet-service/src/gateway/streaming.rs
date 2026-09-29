use axum::body::Body;
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH,
    RANGE,
};
use axum::http::{HeaderMap, Response, StatusCode};
use axum::response::IntoResponse;
use fieldnet_content::{ContentId, ContentStore};
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::io::AsyncSeekExt;
use tokio_util::io::ReaderStream;

/// 64 KiB chunk buffer size for streaming content.
pub const STREAM_CHUNK_SIZE: usize = 64 * 1024;

/// Represents a parsed HTTP byte-range request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    /// Attempts to parse standard HTTP Range header value (e.g. "bytes=0-1023", "bytes=500-", "bytes=-500").
    pub fn parse(range_header: &str, total_length: u64) -> Option<Self> {
        let s = range_header.trim();
        if !s.starts_with("bytes=") {
            return None;
        }
        let spec = &s[6..].trim();
        let parts: Vec<&str> = spec.split('-').collect();
        if parts.len() != 2 {
            return None;
        }

        if parts[0].is_empty() {
            // Suffix range: "-500" -> last 500 bytes
            let suffix_len: u64 = parts[1].parse().ok()?;
            if suffix_len == 0 {
                return None;
            }
            let start = total_length.saturating_sub(suffix_len);
            let end = total_length.saturating_sub(1);
            Some(Self { start, end })
        } else if parts[1].is_empty() {
            // Prefix range: "500-" -> from 500 to end
            let start: u64 = parts[0].parse().ok()?;
            if start >= total_length {
                return None;
            }
            Some(Self {
                start,
                end: total_length.saturating_sub(1),
            })
        } else {
            // Explicit range: "0-1023"
            let start: u64 = parts[0].parse().ok()?;
            let end: u64 = parts[1].parse().ok()?;
            if start > end || start >= total_length {
                return None;
            }
            let end = end.min(total_length.saturating_sub(1));
            Some(Self { start, end })
        }
    }
}

/// Serves content from Phase 07 `ContentStore` with streaming, immutable ETag, and HTTP Range support.
pub async fn serve_content_stream(
    content_store: &Arc<ContentStore>,
    content_id: &ContentId,
    headers: &HeaderMap,
) -> Response<Body> {
    // 1. Open async file reader
    let mut file = match content_store.get_stream(content_id).await {
        Ok(f) => f,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                format!("Content not found: {}", content_id.to_hex()),
            )
                .into_response();
        }
    };

    let total_size = match file.metadata().await {
        Ok(m) => m.len(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to read content metadata: {}", e),
            )
                .into_response();
        }
    };

    let etag_value = format!("\"{}\"", content_id.to_hex());

    // 2. Check conditional If-None-Match (304 Not Modified)
    if let Some(if_none_match) = headers.get(IF_NONE_MATCH).and_then(|h| h.to_str().ok()) {
        if if_none_match == etag_value || if_none_match == "*" {
            return (
                StatusCode::NOT_MODIFIED,
                [
                    (ETAG, etag_value.clone()),
                    (
                        CACHE_CONTROL,
                        "public, max-age=31536000, immutable".to_string(),
                    ),
                ],
            )
                .into_response();
        }
    }

    let content_type = "application/octet-stream".to_string();

    // 4. Handle HTTP Range request (206 Partial Content)
    if let Some(range_hdr) = headers.get(RANGE).and_then(|h| h.to_str().ok()) {
        if let Some(range) = ByteRange::parse(range_hdr, total_size) {
            let slice_len = (range.end - range.start) + 1;

            if let Err(e) = file.seek(SeekFrom::Start(range.start)).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to seek content stream: {}", e),
                )
                    .into_response();
            }

            let limited_reader = tokio::io::AsyncReadExt::take(file, slice_len);
            let stream = ReaderStream::with_capacity(limited_reader, STREAM_CHUNK_SIZE);
            let body = Body::from_stream(stream);

            let content_range = format!("bytes {}-{}/{}", range.start, range.end, total_size);

            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_TYPE, content_type)
                .header(CONTENT_LENGTH, slice_len.to_string())
                .header(CONTENT_RANGE, content_range)
                .header(ACCEPT_RANGES, "bytes")
                .header(ETAG, etag_value)
                .header(CACHE_CONTROL, "public, max-age=31536000, immutable")
                .body(body)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        } else {
            // Range not satisfiable
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(CONTENT_RANGE, format!("bytes */{}", total_size))
                .body(Body::empty())
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
        }
    }

    // 5. Full file response (200 OK)
    let stream = ReaderStream::with_capacity(file, STREAM_CHUNK_SIZE);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_LENGTH, total_size.to_string())
        .header(ACCEPT_RANGES, "bytes")
        .header(ETAG, etag_value)
        .header(CACHE_CONTROL, "public, max-age=31536000, immutable")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
