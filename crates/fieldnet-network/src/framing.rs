use crate::error::NetworkError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024; // 16 MB limit

/// Reads a 4-byte length-prefixed frame from an async stream.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, NetworkError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            NetworkError::ConnectionClosed
        } else {
            NetworkError::Io(e)
        }
    })?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(NetworkError::Protocol(
            fieldnet_protocol::ProtocolError::MalformedMessage(format!(
                "Frame size {} exceeds maximum permitted ({})",
                len, MAX_FRAME_SIZE
            )),
        ));
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            NetworkError::ConnectionClosed
        } else {
            NetworkError::Io(e)
        }
    })?;

    Ok(buf)
}

/// Writes a 4-byte length-prefixed frame to an async stream and flushes.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), NetworkError> {
    if data.len() > MAX_FRAME_SIZE {
        return Err(NetworkError::Protocol(
            fieldnet_protocol::ProtocolError::MalformedMessage(format!(
                "Frame size {} exceeds maximum permitted ({})",
                data.len(),
                MAX_FRAME_SIZE
            )),
        ));
    }

    let len = data.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_framing_roundtrip() {
        let (mut client, mut server) = duplex(1024);
        let payload = b"FIELDNET framed test payload";

        let client_task = tokio::spawn(async move {
            write_frame(&mut client, payload).await.unwrap();
        });

        let server_task = tokio::spawn(async move {
            let received = read_frame(&mut server).await.unwrap();
            assert_eq!(received, payload);
        });

        tokio::try_join!(client_task, server_task).unwrap();
    }
}
