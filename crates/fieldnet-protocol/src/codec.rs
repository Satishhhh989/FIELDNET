use crate::error::ProtocolError;
use crate::message::Message;

/// Serializes a Message to UTF-8 JSON bytes.
pub fn serialize_message(msg: &Message) -> Result<Vec<u8>, ProtocolError> {
    serde_json::to_vec(msg).map_err(|e| ProtocolError::Serialization(e.to_string()))
}

/// Deserializes a Message from UTF-8 JSON bytes.
pub fn deserialize_message(bytes: &[u8]) -> Result<Message, ProtocolError> {
    serde_json::from_slice(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_type::MessageType;
    use fieldnet_core::{NodeId, Priority};
    use fieldnet_identity::NodeIdentity;

    #[test]
    fn test_codec_roundtrip() {
        let identity = NodeIdentity::generate();
        let dest = NodeId::from_bytes([99u8; 32]);
        let msg = Message::sign_and_build(
            &identity,
            dest,
            MessageType::Data,
            b"Codec Test".to_vec(),
            10,
            30_000,
            42,
            Priority::High,
        )
        .unwrap();

        let bytes = serialize_message(&msg).unwrap();
        let decoded = deserialize_message(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }
}
