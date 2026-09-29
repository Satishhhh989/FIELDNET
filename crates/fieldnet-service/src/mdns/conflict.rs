use crate::mdns::error::MdnsError;
use fieldnet_core::NodeId;

/// Normalizes a hostname to ensure standard mDNS formatting (ends with ".local.").
pub fn normalize_hostname(name: &str) -> Result<String, MdnsError> {
    let mut trimmed = name.trim().to_lowercase();
    if trimmed.is_empty() {
        return Err(MdnsError::InvalidHostname(
            "Hostname cannot be empty".to_string(),
        ));
    }

    if !trimmed.ends_with('.') {
        trimmed.push('.');
    }

    if !trimmed.ends_with(".local.") {
        return Err(MdnsError::InvalidHostname(format!(
            "Hostname '{}' must end with .local",
            name
        )));
    }

    // Verify label lengths and valid characters
    for label in trimmed.trim_end_matches('.').split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err(MdnsError::InvalidHostname(format!(
                "Invalid label length in hostname '{}'",
                name
            )));
        }
        if !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(MdnsError::InvalidHostname(format!(
                "Invalid characters in hostname '{}'",
                name
            )));
        }
    }

    Ok(trimmed)
}

/// Computes a deterministic fallback hostname using the node's NodeId.
/// Example: "fieldnet-a1b2c3d4.local."
pub fn deterministic_fallback_hostname(base: &str, node_id: &NodeId) -> String {
    let base_trimmed = base.trim_end_matches('.').trim_end_matches(".local");
    let short_id = &node_id.to_string()[..8.min(node_id.to_string().len())];
    format!("{}-{}.local.", base_trimmed, short_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_hostname() {
        assert_eq!(
            normalize_hostname("fieldnet.local").unwrap(),
            "fieldnet.local."
        );
        assert_eq!(
            normalize_hostname("fieldnet.local.").unwrap(),
            "fieldnet.local."
        );
        assert_eq!(
            normalize_hostname("My-Node.Local").unwrap(),
            "my-node.local."
        );

        assert!(normalize_hostname("fieldnet.com").is_err());
        assert!(normalize_hostname("").is_err());
        assert!(normalize_hostname("invalid_underscore.local").is_err());
    }

    #[test]
    fn test_deterministic_fallback_hostname() {
        let node_id = NodeId::from_bytes([
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08,
        ]);
        let fallback = deterministic_fallback_hostname("fieldnet.local", &node_id);
        assert_eq!(fallback, "fieldnet-12345678.local.");
    }
}
