use std::net::{IpAddr, Ipv4Addr};
use tracing::debug;

/// Enumerates network interfaces on the local host and filters usable IP addresses.
pub fn get_local_interfaces(allow_loopback: bool) -> Vec<IpAddr> {
    let mut ips = Vec::new();

    match if_addrs::get_if_addrs() {
        Ok(ifaces) => {
            for iface in ifaces {
                let is_loopback = iface.is_loopback();
                let ip = iface.ip();

                // Only consider IPv4 for mDNS currently (standard 224.0.0.251)
                if let IpAddr::V4(ipv4) = ip {
                    // Ignore unspecified (0.0.0.0) or link-local auto-assigned unconfigured (169.254.0.0/16 if desired)
                    if ipv4.is_unspecified() {
                        continue;
                    }

                    if is_loopback {
                        if allow_loopback {
                            ips.push(IpAddr::V4(ipv4));
                        }
                    } else {
                        ips.push(IpAddr::V4(ipv4));
                    }
                }
            }
        }
        Err(e) => {
            debug!("Failed to query system network interfaces: {}", e);
        }
    }

    // Deduplicate IPs
    ips.sort();
    ips.dedup();

    // If no non-loopback interfaces were found and allow_loopback was false,
    // fallback to loopback (127.0.0.1) so local services still function
    if ips.is_empty() {
        debug!("No external network interfaces found, falling back to 127.0.0.1");
        ips.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    ips
}

/// Returns human-readable interface strings for CLI inspection.
pub fn get_interface_summaries() -> Vec<String> {
    let mut summaries = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            summaries.push(format!(
                "{}: {} (loopback: {})",
                iface.name,
                iface.ip(),
                iface.is_loopback()
            ));
        }
    }
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interface_enumeration() {
        let ips = get_local_interfaces(true);
        assert!(
            !ips.is_empty(),
            "Should discover at least one IP address (loopback or local)"
        );
    }
}
