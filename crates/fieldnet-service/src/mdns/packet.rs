use simple_dns::rdata::{RData, A, PTR, SRV, TXT};
use simple_dns::{Name, Packet, Question, ResourceRecord, CLASS, TYPE};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};

use crate::mdns::error::MdnsError;

pub const MAX_MDNS_PACKET_SIZE: usize = 8192;

/// Structured representation of a discovered or advertised mDNS service instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdnsRecord {
    pub instance_name: String,
    pub service_type: String,
    pub hostname: String,
    pub port: u16,
    pub addresses: Vec<IpAddr>,
    pub txt: HashMap<String, String>,
    pub ttl: u32,
}

/// Parses an inbound raw mDNS packet byte slice.
pub fn parse_packet(buf: &[u8]) -> Result<Packet<'_>, MdnsError> {
    if buf.len() > MAX_MDNS_PACKET_SIZE {
        return Err(MdnsError::PacketParse(format!(
            "Packet size {} exceeds maximum allowed {}",
            buf.len(),
            MAX_MDNS_PACKET_SIZE
        )));
    }

    Packet::parse(buf).map_err(|e| MdnsError::PacketParse(e.to_string()))
}

/// Builds an mDNS probe packet (querying for an A record on the requested hostname).
pub fn build_probe_packet(hostname: &str) -> Result<Vec<u8>, MdnsError> {
    let norm = hostname.trim_end_matches('.');
    let name = Name::new_unchecked(norm);
    let mut packet = Packet::new_query(0);
    // Unicast response flag false for standard multicast probe
    packet
        .questions
        .push(Question::new(name, TYPE::A.into(), CLASS::IN.into(), false));
    packet
        .build_bytes_vec()
        .map_err(|e| MdnsError::PacketBuild(e.to_string()))
}

/// Builds an mDNS query packet for browsing services or resolving hostnames.
pub fn build_query_packet(query_name: &str, qtype: TYPE) -> Result<Vec<u8>, MdnsError> {
    let norm = query_name.trim_end_matches('.');
    let name = Name::new_unchecked(norm);
    let mut packet = Packet::new_query(0);
    packet
        .questions
        .push(Question::new(name, qtype.into(), CLASS::IN.into(), false));
    packet
        .build_bytes_vec()
        .map_err(|e| MdnsError::PacketBuild(e.to_string()))
}

/// Parameters describing a service advertisement for building DNS-SD resource records.
#[derive(Debug, Clone)]
pub struct ServiceAdvertisementInfo<'a> {
    pub service_name: &'a str,
    pub service_type: &'a str,
    pub hostname: &'a str,
    pub port: u16,
    pub ips: &'a [IpAddr],
    pub txt_attrs: &'a HashMap<String, String>,
    pub ttl: u32,
}

/// Builds an mDNS announcement packet (or goodbye packet if ttl == 0) announcing:
/// - PTR record: <service_type> -> <service_name>.<service_type>
/// - SRV record: <service_name>.<service_type> -> target <hostname>, port <port>
/// - TXT record: <service_name>.<service_type> -> key=value pairs
/// - A records: <hostname> -> IPv4 addresses
pub fn build_announcement_packet(
    info: &ServiceAdvertisementInfo<'_>,
) -> Result<Vec<u8>, MdnsError> {
    let mut packet = Packet::new_reply(0);

    let clean_service_type = info.service_type.trim_matches('.');
    let clean_hostname = info.hostname.trim_matches('.');
    let clean_service_name = info.service_name.trim();

    let srv_full_name = format!("{}.{}", clean_service_name, clean_service_type);
    let srv_name = Name::new_unchecked(&srv_full_name);
    let type_name = Name::new_unchecked(clean_service_type);
    let host_name = Name::new_unchecked(clean_hostname);

    // 1. PTR record
    packet.answers.push(ResourceRecord::new(
        type_name,
        CLASS::IN,
        info.ttl,
        RData::PTR(PTR(srv_name.clone())),
    ));

    // 2. SRV record
    packet.answers.push(ResourceRecord::new(
        srv_name.clone(),
        CLASS::IN,
        info.ttl,
        RData::SRV(SRV {
            priority: 0,
            weight: 0,
            port: info.port,
            target: host_name.clone(),
        }),
    ));

    // 3. TXT record
    let entries: Vec<String> = info
        .txt_attrs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect();
    let mut txt_rdata = TXT::new();
    for entry in &entries {
        let _ = txt_rdata.add_string(entry);
    }
    packet.answers.push(ResourceRecord::new(
        srv_name,
        CLASS::IN,
        info.ttl,
        RData::TXT(txt_rdata),
    ));

    // 4. A records for host
    for ip in info.ips {
        if let IpAddr::V4(ipv4) = ip {
            packet.answers.push(ResourceRecord::new(
                host_name.clone(),
                CLASS::IN,
                info.ttl,
                RData::A(A {
                    address: (*ipv4).into(),
                }),
            ));
        }
    }

    packet
        .build_bytes_vec()
        .map_err(|e| MdnsError::PacketBuild(e.to_string()))
}

/// Builds an RFC 6762 Goodbye packet (TTL = 0) to flush network caches.
pub fn build_goodbye_packet(info: &ServiceAdvertisementInfo<'_>) -> Result<Vec<u8>, MdnsError> {
    let mut goodbye_info = info.clone();
    goodbye_info.ttl = 0;
    build_announcement_packet(&goodbye_info)
}

/// Evaluates questions in an incoming query and builds a response packet if applicable.
pub fn build_response_packet(
    incoming: &Packet,
    info: &ServiceAdvertisementInfo<'_>,
) -> Result<Option<Vec<u8>>, MdnsError> {
    let srv_full_name = format!("{}.{}", info.service_name, info.service_type);
    let norm_srv = srv_full_name.trim_end_matches('.').to_lowercase();
    let norm_type = info.service_type.trim_end_matches('.').to_lowercase();
    let norm_host = info.hostname.trim_end_matches('.').to_lowercase();

    let mut matches = false;

    for question in &incoming.questions {
        let qname = question.qname.to_string();
        let norm_qname = qname.trim_end_matches('.').to_lowercase();

        if norm_qname == norm_type || norm_qname == norm_srv || norm_qname == norm_host {
            matches = true;
            break;
        }
    }

    if !matches {
        return Ok(None);
    }

    // Return the full announcement payload for the matching query
    let bytes = build_announcement_packet(info)?;
    Ok(Some(bytes))
}

/// Extracts any discovered `MdnsRecord` from an incoming DNS packet.
pub fn extract_records_from_packet(packet: &Packet) -> Vec<MdnsRecord> {
    let mut records = Vec::new();
    let mut srv_map: HashMap<String, (u16, String, u32)> = HashMap::new(); // srv_name -> (port, target_host, ttl)
    let mut txt_map: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut ip_map: HashMap<String, Vec<IpAddr>> = HashMap::new(); // host -> [ips]

    // Examine answers and additional records
    let all_records = packet
        .answers
        .iter()
        .chain(packet.additional_records.iter());

    for r in all_records {
        let name_str = r.name.to_string().trim_end_matches('.').to_string();
        match &r.rdata {
            RData::SRV(srv) => {
                let target = srv.target.to_string().trim_end_matches('.').to_string();
                srv_map.insert(name_str, (srv.port, target, r.ttl));
            }
            RData::TXT(txt) => {
                let mut map = HashMap::new();
                for (k, v) in txt.attributes() {
                    map.insert(k.to_string(), v.unwrap_or_default());
                }
                txt_map.insert(name_str, map);
            }
            RData::A(a) => {
                let ip = IpAddr::V4(Ipv4Addr::from(a.address));
                ip_map.entry(name_str).or_default().push(ip);
            }
            _ => {}
        }
    }

    for (srv_name, (port, target_host, ttl)) in srv_map {
        let txt = txt_map.remove(&srv_name).unwrap_or_default();
        let ips = ip_map.get(&target_host).cloned().unwrap_or_default();

        let (instance_name, service_type) = if let Some(idx) = srv_name.find('.') {
            (srv_name[..idx].to_string(), srv_name[idx + 1..].to_string())
        } else {
            (srv_name.clone(), "".to_string())
        };

        records.push(MdnsRecord {
            instance_name,
            service_type,
            hostname: target_host,
            port,
            addresses: ips,
            txt,
            ttl,
        });
    }

    records
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_and_parse_probe_packet() {
        let bytes = build_probe_packet("fieldnet.local.").unwrap();
        let parsed = parse_packet(&bytes).unwrap();
        assert_eq!(parsed.questions.len(), 1);
        assert_eq!(
            parsed.questions[0].qname.to_string().trim_end_matches('.'),
            "fieldnet.local"
        );
        assert_eq!(parsed.questions[0].qtype, TYPE::A.into());
    }

    #[test]
    fn test_build_and_extract_announcement_packet() {
        let mut txt = HashMap::new();
        txt.insert("fieldnet_version".to_string(), "0.1.0".to_string());
        txt.insert("protocol_version".to_string(), "1".to_string());

        let ips = vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42))];
        let info = ServiceAdvertisementInfo {
            service_name: "FIELDNET Local Gateway",
            service_type: "_http._tcp.local.",
            hostname: "fieldnet.local.",
            port: 8080,
            ips: &ips,
            txt_attrs: &txt,
            ttl: 120,
        };
        let bytes = build_announcement_packet(&info).unwrap();

        let parsed = parse_packet(&bytes).unwrap();
        let extracted = extract_records_from_packet(&parsed);

        assert_eq!(extracted.len(), 1);
        let record = &extracted[0];
        assert_eq!(record.instance_name, "FIELDNET Local Gateway");
        assert_eq!(record.service_type, "_http._tcp.local");
        assert_eq!(record.hostname, "fieldnet.local");
        assert_eq!(record.port, 8080);
        assert_eq!(record.addresses, ips);
        assert_eq!(record.txt.get("fieldnet_version").unwrap(), "0.1.0");
        assert_eq!(record.txt.get("protocol_version").unwrap(), "1");
        assert_eq!(record.ttl, 120);
    }

    #[test]
    fn test_goodbye_packet_ttl_is_zero() {
        let txt = HashMap::new();
        let ips = vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))];
        let info = ServiceAdvertisementInfo {
            service_name: "FIELDNET",
            service_type: "_http._tcp.local.",
            hostname: "fieldnet.local.",
            port: 8080,
            ips: &ips,
            txt_attrs: &txt,
            ttl: 120,
        };
        let bytes = build_goodbye_packet(&info).unwrap();

        let parsed = parse_packet(&bytes).unwrap();
        assert!(parsed.answers.iter().all(|r| r.ttl == 0));
    }

    #[test]
    fn test_response_to_matching_question() {
        let txt = HashMap::new();
        let ips = vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))];
        let info = ServiceAdvertisementInfo {
            service_name: "FIELDNET",
            service_type: "_http._tcp.local.",
            hostname: "fieldnet.local.",
            port: 8080,
            ips: &ips,
            txt_attrs: &txt,
            ttl: 120,
        };

        // 1. Question matching service type _http._tcp.local
        let query_bytes = build_query_packet("_http._tcp.local.", TYPE::PTR).unwrap();
        let query_packet = parse_packet(&query_bytes).unwrap();

        let response = build_response_packet(&query_packet, &info).unwrap();
        assert!(response.is_some());

        // 2. Question matching hostname fieldnet.local
        let host_query_bytes = build_query_packet("fieldnet.local.", TYPE::A).unwrap();
        let host_query_packet = parse_packet(&host_query_bytes).unwrap();

        let host_response = build_response_packet(&host_query_packet, &info).unwrap();
        assert!(host_response.is_some());

        // 3. Question for unrelated service
        let unrelated_query_bytes = build_query_packet("_spotify._tcp.local.", TYPE::PTR).unwrap();
        let unrelated_query_packet = parse_packet(&unrelated_query_bytes).unwrap();

        let unrelated_response = build_response_packet(&unrelated_query_packet, &info).unwrap();
        assert!(unrelated_response.is_none());
    }

    #[test]
    fn test_malformed_and_oversized_packet_safety() {
        // 1. Malformed garbage
        let garbage = vec![0xde, 0xad, 0xbe, 0xef, 0x01, 0x02];
        let res = parse_packet(&garbage);
        assert!(res.is_err(), "Malformed packet must return error safely");

        // 2. Oversized packet > 8192 bytes
        let huge = vec![0u8; 9000];
        let res_huge = parse_packet(&huge);
        assert!(res_huge.is_err(), "Oversized packet must be rejected");
    }
}
