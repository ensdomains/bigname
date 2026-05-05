use super::*;
use alloy_sol_types::sol_data::{
    Address as SolAddress, Bytes as SolBytes, FixedBytes, String as SolString, Uint,
};

pub(super) fn build_ens_v1_generic_record_observation(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<AuthorityObservation>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        return Ok(None);
    }

    if event_topics.matches(ABI_CHANGED_SIGNATURE, topic0)? {
        let Some(content_type) = raw_log.topics.get(2).and_then(|topic| {
            crate::evm_abi::u256_topic_i64(topic, "ABIChanged content type").ok()
        }) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "ABIChanged",
            RecordSelector {
                record_key: format!("abi:{content_type}"),
                record_family: "abi".to_owned(),
                selector_key: Some(content_type.to_string()),
            },
            Some(json!(content_type)),
            None,
        );
    }

    if event_topics.matches(CONTENT_CHANGED_SIGNATURE, topic0)? {
        let Ok((content,)) = crate::evm_abi::abi_decode_params::<(FixedBytes<32>,)>(
            &raw_log.data,
            "ContentChanged data is malformed",
        ) else {
            return Ok(None);
        };
        let content = normalize_hex_32(&hex_string(content.as_slice()))?;
        return resolver_record_observation(
            raw_log,
            "ContentChanged",
            RecordSelector {
                record_key: "content".to_owned(),
                record_family: "content".to_owned(),
                selector_key: None,
            },
            Some(json!(content)),
            None,
        );
    }

    if event_topics.matches(CONTENTHASH_CHANGED_SIGNATURE, topic0)? {
        let Ok((contenthash,)) = crate::evm_abi::abi_decode_params::<(SolBytes,)>(
            &raw_log.data,
            "ContenthashChanged data is malformed",
        ) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "ContenthashChanged",
            RecordSelector {
                record_key: "contenthash".to_owned(),
                record_family: "contenthash".to_owned(),
                selector_key: None,
            },
            Some(json!({
                "encoding": "hex",
                "bytes": hex_string(contenthash.as_ref()),
            })),
            None,
        );
    }

    if event_topics.matches(DNS_RECORD_CHANGED_SIGNATURE, topic0)? {
        let Ok((dns_name, resource, record)) =
            crate::evm_abi::abi_decode_params::<(SolBytes, Uint<16>, SolBytes)>(
                &raw_log.data,
                "DNSRecordChanged data is malformed",
            )
        else {
            return Ok(None);
        };
        let resource = i64::from(resource);
        return resolver_record_observation(
            raw_log,
            "DNSRecordChanged",
            dns_record_selector(resource, dns_name.as_ref()),
            Some(json!({
                "encoding": "hex",
                "bytes": hex_string(record.as_ref()),
            })),
            None,
        );
    }

    if event_topics.matches(DNS_RECORD_DELETED_SIGNATURE, topic0)? {
        let Ok((dns_name, resource)) = crate::evm_abi::abi_decode_params::<(SolBytes, Uint<16>)>(
            &raw_log.data,
            "DNSRecordDeleted data is malformed",
        ) else {
            return Ok(None);
        };
        let resource = i64::from(resource);
        return resolver_record_observation(
            raw_log,
            "DNSRecordDeleted",
            dns_record_selector(resource, dns_name.as_ref()),
            Some(json!({ "deleted": true })),
            None,
        );
    }

    if event_topics.matches(DNS_ZONEHASH_CHANGED_SIGNATURE, topic0)? {
        let Ok((last_zonehash, zonehash)) = crate::evm_abi::abi_decode_params::<(SolBytes, SolBytes)>(
            &raw_log.data,
            "DNSZonehashChanged data is malformed",
        ) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "DNSZonehashChanged",
            RecordSelector {
                record_key: "dns:zonehash".to_owned(),
                record_family: "dns".to_owned(),
                selector_key: Some("zonehash".to_owned()),
            },
            Some(json!({
                "previous": {
                    "encoding": "hex",
                    "bytes": hex_string(last_zonehash.as_ref()),
                },
                "current": {
                    "encoding": "hex",
                    "bytes": hex_string(zonehash.as_ref()),
                },
            })),
            None,
        );
    }

    if event_topics.matches(INTERFACE_CHANGED_SIGNATURE, topic0)? {
        let Some(interface_id) = raw_log
            .topics
            .get(2)
            .and_then(|topic| topic_bytes4(topic).ok())
        else {
            return Ok(None);
        };
        let Ok((implementer,)) = crate::evm_abi::abi_decode_params::<(SolAddress,)>(
            &raw_log.data,
            "InterfaceChanged data is malformed",
        ) else {
            return Ok(None);
        };
        let implementer = crate::evm_abi::address_hex(implementer);
        return resolver_record_observation(
            raw_log,
            "InterfaceChanged",
            RecordSelector {
                record_key: format!("interface:{interface_id}"),
                record_family: "interface".to_owned(),
                selector_key: Some(interface_id),
            },
            Some(json!(implementer)),
            None,
        );
    }

    if event_topics.matches(DATA_CHANGED_SIGNATURE, topic0)? {
        let Ok((key,)) = crate::evm_abi::abi_decode_params::<(SolString,)>(
            &raw_log.data,
            "DataChanged data is malformed",
        ) else {
            return Ok(None);
        };
        let Some(indexed_key_hash) = normalize_resolver_topic(raw_log.topics.get(2)) else {
            return Ok(None);
        };
        if indexed_key_hash != keccak256_hex(key.as_bytes()) {
            return Ok(None);
        }
        let Some(indexed_data_hash) = normalize_resolver_topic(raw_log.topics.get(3)) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "DataChanged",
            RecordSelector {
                record_key: format!("data:{key}"),
                record_family: "data".to_owned(),
                selector_key: Some(key),
            },
            Some(json!({ "indexed_data_hash": indexed_data_hash })),
            None,
        );
    }

    Ok(None)
}

fn resolver_record_observation(
    raw_log: &AuthorityRawLogRow,
    _event_name: &str,
    selector: RecordSelector,
    value: Option<Value>,
    raw_name: Option<String>,
) -> Result<Option<AuthorityObservation>> {
    let Some(namehash) = normalize_resolver_topic(raw_log.topics.get(1)) else {
        return Ok(None);
    };
    Ok(Some(AuthorityObservation::RecordChanged(
        RecordChangeObservation {
            namehash,
            resolver: raw_log.emitting_address.clone(),
            selector,
            value,
            raw_name,
            reference: raw_log.reference(),
        },
    )))
}

fn dns_record_selector(resource: i64, dns_name: &[u8]) -> RecordSelector {
    let selector_key = format!("{resource}:{}", hex_string(dns_name));
    RecordSelector {
        record_key: format!("dns:{selector_key}"),
        record_family: "dns".to_owned(),
        selector_key: Some(selector_key),
    }
}

fn topic_bytes4(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[2..10]))
}
