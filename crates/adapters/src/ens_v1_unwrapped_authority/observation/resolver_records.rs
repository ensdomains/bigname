use super::*;
use alloy_sol_types::sol_data::{
    Address as SolAddress, Bytes as SolBytes, FixedBytes as SolFixedBytes, String as SolString,
    Uint,
};
use alloy_sol_types::{SolType, abi::TokenSeq};

pub(super) fn build_ens_v1_generic_record_observation(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
    event_topics: &AuthorityEventTopics,
) -> Result<Option<AuthorityObservation>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        return Ok(None);
    }

    if event_topics.matches(ABI_CHANGED_SIGNATURE, topic0)? {
        return abi_changed_observation(raw_log);
    }

    if event_topics.matches(CONTENT_CHANGED_SIGNATURE, topic0)? {
        return content_changed_observation(raw_log);
    }

    if event_topics.matches(CONTENTHASH_CHANGED_SIGNATURE, topic0)? {
        return contenthash_changed_observation(raw_log);
    }

    if event_topics.matches(DNS_RECORD_CHANGED_SIGNATURE, topic0)? {
        return dns_record_changed_observation(raw_log);
    }

    if event_topics.matches(DNS_RECORD_DELETED_SIGNATURE, topic0)? {
        return dns_record_deleted_observation(raw_log);
    }

    if event_topics.matches(DNS_ZONEHASH_CHANGED_SIGNATURE, topic0)? {
        return dns_zonehash_changed_observation(raw_log);
    }

    if event_topics.matches(INTERFACE_CHANGED_SIGNATURE, topic0)? {
        return interface_changed_observation(raw_log);
    }

    if event_topics.matches(DATA_CHANGED_SIGNATURE, topic0)? {
        return data_changed_observation(raw_log);
    }

    Ok(None)
}

fn abi_changed_observation(raw_log: &AuthorityRawLogRow) -> Result<Option<AuthorityObservation>> {
    let Some(content_type) =
        decode_topic_u256_i64_skip(raw_log.topics.get(2), "ABIChanged content type")
    else {
        return Ok(None);
    };
    resolver_record_observation(
        raw_log,
        "ABIChanged",
        RecordSelector {
            record_key: format!("abi:{content_type}"),
            record_family: "abi".to_owned(),
            selector_key: Some(content_type.to_string()),
        },
        Some(json!(content_type)),
        None,
    )
}

fn content_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some((content,)) = decode_params_skip::<(SolFixedBytes<32>,)>(
        &raw_log.data,
        "ContentChanged data is malformed",
    ) else {
        return Ok(None);
    };
    let content = normalize_hex_32(&hex_string(content.as_slice()))?;
    resolver_record_observation(
        raw_log,
        "ContentChanged",
        RecordSelector {
            record_key: "content".to_owned(),
            record_family: "content".to_owned(),
            selector_key: None,
        },
        Some(json!(content)),
        None,
    )
}

fn contenthash_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some((contenthash,)) =
        decode_params_skip::<(SolBytes,)>(&raw_log.data, "ContenthashChanged data is malformed")
    else {
        return Ok(None);
    };
    resolver_record_observation(
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
    )
}

fn dns_record_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some((dns_name, resource, record)) = decode_params_skip::<(SolBytes, Uint<16>, SolBytes)>(
        &raw_log.data,
        "DNSRecordChanged data is malformed",
    ) else {
        return Ok(None);
    };
    let resource = i64::from(resource);
    resolver_record_observation(
        raw_log,
        "DNSRecordChanged",
        dns_record_selector(resource, dns_name.as_ref()),
        Some(json!({
            "encoding": "hex",
            "bytes": hex_string(record.as_ref()),
        })),
        None,
    )
}

fn dns_record_deleted_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some((dns_name, resource)) = decode_params_skip::<(SolBytes, Uint<16>)>(
        &raw_log.data,
        "DNSRecordDeleted data is malformed",
    ) else {
        return Ok(None);
    };
    let resource = i64::from(resource);
    resolver_record_observation(
        raw_log,
        "DNSRecordDeleted",
        dns_record_selector(resource, dns_name.as_ref()),
        Some(json!({ "deleted": true })),
        None,
    )
}

fn dns_zonehash_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some((last_zonehash, zonehash)) = decode_params_skip::<(SolBytes, SolBytes)>(
        &raw_log.data,
        "DNSZonehashChanged data is malformed",
    ) else {
        return Ok(None);
    };
    resolver_record_observation(
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
    )
}

fn interface_changed_observation(
    raw_log: &AuthorityRawLogRow,
) -> Result<Option<AuthorityObservation>> {
    let Some(interface_id) = decode_topic_skip::<SolFixedBytes<4>>(raw_log.topics.get(2)) else {
        return Ok(None);
    };
    let Some((implementer,)) =
        decode_params_skip::<(SolAddress,)>(&raw_log.data, "InterfaceChanged data is malformed")
    else {
        return Ok(None);
    };
    let interface_id = hex_string(interface_id.as_slice());
    let implementer = crate::evm_abi::address_hex(implementer);
    resolver_record_observation(
        raw_log,
        "InterfaceChanged",
        RecordSelector {
            record_key: format!("interface:{interface_id}"),
            record_family: "interface".to_owned(),
            selector_key: Some(interface_id),
        },
        Some(json!(implementer)),
        None,
    )
}

fn data_changed_observation(raw_log: &AuthorityRawLogRow) -> Result<Option<AuthorityObservation>> {
    let Some((key,)) =
        decode_params_skip::<(SolString,)>(&raw_log.data, "DataChanged data is malformed")
    else {
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
    resolver_record_observation(
        raw_log,
        "DataChanged",
        RecordSelector {
            record_key: format!("data:{key}"),
            record_family: "data".to_owned(),
            selector_key: Some(key),
        },
        Some(json!({ "indexed_data_hash": indexed_data_hash })),
        None,
    )
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

fn decode_params_skip<'de, T>(data: &'de [u8], context: &'static str) -> Option<T::RustType>
where
    T: SolType,
    T::Token<'de>: TokenSeq<'de>,
{
    crate::evm_abi::abi_decode_params::<T>(data, context).ok()
}

fn decode_topic_u256_i64_skip(topic: Option<&String>, label: &str) -> Option<i64> {
    crate::evm_abi::u256_topic_i64(topic?, label).ok()
}

fn decode_topic_skip<T>(topic: Option<&String>) -> Option<T::RustType>
where
    T: SolType,
{
    let word = crate::evm_abi::hex_32(topic?).ok()?;
    T::abi_decode_validate(&word).ok()
}
