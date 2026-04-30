use super::*;

pub(super) fn build_ens_v1_generic_record_observation(
    raw_log: &AuthorityRawLogRow,
    topic0: &str,
) -> Result<Option<AuthorityObservation>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
        return Ok(None);
    }

    if topic0.eq_ignore_ascii_case(&abi_changed_topic0()) {
        let Some(content_type) = raw_log
            .topics
            .get(2)
            .and_then(|topic| hex_to_word(topic).ok())
            .and_then(|word| abi_word_to_i64(&word).ok())
        else {
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

    if topic0.eq_ignore_ascii_case(&content_changed_topic0()) {
        let Some(content) = raw_log
            .data
            .get(..32)
            .map(hex_string)
            .and_then(|content| normalize_hex_32(&content).ok())
        else {
            return Ok(None);
        };
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

    if topic0.eq_ignore_ascii_case(&contenthash_changed_topic0()) {
        let Some(contenthash) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 0) else {
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
                "bytes": hex_string(&contenthash),
            })),
            None,
        );
    }

    if topic0.eq_ignore_ascii_case(&dns_record_changed_topic0()) {
        let Some(dns_name) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 0) else {
            return Ok(None);
        };
        let Some(resource) = decode_resolver_i64_word(raw_log.data.get(32..64)) else {
            return Ok(None);
        };
        let Some(record) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 2) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "DNSRecordChanged",
            dns_record_selector(resource, &dns_name),
            Some(json!({
                "encoding": "hex",
                "bytes": hex_string(&record),
            })),
            None,
        );
    }

    if topic0.eq_ignore_ascii_case(&dns_record_deleted_topic0()) {
        let Some(dns_name) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 0) else {
            return Ok(None);
        };
        let Some(resource) = decode_resolver_i64_word(raw_log.data.get(32..64)) else {
            return Ok(None);
        };
        return resolver_record_observation(
            raw_log,
            "DNSRecordDeleted",
            dns_record_selector(resource, &dns_name),
            Some(json!({ "deleted": true })),
            None,
        );
    }

    if topic0.eq_ignore_ascii_case(&dns_zonehash_changed_topic0()) {
        let Some(last_zonehash) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 0) else {
            return Ok(None);
        };
        let Some(zonehash) = decode_resolver_nth_dynamic_bytes(&raw_log.data, 1) else {
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
                    "bytes": hex_string(&last_zonehash),
                },
                "current": {
                    "encoding": "hex",
                    "bytes": hex_string(&zonehash),
                },
            })),
            None,
        );
    }

    if topic0.eq_ignore_ascii_case(&interface_changed_topic0()) {
        let Some(interface_id) = raw_log
            .topics
            .get(2)
            .and_then(|topic| topic_bytes4(topic).ok())
        else {
            return Ok(None);
        };
        let Some(implementer) = raw_log
            .data
            .get(..32)
            .and_then(decode_resolver_owner_address)
        else {
            return Ok(None);
        };
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

    if topic0.eq_ignore_ascii_case(&data_changed_topic0()) {
        let Some(key) = decode_resolver_first_dynamic_string(&raw_log.data) else {
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

fn hex_to_word(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut word = [0u8; 32];
    for (index, chunk) in normalized[2..].as_bytes().chunks(2).enumerate() {
        let value = std::str::from_utf8(chunk).context("hex topic chunk is not UTF-8")?;
        word[index] = u8::from_str_radix(value, 16).context("hex topic chunk is invalid")?;
    }
    Ok(word)
}

fn topic_bytes4(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[2..10]))
}
