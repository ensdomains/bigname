use alloy_sol_types::sol_data::{
    Address as SolAddress, Bytes as SolBytes, FixedBytes, String as SolString, Uint,
};
use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::{Value, json};

use super::constants::{
    ALIAS_CHANGED_SIGNATURE, DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION,
    ENS_V1_NAME_REGISTERED_SIGNATURE, ENS_V1_NAME_RENEWED_SIGNATURE,
    ENS_V2_NAME_REGISTERED_SIGNATURE, ENS_V2_NAME_RENEWED_SIGNATURE, EVENT_KIND_PREIMAGE_OBSERVED,
    LABEL_REGISTERED_SIGNATURE, LABEL_RESERVED_SIGNATURE, NAME_WRAPPED_SIGNATURE,
    NAMED_ADDR_RESOURCE_SIGNATURE, NAMED_RESOURCE_SIGNATURE, NAMED_TEXT_RESOURCE_SIGNATURE,
    PARENT_UPDATED_SIGNATURE, SOURCE_EVENT_ALIAS_CHANGED, SOURCE_EVENT_LABEL_REGISTERED,
    SOURCE_EVENT_LABEL_RESERVED, SOURCE_EVENT_NAME_REGISTERED, SOURCE_EVENT_NAME_RENEWED,
    SOURCE_EVENT_NAME_WRAPPED, SOURCE_EVENT_NAMED_ADDR_RESOURCE, SOURCE_EVENT_NAMED_RESOURCE,
    SOURCE_EVENT_NAMED_TEXT_RESOURCE, SOURCE_EVENT_PARENT_UPDATED,
    SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::decoding::{hex_string_without_prefix, keccak256_hex};
use super::event_topics::PreimageObservedEventTopics;
use super::preimage_observation::{
    can_observe_dns_label, observe_dns_encoded_name, observe_registrar_eth_name,
    observe_single_label,
};
use super::types::{PreimageObservation, WatchedRawLogRow};

type EnsV1RegistrarLabelData = (SolString, Uint<256>, Uint<256>);
type EnsV2NameRegisteredData = (
    SolString,
    SolAddress,
    SolAddress,
    SolAddress,
    Uint<64>,
    SolAddress,
    FixedBytes<32>,
    Uint<256>,
    Uint<256>,
);
type EnsV2NameRenewedData = (
    SolString,
    Uint<64>,
    Uint<64>,
    SolAddress,
    FixedBytes<32>,
    Uint<256>,
);

pub(super) fn build_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    match raw_log.source_family.as_str() {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => {
            build_registrar_preimage_observed_events(raw_log, event_topics)
        }
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => {
            build_name_wrapped_preimage_observed_events(raw_log, event_topics)
        }
        SOURCE_FAMILY_ENS_V2_ROOT_L1
        | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        | SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
        | SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => {
            build_ens_v2_preimage_observed_events(raw_log, event_topics)
        }
        _ => Ok(Vec::new()),
    }
}

fn build_name_wrapped_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_WRAPPER_L1 {
        return Ok(Vec::new());
    }

    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    if !event_topics.matches(raw_log, NAME_WRAPPED_SIGNATURE, topic0)? {
        return Ok(Vec::new());
    }

    let dns_name = decode_name_wrapped_dns_name(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NameWrapped bytes payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation = observe_dns_encoded_name(&dns_name).with_context(|| {
        format!(
            "failed to interpret dns-encoded name for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    if let Some(indexed_namehash) = raw_log.topics.get(1)
        && !indexed_namehash.eq_ignore_ascii_case(&observation.namehash)
    {
        bail!(
            "NameWrapped indexed namehash {} does not match decoded namehash {} for chain {} block {} log {}",
            indexed_namehash,
            observation.namehash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        SOURCE_EVENT_NAME_WRAPPED,
        observation,
        None,
    )])
}

fn build_registrar_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };
    let (source_event, signature) =
        if event_topics.matches(raw_log, ENS_V1_NAME_REGISTERED_SIGNATURE, topic0)? {
            (
                SOURCE_EVENT_NAME_REGISTERED,
                ENS_V1_NAME_REGISTERED_SIGNATURE,
            )
        } else if event_topics.matches(raw_log, ENS_V1_NAME_RENEWED_SIGNATURE, topic0)? {
            (SOURCE_EVENT_NAME_RENEWED, ENS_V1_NAME_RENEWED_SIGNATURE)
        } else {
            return Ok(Vec::new());
        };

    let Some(label) = decode_observable_event_label(raw_log, signature)? else {
        return Ok(Vec::new());
    };
    let observation = observe_registrar_eth_name(&label).with_context(|| {
        format!(
            "failed to derive registrar .eth preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observed_labelhash = observation
        .labelhashes
        .first()
        .context("registrar observation is missing the explicit labelhash")?;

    if let Some(indexed_labelhash) = raw_log.topics.get(1)
        && !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash)
    {
        bail!(
            "{source_event} indexed labelhash {} does not match decoded labelhash {} for chain {} block {} log {}",
            indexed_labelhash,
            observed_labelhash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn build_ens_v2_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    event_topics: &PreimageObservedEventTopics,
) -> Result<Vec<NormalizedEvent>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(Vec::new());
    };

    match raw_log.source_family.as_str() {
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
            if event_topics.matches(raw_log, LABEL_REGISTERED_SIGNATURE, topic0)? {
                return build_ens_v2_registry_label_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_LABEL_REGISTERED,
                    LABEL_REGISTERED_SIGNATURE,
                );
            }
            if event_topics.matches(raw_log, LABEL_RESERVED_SIGNATURE, topic0)? {
                return build_ens_v2_registry_label_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_LABEL_RESERVED,
                    LABEL_RESERVED_SIGNATURE,
                );
            }
            if event_topics.matches(raw_log, PARENT_UPDATED_SIGNATURE, topic0)? {
                let Some(label) = decode_observable_event_label(raw_log, PARENT_UPDATED_SIGNATURE)?
                else {
                    return Ok(Vec::new());
                };
                let observation = observe_single_label(&label).with_context(|| {
                format!(
                    "failed to derive ENSv2 registry parent label preimage for chain {} block {} log {}",
                    raw_log.chain_id, raw_log.block_hash, raw_log.log_index
                )
            })?;
                return Ok(vec![build_preimage_observed_normalized_event(
                    raw_log,
                    SOURCE_EVENT_PARENT_UPDATED,
                    observation,
                    None,
                )]);
            }
            return Ok(Vec::new());
        }
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 => {
            if event_topics.matches(raw_log, ENS_V2_NAME_REGISTERED_SIGNATURE, topic0)? {
                return build_ens_v2_registrar_label_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAME_REGISTERED,
                    ENS_V2_NAME_REGISTERED_SIGNATURE,
                );
            }
            if event_topics.matches(raw_log, ENS_V2_NAME_RENEWED_SIGNATURE, topic0)? {
                return build_ens_v2_registrar_label_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAME_RENEWED,
                    ENS_V2_NAME_RENEWED_SIGNATURE,
                );
            }
            return Ok(Vec::new());
        }
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => {
            if event_topics.matches(raw_log, ALIAS_CHANGED_SIGNATURE, topic0)? {
                return build_ens_v2_alias_preimage_observed_events(raw_log);
            }
            if event_topics.matches(raw_log, NAMED_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_RESOURCE,
                );
            }
            if event_topics.matches(raw_log, NAMED_TEXT_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_TEXT_RESOURCE,
                );
            }
            if event_topics.matches(raw_log, NAMED_ADDR_RESOURCE_SIGNATURE, topic0)? {
                return build_ens_v2_named_dns_preimage_observed_events(
                    raw_log,
                    SOURCE_EVENT_NAMED_ADDR_RESOURCE,
                );
            }
        }
        _ => {}
    }

    Ok(Vec::new())
}

fn build_ens_v2_registry_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
    signature: &str,
) -> Result<Vec<NormalizedEvent>> {
    let Some(label) = decode_observable_event_label(raw_log, signature)? else {
        return Ok(Vec::new());
    };
    let observation = observe_single_label(&label).with_context(|| {
        format!(
            "failed to derive ENSv2 registry label preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observed_labelhash = observation
        .labelhashes
        .first()
        .context("ENSv2 registry observation is missing the explicit labelhash")?;
    if let Some(indexed_labelhash) = raw_log.topics.get(2)
        && !indexed_labelhash.eq_ignore_ascii_case(observed_labelhash)
    {
        bail!(
            "{source_event} indexed labelhash {} does not match decoded labelhash {} for chain {} block {} log {}",
            indexed_labelhash,
            observed_labelhash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn build_ens_v2_registrar_label_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
    signature: &str,
) -> Result<Vec<NormalizedEvent>> {
    let Some(label) = decode_observable_event_label(raw_log, signature)? else {
        return Ok(Vec::new());
    };
    let observation = observe_registrar_eth_name(&label).with_context(|| {
        format!(
            "failed to derive ENSv2 registrar .eth preimage for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn decode_observable_event_label(
    raw_log: &WatchedRawLogRow,
    signature: &str,
) -> Result<Option<String>> {
    let Some(label) = decode_event_label(raw_log, signature) else {
        return Ok(None);
    };
    if can_observe_dns_label(&label) {
        Ok(Some(label))
    } else {
        Ok(None)
    }
}

fn decode_event_label(raw_log: &WatchedRawLogRow, signature: &str) -> Option<String> {
    match (raw_log.source_family.as_str(), signature) {
        (SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, ENS_V1_NAME_REGISTERED_SIGNATURE) => {
            decode_ens_v1_registrar_label(&raw_log.data, "ENSv1 NameRegistered data is malformed")
        }
        (SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, ENS_V1_NAME_RENEWED_SIGNATURE) => {
            decode_ens_v1_registrar_label(&raw_log.data, "ENSv1 NameRenewed data is malformed")
        }
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            LABEL_REGISTERED_SIGNATURE,
        ) => {
            let (label, _owner, _expiry) =
                crate::evm_abi::abi_decode_params::<(SolString, SolAddress, Uint<64>)>(
                    &raw_log.data,
                    "LabelRegistered data is malformed",
                )
                .ok()?;
            Some(label)
        }
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            LABEL_RESERVED_SIGNATURE,
        ) => {
            let (label, _expiry) = crate::evm_abi::abi_decode_params::<(SolString, Uint<64>)>(
                &raw_log.data,
                "LabelReserved data is malformed",
            )
            .ok()?;
            Some(label)
        }
        (
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            PARENT_UPDATED_SIGNATURE,
        ) => {
            let (label,) = crate::evm_abi::abi_decode_params::<(SolString,)>(
                &raw_log.data,
                "ParentUpdated data is malformed",
            )
            .ok()?;
            Some(label)
        }
        (SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ENS_V2_NAME_REGISTERED_SIGNATURE) => {
            let (label, ..) = crate::evm_abi::abi_decode_params::<EnsV2NameRegisteredData>(
                &raw_log.data,
                "ENSv2 NameRegistered data is malformed",
            )
            .ok()?;
            Some(label)
        }
        (SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, ENS_V2_NAME_RENEWED_SIGNATURE) => {
            let (label, ..) = crate::evm_abi::abi_decode_params::<EnsV2NameRenewedData>(
                &raw_log.data,
                "ENSv2 NameRenewed data is malformed",
            )
            .ok()?;
            Some(label)
        }
        _ => None,
    }
}

fn decode_ens_v1_registrar_label(data: &[u8], context: &'static str) -> Option<String> {
    let (label, ..) =
        crate::evm_abi::abi_decode_params::<EnsV1RegistrarLabelData>(data, context).ok()?;
    Some(label)
}

fn build_ens_v2_alias_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    let (from_name, to_name) = decode_alias_changed_names(&raw_log.data).with_context(|| {
        format!(
            "failed to decode AliasChanged name payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    validate_indexed_bytes_hash(raw_log, 1, &from_name, "AliasChanged indexedFromName")?;
    validate_indexed_bytes_hash(raw_log, 2, &to_name, "AliasChanged indexedToName")?;

    let mut events = Vec::new();
    if !from_name.is_empty() {
        events.push(build_preimage_observed_normalized_event(
            raw_log,
            SOURCE_EVENT_ALIAS_CHANGED,
            observe_dns_encoded_name(&from_name)?,
            Some("from_name"),
        ));
    }
    if !to_name.is_empty() {
        events.push(build_preimage_observed_normalized_event(
            raw_log,
            SOURCE_EVENT_ALIAS_CHANGED,
            observe_dns_encoded_name(&to_name)?,
            Some("to_name"),
        ));
    }
    Ok(events)
}

fn build_ens_v2_named_dns_preimage_observed_events(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
) -> Result<Vec<NormalizedEvent>> {
    let dns_name = decode_named_resource_name(&raw_log.data, source_event).with_context(|| {
        format!(
            "failed to decode {source_event} DNS name payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    if dns_name.is_empty() {
        return Ok(Vec::new());
    }
    let observation = observe_dns_encoded_name(&dns_name).with_context(|| {
        format!(
            "failed to interpret {source_event} DNS-encoded name for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(vec![build_preimage_observed_normalized_event(
        raw_log,
        source_event,
        observation,
        None,
    )])
}

fn decode_name_wrapped_dns_name(data: &[u8]) -> Result<Vec<u8>> {
    let (dns_name, _owner, _fuses, _expiry) =
        crate::evm_abi::abi_decode_params::<(SolBytes, SolAddress, Uint<32>, Uint<64>)>(
            data,
            "NameWrapped data is malformed",
        )?;
    Ok(dns_name.to_vec())
}

fn decode_alias_changed_names(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let (from_name, to_name) = crate::evm_abi::abi_decode_params::<(SolBytes, SolBytes)>(
        data,
        "AliasChanged data is malformed",
    )?;
    Ok((from_name.to_vec(), to_name.to_vec()))
}

fn decode_named_resource_name(data: &[u8], source_event: &str) -> Result<Vec<u8>> {
    match source_event {
        SOURCE_EVENT_NAMED_RESOURCE | SOURCE_EVENT_NAMED_ADDR_RESOURCE => {
            let (name,) = crate::evm_abi::abi_decode_params::<(SolBytes,)>(
                data,
                "named resolver resource data is malformed",
            )?;
            Ok(name.to_vec())
        }
        SOURCE_EVENT_NAMED_TEXT_RESOURCE => {
            let (name, _key) = crate::evm_abi::abi_decode_params::<(SolBytes, SolString)>(
                data,
                "named resolver text resource data is malformed",
            )?;
            Ok(name.to_vec())
        }
        _ => bail!("unsupported named resolver preimage event {source_event}"),
    }
}

fn build_preimage_observed_normalized_event(
    raw_log: &WatchedRawLogRow,
    source_event: &str,
    observation: PreimageObservation,
    observation_slot: Option<&str>,
) -> NormalizedEvent {
    let identity_suffix = observation_slot
        .map(|slot| format!(":{}", slot))
        .unwrap_or_default();
    let mut after_state = json!({
        "source_event": source_event,
        "dns_encoded_name": observation.dns_encoded_name,
        "decoded_name": observation.decoded_name,
        "labelhashes": observation.labelhashes,
        "namehash": observation.namehash,
    });
    if let Some(observation_slot) = observation_slot
        && let Some(object) = after_state.as_object_mut()
    {
        object.insert(
            "observation_slot".to_owned(),
            Value::String(observation_slot.to_owned()),
        );
    }
    NormalizedEvent {
        event_identity: format!(
            "raw_log_preimage_observed:{}:{}:{}:{}:{}{}",
            raw_log.source_manifest_id,
            raw_log.block_hash,
            raw_log.transaction_hash,
            raw_log.log_index,
            raw_log.emitting_address,
            identity_suffix
        ),
        namespace: raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PREIMAGE_OBSERVED.to_owned(),
        source_family: raw_log.source_family.clone(),
        manifest_version: raw_log.manifest_version,
        source_manifest_id: Some(raw_log.source_manifest_id),
        chain_id: Some(raw_log.chain_id.clone()),
        block_number: Some(raw_log.block_number),
        block_hash: Some(raw_log.block_hash.clone()),
        transaction_hash: Some(raw_log.transaction_hash.clone()),
        log_index: Some(raw_log.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": raw_log.chain_id.clone(),
            "block_hash": raw_log.block_hash.clone(),
            "block_number": raw_log.block_number,
            "transaction_hash": raw_log.transaction_hash.clone(),
            "transaction_index": raw_log.transaction_index,
            "log_index": raw_log.log_index,
            "emitting_address": raw_log.emitting_address.clone(),
            "topic0": raw_log.topics.first().cloned(),
            "topic1": raw_log.topics.get(1).cloned(),
            "topic2": raw_log.topics.get(2).cloned(),
            "data_hex": hex_string_without_prefix(&raw_log.data),
        }),
        derivation_kind: DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION.to_owned(),
        canonicality_state: raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    }
}

fn validate_indexed_bytes_hash(
    raw_log: &WatchedRawLogRow,
    topic_index: usize,
    bytes: &[u8],
    context: &str,
) -> Result<()> {
    let Some(indexed_hash) = raw_log.topics.get(topic_index) else {
        return Ok(());
    };
    let observed_hash = keccak256_hex(bytes);
    if !indexed_hash.eq_ignore_ascii_case(&observed_hash) {
        bail!(
            "{context} {} does not match decoded bytes hash {} for chain {} block {} log {}",
            indexed_hash,
            observed_hash,
            raw_log.chain_id,
            raw_log.block_hash,
            raw_log.log_index
        );
    }
    Ok(())
}
