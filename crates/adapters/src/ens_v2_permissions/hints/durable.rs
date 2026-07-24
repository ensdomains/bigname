use std::collections::HashMap;

use alloy_primitives::hex;
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::ens_v2_common::{
    ActiveEmitter, active_emitter_for_log, emitters_by_address, normalize_address,
};
use crate::evm_abi::keccak_signature_hex;

use super::{required_text, resolver_resource_hint};
use crate::ens_v2_permissions::constants::{
    ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE, ABI_EVENT_NAMED_RESOURCE_SIGNATURE,
    ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE,
};
use crate::ens_v2_permissions::decode::build_permissions_observation;
use crate::ens_v2_permissions::types::{
    PermissionsObservation, PermissionsRawLogRow, ResolverResourceHint,
};
use crate::ens_v2_permissions::util::dns_decode;

pub(super) async fn load_durable_resolver_resource_hint(
    pool: &PgPool,
    raw_log: &PermissionsRawLogRow,
    upstream_resource: &str,
    active_emitters: &[ActiveEmitter],
) -> Result<Option<ResolverResourceHint>> {
    let event_topics = ActiveManifestEventTopic0sBySignature::new(HashMap::from([
        (
            ABI_EVENT_NAMED_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE),
        ),
    ]));
    let row = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            i64,
            String,
            String,
            i64,
            i64,
            Value,
            Value,
        ),
    >(
        r#"
        WITH RECURSIVE candidates AS NOT MATERIALIZED (
            SELECT
                event.block_number,
                event.block_hash,
                event.transaction_hash,
                event.log_index,
                event.namespace,
                event.source_family,
                event.manifest_version,
                event.source_manifest_id,
                event.raw_fact_ref,
                event.after_state,
                event.event_identity
            FROM normalized_events event
            WHERE event.chain_id = $1
              AND event.derivation_kind = 'raw_log_preimage_observation'
              AND event.event_kind = 'PreimageObserved'
              AND event.namespace = $2
              AND event.source_family = $3
              AND LOWER(event.raw_fact_ref ->> 'emitting_address') = LOWER($4)
              AND LOWER(event.raw_fact_ref ->> 'topic1') = LOWER($5)
              AND event.after_state ->> 'source_event' IN (
                  'NamedResource',
                  'NamedTextResource',
                  'NamedAddrResource'
              )
              AND (
                  event.block_number,
                  (event.raw_fact_ref ->> 'transaction_index')::BIGINT,
                  event.log_index
              ) < ($6::BIGINT, $7::BIGINT, $8::BIGINT)
              AND event.block_number IS NOT NULL
              AND event.block_hash IS NOT NULL
              AND event.transaction_hash IS NOT NULL
              AND event.log_index IS NOT NULL
              AND event.source_manifest_id IS NOT NULL
              AND event.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        candidate_floor AS (
            SELECT MIN(block_number) AS block_number
            FROM candidates
        ),
        selected_path AS (
            SELECT
                descendant.chain_id,
                descendant.block_hash,
                descendant.parent_hash,
                descendant.block_number,
                0::BIGINT AS depth,
                candidate_floor.block_number AS floor_block_number,
                descendant.block_number - candidate_floor.block_number AS max_depth
            FROM chain_lineage descendant
            CROSS JOIN candidate_floor
            WHERE candidate_floor.block_number IS NOT NULL
              AND descendant.chain_id = $1
              AND descendant.block_hash = $9
              AND descendant.block_number = $6
              AND descendant.block_number >= candidate_floor.block_number
              AND descendant.canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT
                parent.chain_id,
                parent.block_hash,
                parent.parent_hash,
                parent.block_number,
                selected_path.depth + 1,
                selected_path.floor_block_number,
                selected_path.max_depth
            FROM chain_lineage parent
            JOIN selected_path
              ON parent.chain_id = selected_path.chain_id
             AND parent.block_hash = selected_path.parent_hash
            WHERE selected_path.block_number > selected_path.floor_block_number
              AND selected_path.depth < selected_path.max_depth
              AND parent.block_number >= selected_path.floor_block_number
              AND parent.block_number < selected_path.block_number
              AND parent.canonicality_state <> 'orphaned'::canonicality_state
        )
        SELECT
            candidate.block_number,
            candidate.block_hash,
            candidate.transaction_hash,
            candidate.log_index,
            candidate.namespace,
            candidate.source_family,
            candidate.manifest_version,
            candidate.source_manifest_id,
            candidate.raw_fact_ref,
            candidate.after_state
        FROM candidates candidate
        JOIN selected_path
          ON selected_path.block_number = candidate.block_number
         AND selected_path.block_hash = candidate.block_hash
        ORDER BY
            candidate.block_number DESC,
            (candidate.raw_fact_ref ->> 'transaction_index')::BIGINT DESC,
            candidate.log_index DESC,
            candidate.event_identity DESC
        LIMIT 1
        "#,
    )
    .bind(&raw_log.chain_id)
    .bind(&raw_log.namespace)
    .bind(&raw_log.source_family)
    .bind(&raw_log.emitting_address)
    .bind(upstream_resource)
    .bind(raw_log.block_number)
    .bind(raw_log.transaction_index)
    .bind(raw_log.log_index)
    .bind(&raw_log.block_hash)
    .fetch_optional(pool)
    .await
    .context("failed to load durable ENSv2 named-resource observation")?;
    let Some((
        block_number,
        block_hash,
        transaction_hash,
        log_index,
        namespace,
        source_family,
        manifest_version,
        source_manifest_id,
        raw_fact_ref,
        after_state,
    )) = row
    else {
        return Ok(None);
    };

    let Some(transaction_index) = required_i64(&raw_fact_ref, "transaction_index") else {
        return Ok(None);
    };
    let Some(emitting_address) = required_text(&raw_fact_ref, "emitting_address") else {
        return Ok(None);
    };
    let emitting_address = normalize_address(emitting_address);
    let active_emitters_by_address = emitters_by_address(active_emitters);
    let Some(emitter) = active_emitters_by_address
        .get(&emitting_address)
        .and_then(|emitters| {
            active_emitter_for_log(emitters, block_number, transaction_index, log_index)
        })
    else {
        return Ok(None);
    };
    if emitter.contract_instance_id != raw_log.emitting_contract_instance_id
        || emitter.source_manifest_id != source_manifest_id
        || emitter.namespace != namespace
        || emitter.source_family != source_family
        || emitter.manifest_version != manifest_version
        || !durable_raw_fact_matches(
            &raw_fact_ref,
            raw_log,
            &block_hash,
            block_number,
            &transaction_hash,
            transaction_index,
            log_index,
            &emitting_address,
        )
    {
        return Ok(None);
    }
    let Some(source_event) = required_text(&after_state, "source_event") else {
        return Ok(None);
    };
    let Some(topics) = durable_topics(&raw_fact_ref, source_event) else {
        return Ok(None);
    };
    let Some(data) =
        required_text(&raw_fact_ref, "data_hex").and_then(|value| hex::decode(value).ok())
    else {
        return Ok(None);
    };
    let candidate = PermissionsRawLogRow {
        chain_id: raw_log.chain_id.clone(),
        block_hash,
        block_number,
        transaction_hash,
        transaction_index,
        log_index,
        emitting_address,
        emitting_contract_instance_id: emitter.contract_instance_id,
        topics,
        data,
        canonicality_state: raw_log.canonicality_state,
        source_manifest_id,
        namespace,
        source_family,
        manifest_version,
    };
    let Ok(Some(observation)) = build_permissions_observation(&candidate, &event_topics) else {
        return Ok(None);
    };
    let Some(hint) = resolver_hint_from_durable_observation(
        &candidate,
        &observation,
        upstream_resource,
        source_event,
    )?
    else {
        return Ok(None);
    };
    let Some(dns_encoded_name) = hint.dns_encoded_name.as_deref() else {
        return Ok(None);
    };
    let Some(normalized_name) = hint.normalized_name.as_deref() else {
        return Ok(None);
    };
    let durable_dns_encoded_name = format!("0x{}", hex::encode(dns_encoded_name));
    if required_text(&after_state, "dns_encoded_name") != Some(durable_dns_encoded_name.as_str())
        || required_text(&after_state, "decoded_name") != Some(normalized_name)
        || dns_decode(&dns_encoded_name).ok().as_deref() != Some(normalized_name)
    {
        return Ok(None);
    }
    Ok(Some(hint))
}

fn required_i64(value: &Value, key: &str) -> Option<i64> {
    value.get(key)?.as_i64()
}

#[allow(clippy::too_many_arguments)]
fn durable_raw_fact_matches(
    raw_fact_ref: &Value,
    current: &PermissionsRawLogRow,
    block_hash: &str,
    block_number: i64,
    transaction_hash: &str,
    transaction_index: i64,
    log_index: i64,
    emitting_address: &str,
) -> bool {
    required_text(raw_fact_ref, "kind") == Some("raw_log")
        && required_text(raw_fact_ref, "chain_id") == Some(current.chain_id.as_str())
        && required_text(raw_fact_ref, "block_hash") == Some(block_hash)
        && required_i64(raw_fact_ref, "block_number") == Some(block_number)
        && required_text(raw_fact_ref, "transaction_hash") == Some(transaction_hash)
        && required_i64(raw_fact_ref, "transaction_index") == Some(transaction_index)
        && required_i64(raw_fact_ref, "log_index") == Some(log_index)
        && required_text(raw_fact_ref, "emitting_address")
            .is_some_and(|address| address.eq_ignore_ascii_case(emitting_address))
}

fn durable_topics(raw_fact_ref: &Value, source_event: &str) -> Option<Vec<String>> {
    let mut topics = vec![
        required_text(raw_fact_ref, "topic0")?.to_owned(),
        required_text(raw_fact_ref, "topic1")?.to_owned(),
    ];
    if matches!(source_event, "NamedTextResource" | "NamedAddrResource") {
        topics.push(required_text(raw_fact_ref, "topic2")?.to_owned());
    }
    Some(topics)
}

pub(in crate::ens_v2_permissions) fn resolver_hint_from_durable_observation(
    raw_log: &PermissionsRawLogRow,
    observation: &PermissionsObservation,
    upstream_resource: &str,
    source_event: &str,
) -> Result<Option<ResolverResourceHint>> {
    Ok(match observation {
        PermissionsObservation::NamedResource { resource, name }
            if source_event == "NamedResource" && resource == upstream_resource =>
        {
            Some(resolver_resource_hint(
                raw_log,
                resource.clone(),
                name.clone(),
                "name",
                None,
                None,
            )?)
        }
        PermissionsObservation::NamedTextResource {
            resource,
            name,
            key_hash,
            key,
        } if source_event == "NamedTextResource" && resource == upstream_resource => {
            Some(resolver_resource_hint(
                raw_log,
                resource.clone(),
                name.clone(),
                "text",
                Some(key.clone()),
                Some(key_hash.clone()),
            )?)
        }
        PermissionsObservation::NamedAddrResource {
            resource,
            name,
            coin_type,
        } if source_event == "NamedAddrResource" && resource == upstream_resource => {
            Some(resolver_resource_hint(
                raw_log,
                resource.clone(),
                name.clone(),
                "addr",
                Some(coin_type.clone()),
                None,
            )?)
        }
        _ => None,
    })
}
