use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{NormalizedEvent, ens_v2_registry_resource_id, sql_row};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, types::Uuid};

use super::super::{
    constants::{
        ABI_EVENT_LABEL_REGISTERED_SIGNATURE, ABI_EVENT_LABEL_RESERVED_SIGNATURE,
        ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE, ABI_EVENT_SIGNATURES,
        ABI_EVENT_TOKEN_REGENERATED_SIGNATURE, ABI_EVENT_TOKEN_RESOURCE_SIGNATURE,
        EVENT_KIND_SUBREGISTRY_CHANGED, EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        SOURCE_FAMILY_ENS_V2_ROOT_L1,
    },
    decode::build_registry_observations,
    names::{observe_name, versionless_token_id},
    types::{ObservationRef, RegistryObservation, RegistryRawLogRow},
    util::{deterministic_uuid, normalize_address},
};
use crate::{
    adapter_manifest::ActiveManifestEventTopic0sBySignature, evm_abi::keccak_signature_hex,
};

struct TargetRequest {
    event_index: i64,
    chain_id: String,
    from_contract_instance_id: Uuid,
    target_address: String,
    block_number: i64,
    block_hash: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TransferRequest {
    event_index: usize,
    chain_id: String,
    namespace: String,
    source_family: String,
    source_manifest_id: i64,
    manifest_version: i64,
    registry_contract_instance_id: Uuid,
    registry_address: String,
    token_id: String,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
}

#[derive(Clone)]
struct LinkedTransferState {
    logical_name_id: String,
    upstream_resource: String,
    resource_id: Uuid,
    token_lineage_id: Uuid,
}

pub(in crate::ens_v2_registry) async fn hydrate_subregistry_event_target_ids(
    pool: &PgPool,
    events: &mut [NormalizedEvent],
) -> Result<()> {
    let requests = events
        .iter_mut()
        .enumerate()
        .filter(|(_, event)| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .filter_map(|(event_index, event)| {
            event.after_state["to_contract_instance_id"] = Value::Null;
            let target_address = event
                .after_state
                .get("subregistry")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)?;
            Some((event_index, event, target_address))
        })
        .map(|(event_index, event, target_address)| {
            let from_contract_instance_id = event
                .after_state
                .get("from_contract_instance_id")
                .and_then(Value::as_str)
                .with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing from_contract_instance_id",
                        event.event_identity
                    )
                })?
                .parse::<Uuid>()
                .with_context(|| {
                    format!(
                        "SubregistryChanged event {} has an invalid from_contract_instance_id",
                        event.event_identity
                    )
                })?;
            Ok(TargetRequest {
                event_index: i64::try_from(event_index)
                    .context("SubregistryChanged event index exceeds i64")?,
                chain_id: event.chain_id.clone().with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing chain_id",
                        event.event_identity
                    )
                })?,
                from_contract_instance_id,
                target_address: normalize_address(&target_address),
                block_number: event.block_number.with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing block_number",
                        event.event_identity
                    )
                })?,
                block_hash: event.block_hash.clone().with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing block_hash",
                        event.event_identity
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if requests.is_empty() {
        return hydrate_cold_token_control_events(pool, events).await;
    }

    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        WITH requested (
            event_index,
            chain_id,
            from_contract_instance_id,
            target_address,
            block_number,
            block_hash
        ) AS (
        "#,
    );
    query.push_values(&requests, |mut row, request| {
        row.push_bind(request.event_index)
            .push_bind(&request.chain_id)
            .push_bind(request.from_contract_instance_id)
            .push_bind(&request.target_address)
            .push_bind(request.block_number)
            .push_bind(&request.block_hash);
    });
    query.push(
        r#"
        )
        SELECT
            requested.event_index,
            discovery_edges.to_contract_instance_id
        FROM requested
        JOIN discovery_edges
         ON discovery_edges.chain_id = requested.chain_id
         AND discovery_edges.edge_kind = 'subregistry'
         AND discovery_edges.from_contract_instance_id = requested.from_contract_instance_id
         AND lower(discovery_edges.provenance ->> 'to_address') = requested.target_address
         AND (
             discovery_edges.deactivated_at IS NULL
             OR discovery_edges.active_to_block_number IS NOT NULL
         )
         AND NOT EXISTS (
             SELECT 1
             FROM chain_lineage edge_start
             WHERE edge_start.chain_id = discovery_edges.chain_id
               AND edge_start.block_hash = discovery_edges.active_from_block_hash
               AND edge_start.canonicality_state = 'orphaned'::canonicality_state
         )
         AND (
             discovery_edges.active_from_block_number < requested.block_number
             OR (
                 discovery_edges.active_from_block_number = requested.block_number
                 AND discovery_edges.active_from_block_hash = requested.block_hash
             )
         )
         AND (
             discovery_edges.active_to_block_number IS NULL
             OR discovery_edges.active_to_block_number > requested.block_number
             OR (
                 discovery_edges.active_to_block_number = requested.block_number
                 AND discovery_edges.active_to_block_hash = requested.block_hash
             )
         )
        ORDER BY requested.event_index, discovery_edges.discovery_edge_id
        "#,
    );

    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load historical ENSv2 subregistry discovery targets")?;
    let mut targets_by_event = BTreeMap::<i64, Uuid>::new();
    for row in rows {
        let event_index = row
            .try_get::<i64, _>("event_index")
            .context("failed to read SubregistryChanged event index")?;
        let target_id = row
            .try_get::<Uuid, _>("to_contract_instance_id")
            .context("failed to read SubregistryChanged target contract instance")?;
        if targets_by_event
            .insert(event_index, target_id)
            .is_some_and(|existing| existing != target_id)
        {
            bail!(
                "multiple historical discovery targets matched SubregistryChanged event index {event_index}"
            );
        }
    }

    for (event_index, target_id) in targets_by_event {
        let event = events
            .get_mut(usize::try_from(event_index).context("negative event index")?)
            .context("historical discovery target returned an unknown event index")?;
        event.after_state["to_contract_instance_id"] = Value::String(target_id.to_string());
    }

    hydrate_cold_token_control_events(pool, events).await?;

    Ok(())
}

async fn hydrate_cold_token_control_events(
    pool: &PgPool,
    events: &mut [NormalizedEvent],
) -> Result<()> {
    let requests = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.event_kind == EVENT_KIND_TOKEN_CONTROL_TRANSFERRED
                && event.after_state["registry_hydration_pending"] == Value::Bool(true)
        })
        .map(|(event_index, event)| transfer_request(event_index, event))
        .collect::<Result<Vec<_>>>()?;
    let mut hydrated = HashMap::<TransferRequest, LinkedTransferState>::new();
    for request in requests {
        let state = if let Some(state) = hydrated.get(&request) {
            state.clone()
        } else {
            let state = load_linked_transfer_state(pool, &request).await?;
            hydrated.insert(request.clone(), state.clone());
            state
        };
        let event = events
            .get_mut(request.event_index)
            .context("cold ENSv2 transfer request has an unknown event index")?;
        event.logical_name_id = Some(state.logical_name_id);
        event.resource_id = Some(state.resource_id);
        event.after_state["upstream_resource"] = Value::String(state.upstream_resource);
        event.after_state["token_lineage_id"] = Value::String(state.token_lineage_id.to_string());
        event
            .after_state
            .as_object_mut()
            .context("TokenControlTransferred after_state is not an object")?
            .remove("registry_hydration_pending");
    }
    Ok(())
}

fn transfer_request(event_index: usize, event: &NormalizedEvent) -> Result<TransferRequest> {
    let identity = &event.event_identity;
    Ok(TransferRequest {
        event_index,
        chain_id: event
            .chain_id
            .clone()
            .with_context(|| format!("TokenControlTransferred event {identity} is missing chain_id"))?,
        namespace: event.namespace.clone(),
        source_family: event.source_family.clone(),
        source_manifest_id: event.source_manifest_id.with_context(|| {
            format!("TokenControlTransferred event {identity} is missing source_manifest_id")
        })?,
        manifest_version: event.manifest_version,
        registry_contract_instance_id: event.after_state["registry_contract_instance_id"]
            .as_str()
            .with_context(|| {
                format!(
                    "TokenControlTransferred event {identity} is missing registry_contract_instance_id"
                )
            })?
            .parse()
            .with_context(|| {
                format!(
                    "TokenControlTransferred event {identity} has an invalid registry_contract_instance_id"
                )
            })?,
        registry_address: event.raw_fact_ref["emitting_address"]
            .as_str()
            .map(normalize_address)
            .with_context(|| {
                format!("TokenControlTransferred event {identity} is missing emitting_address")
            })?,
        token_id: event.after_state["token_id"]
            .as_str()
            .map(str::to_owned)
            .with_context(|| format!("TokenControlTransferred event {identity} is missing token_id"))?,
        block_number: event
            .block_number
            .with_context(|| format!("TokenControlTransferred event {identity} is missing block_number"))?,
        transaction_index: event.raw_fact_ref["transaction_index"]
            .as_i64()
            .with_context(|| {
                format!("TokenControlTransferred event {identity} is missing transaction_index")
            })?,
        log_index: event
            .log_index
            .with_context(|| format!("TokenControlTransferred event {identity} is missing log_index"))?,
    })
}

async fn load_linked_transfer_state(
    pool: &PgPool,
    request: &TransferRequest,
) -> Result<LinkedTransferState> {
    let event_topics = registry_event_topics();
    let predecessor_topics = [
        ABI_EVENT_LABEL_REGISTERED_SIGNATURE,
        ABI_EVENT_LABEL_RESERVED_SIGNATURE,
        ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE,
        ABI_EVENT_TOKEN_RESOURCE_SIGNATURE,
        ABI_EVENT_TOKEN_REGENERATED_SIGNATURE,
    ]
    .into_iter()
    .map(|signature| keccak_signature_hex(signature))
    .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT
            raw.chain_id,
            raw.block_hash,
            raw.block_number,
            lineage.block_timestamp,
            raw.transaction_hash,
            raw.transaction_index,
            raw.log_index,
            raw.emitting_address,
            raw.topics,
            raw.data,
            raw.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs raw
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw.chain_id
         AND lineage.block_hash = raw.block_hash
         AND lineage.block_number = raw.block_number
        WHERE raw.chain_id = $1
          AND lower(raw.emitting_address) = $2
          AND raw.topics[1] = ANY($3::TEXT[])
          AND raw.canonicality_state <> 'orphaned'::canonicality_state
          AND lineage.canonicality_state <> 'orphaned'::canonicality_state
          AND (raw.block_number, raw.transaction_index, raw.log_index)
              < ($4::BIGINT, $5::BIGINT, $6::BIGINT)
        ORDER BY raw.block_number, raw.transaction_index, raw.log_index, raw.raw_log_id
        "#,
    )
    .bind(&request.chain_id)
    .bind(&request.registry_address)
    .bind(&predecessor_topics)
    .bind(request.block_number)
    .bind(request.transaction_index)
    .bind(request.log_index)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load retained ENSv2 transfer predecessors for {} {}",
            request.registry_address, request.token_id
        )
    })?;

    let target_identity = versionless_token_id(&request.token_id);
    let mut lifecycle: Option<(String, ObservationRef, String, Option<String>)> = None;
    for row in rows {
        let raw_log = RegistryRawLogRow {
            chain_id: sql_row::get(&row, "chain_id")?,
            block_hash: sql_row::get(&row, "block_hash")?,
            block_number: sql_row::get(&row, "block_number")?,
            block_timestamp: sql_row::get(&row, "block_timestamp")?,
            transaction_hash: sql_row::get(&row, "transaction_hash")?,
            transaction_index: sql_row::get(&row, "transaction_index")?,
            log_index: sql_row::get(&row, "log_index")?,
            emitting_address: normalize_address(&sql_row::get::<String>(&row, "emitting_address")?),
            topics: sql_row::get(&row, "topics")?,
            data: sql_row::get(&row, "data")?,
            canonicality_state: sql_row::get(&row, "canonicality_state")?,
            emitting_contract_instance_id: request.registry_contract_instance_id,
            source_manifest_id: request.source_manifest_id,
            namespace: request.namespace.clone(),
            source_family: request.source_family.clone(),
            manifest_version: request.manifest_version,
            normalizer_version: String::new(),
        };
        for observation in build_registry_observations(&raw_log, &event_topics)? {
            match observation {
                RegistryObservation::LabelRegistered {
                    token_id,
                    label,
                    reference,
                    ..
                } if versionless_token_id(&token_id) == target_identity => {
                    lifecycle = Some((label, reference, token_id, None));
                }
                RegistryObservation::LabelReserved { token_id, .. }
                    if versionless_token_id(&token_id) == target_identity =>
                {
                    lifecycle = None;
                }
                RegistryObservation::LabelUnregistered { token_id, .. }
                    if versionless_token_id(&token_id) == target_identity =>
                {
                    lifecycle = None;
                }
                RegistryObservation::TokenResource {
                    token_id,
                    upstream_resource,
                    ..
                } if lifecycle
                    .as_ref()
                    .is_some_and(|(_, _, current_token, _)| current_token == &token_id) =>
                {
                    lifecycle.as_mut().expect("checked lifecycle").3 = Some(upstream_resource);
                }
                RegistryObservation::TokenRegenerated {
                    old_token_id,
                    new_token_id,
                    ..
                } if lifecycle
                    .as_ref()
                    .is_some_and(|(_, _, current_token, _)| current_token == &old_token_id) =>
                {
                    lifecycle.as_mut().expect("checked lifecycle").2 = new_token_id;
                }
                _ => {}
            }
        }
    }

    let (label, registration_ref, current_token, upstream_resource) = lifecycle.with_context(|| {
        format!(
            "ENSv2 TokenControlTransferred {} {} is missing a retained non-orphaned LabelRegistered predecessor",
            request.registry_address, request.token_id
        )
    })?;
    ensure!(
        current_token == request.token_id,
        "ENSv2 TokenControlTransferred {} {} is missing a complete retained TokenRegenerated predecessor chain",
        request.registry_address,
        request.token_id
    );
    let upstream_resource = upstream_resource.with_context(|| {
        format!(
            "ENSv2 TokenControlTransferred {} {} is missing a retained non-orphaned TokenResource predecessor",
            request.registry_address, request.token_id
        )
    })?;
    let suffix = load_registry_suffix(pool, request).await?;
    let full_name = if suffix.is_empty() {
        label.clone()
    } else {
        format!("{label}.{suffix}")
    };
    let name = observe_name(&request.namespace, &full_name, &registration_ref, &label)
        .with_context(|| format!("failed to normalize retained ENSv2 transfer name {full_name}"))?;
    let resource_id = ens_v2_registry_resource_id(
        &request.chain_id,
        request.registry_contract_instance_id,
        &upstream_resource,
    );
    let token_lineage_id = deterministic_uuid(&format!(
        "ens-v2-token-lineage:{}:{}:{}",
        request.chain_id, request.registry_contract_instance_id, upstream_resource
    ));
    Ok(LinkedTransferState {
        logical_name_id: name.logical_name_id,
        upstream_resource,
        resource_id,
        token_lineage_id,
    })
}

async fn load_registry_suffix(pool: &PgPool, request: &TransferRequest) -> Result<String> {
    if request.source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1 {
        return Ok(String::new());
    }
    let manifest_declared = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM manifest_contract_instances
            WHERE manifest_id = $1
              AND contract_instance_id = $2
        )
        "#,
    )
    .bind(request.source_manifest_id)
    .bind(request.registry_contract_instance_id)
    .fetch_one(pool)
    .await
    .context("failed to classify ENSv2 transfer registry emitter")?;
    if manifest_declared {
        return Ok("eth".to_owned());
    }

    let logical_name_id = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT edge.provenance ->> 'logical_name_id'
        FROM discovery_edges edge
        WHERE edge.chain_id = $1
          AND edge.edge_kind = 'subregistry'
          AND edge.to_contract_instance_id = $2
          AND (edge.deactivated_at IS NULL OR edge.active_to_block_number IS NOT NULL)
          AND NOT EXISTS (
              SELECT 1
              FROM chain_lineage edge_start
              WHERE edge_start.chain_id = edge.chain_id
                AND edge_start.block_hash = edge.active_from_block_hash
                AND edge_start.canonicality_state = 'orphaned'::canonicality_state
          )
          AND edge.active_from_block_number <= $3
          AND (edge.active_to_block_number IS NULL OR edge.active_to_block_number >= $3)
        ORDER BY edge.active_from_block_number DESC, edge.discovery_edge_id DESC
        LIMIT 1
        "#,
    )
    .bind(&request.chain_id)
    .bind(request.registry_contract_instance_id)
    .bind(request.block_number)
    .fetch_optional(pool)
    .await
    .context("failed to load discovered ENSv2 registry suffix for transfer")?
    .flatten()
    .with_context(|| {
        format!(
            "ENSv2 TokenControlTransferred {} {} has no retained canonical registry-parent discovery edge",
            request.registry_address, request.token_id
        )
    })?;
    logical_name_id
        .strip_prefix(&format!("{}:", request.namespace))
        .map(str::to_owned)
        .with_context(|| format!("ENSv2 registry parent {logical_name_id} has the wrong namespace"))
}

fn registry_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(
        ABI_EVENT_SIGNATURES
            .into_iter()
            .map(|signature| (signature.to_owned(), keccak_signature_hex(signature)))
            .collect(),
    )
}
