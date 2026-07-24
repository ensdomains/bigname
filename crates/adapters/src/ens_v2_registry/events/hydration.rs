use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use bigname_storage::{CanonicalityState, NormalizedEvent, ens_v2_registry_resource_id, sql_row};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgRow, types::Uuid};

use super::super::{
    constants::{
        ABI_EVENT_SIGNATURES, EVENT_KIND_SUBREGISTRY_CHANGED, EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
    },
    decode::build_registry_observations,
    names::{observe_name, versionless_token_id},
    types::{ObservationRef, RegistryObservation, RegistryRawLogRow},
    util::{deterministic_uuid, normalize_address},
};

mod history;

use crate::{
    adapter_manifest::ActiveManifestEventTopic0sBySignature, evm_abi::keccak_signature_hex,
};
use history::{load_linked_transfer_states, load_subregistry_target_rows};

struct TargetRequest {
    event_index: i64,
    chain_id: String,
    from_contract_instance_id: Uuid,
    target_address: String,
    block_number: i64,
    block_hash: String,
    transaction_index: i64,
    log_index: i64,
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
    block_hash: String,
    transaction_index: i64,
    log_index: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TransferHydrationKey {
    chain_id: String,
    namespace: String,
    source_family: String,
    source_manifest_id: i64,
    manifest_version: i64,
    registry_contract_instance_id: Uuid,
    registry_address: String,
    token_id: String,
    block_number: i64,
    block_hash: String,
    transaction_index: i64,
    log_index: i64,
}

impl From<&TransferRequest> for TransferHydrationKey {
    fn from(request: &TransferRequest) -> Self {
        Self {
            chain_id: request.chain_id.clone(),
            namespace: request.namespace.clone(),
            source_family: request.source_family.clone(),
            source_manifest_id: request.source_manifest_id,
            manifest_version: request.manifest_version,
            registry_contract_instance_id: request.registry_contract_instance_id,
            registry_address: request.registry_address.clone(),
            token_id: request.token_id.clone(),
            block_number: request.block_number,
            block_hash: request.block_hash.clone(),
            transaction_index: request.transaction_index,
            log_index: request.log_index,
        }
    }
}

#[derive(Clone)]
struct LinkedTransferState {
    logical_name_id: String,
    upstream_resource: String,
    resource_id: Uuid,
    token_lineage_id: Uuid,
}

struct TransferLifecycle {
    label: String,
    registration_ref: ObservationRef,
    current_token: String,
    upstream_resource: Option<String>,
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
                transaction_index: event.raw_fact_ref["transaction_index"]
                    .as_i64()
                    .with_context(|| {
                        format!(
                            "SubregistryChanged event {} is missing transaction_index",
                            event.event_identity
                        )
                    })?,
                log_index: event.log_index.with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing log_index",
                        event.event_identity
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if requests.is_empty() {
        return hydrate_cold_token_control_events(pool, events).await;
    }

    let mut targets_by_event = BTreeMap::<i64, Uuid>::new();
    for (event_index, target_id) in load_subregistry_target_rows(pool, &requests).await? {
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
    for request in &requests {
        let event = events
            .get(usize::try_from(request.event_index).context("negative event index")?)
            .context("historical discovery request has an unknown event index")?;
        if event.after_state["to_contract_instance_id"].is_null()
            && matches!(
                event.canonicality_state,
                CanonicalityState::Canonical
                    | CanonicalityState::Safe
                    | CanonicalityState::Finalized
            )
        {
            bail!(
                "canonical SubregistryChanged event {} has no matching selected-path discovery edge for target {}",
                event.event_identity,
                request.target_address
            );
        }
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
    let hydrated = load_linked_transfer_states(pool, &requests).await?;
    for request in requests {
        let state = hydrated
            .get(&TransferHydrationKey::from(&request))
            .cloned()
            .context("cold ENSv2 transfer hydration result is absent")?;
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
        block_hash: event
            .block_hash
            .clone()
            .with_context(|| format!("TokenControlTransferred event {identity} is missing block_hash"))?,
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

fn linked_transfer_state_from_rows(
    request: &TransferRequest,
    event_topics: &ActiveManifestEventTopic0sBySignature,
    rows: Vec<PgRow>,
    suffix: &str,
) -> Result<LinkedTransferState> {
    let target_identity = versionless_token_id(&request.token_id);
    let mut lifecycle = None::<TransferLifecycle>;
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
        for observation in build_registry_observations(&raw_log, event_topics)? {
            match observation {
                RegistryObservation::LabelRegistered {
                    token_id,
                    label,
                    reference,
                    ..
                } if versionless_token_id(&token_id) == target_identity => {
                    lifecycle = Some(TransferLifecycle {
                        label,
                        registration_ref: reference,
                        current_token: token_id,
                        upstream_resource: None,
                    });
                }
                RegistryObservation::LabelReserved { token_id, .. }
                | RegistryObservation::LabelUnregistered { token_id, .. }
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
                    .is_some_and(|lifecycle| lifecycle.current_token == token_id) =>
                {
                    lifecycle
                        .as_mut()
                        .expect("checked lifecycle")
                        .upstream_resource = Some(upstream_resource);
                }
                RegistryObservation::TokenRegenerated {
                    old_token_id,
                    new_token_id,
                    ..
                } if lifecycle
                    .as_ref()
                    .is_some_and(|lifecycle| lifecycle.current_token == old_token_id) =>
                {
                    lifecycle.as_mut().expect("checked lifecycle").current_token = new_token_id;
                }
                _ => {}
            }
        }
    }

    let lifecycle = lifecycle.with_context(|| {
        format!(
            "ENSv2 TokenControlTransferred {} {} is missing a retained non-orphaned LabelRegistered predecessor",
            request.registry_address, request.token_id
        )
    })?;
    ensure!(
        lifecycle.current_token == request.token_id,
        "ENSv2 TokenControlTransferred {} {} is missing a complete retained TokenRegenerated predecessor chain",
        request.registry_address,
        request.token_id
    );
    let upstream_resource = lifecycle.upstream_resource.with_context(|| {
        format!(
            "ENSv2 TokenControlTransferred {} {} is missing a retained non-orphaned TokenResource predecessor",
            request.registry_address, request.token_id
        )
    })?;
    let full_name = if suffix.is_empty() {
        lifecycle.label.clone()
    } else {
        format!("{}.{suffix}", lifecycle.label)
    };
    let name = observe_name(
        &request.namespace,
        &full_name,
        &lifecycle.registration_ref,
        &lifecycle.label,
    )
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

fn registry_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(
        ABI_EVENT_SIGNATURES
            .into_iter()
            .map(|signature| (signature.to_owned(), keccak_signature_hex(signature)))
            .collect(),
    )
}
