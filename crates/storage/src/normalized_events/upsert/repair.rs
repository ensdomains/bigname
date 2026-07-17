use std::collections::HashMap;

use anyhow::Result;
use sqlx::Postgres;

use super::super::types::NormalizedEvent;

#[path = "repair/ens_v1_authority_epoch_registry_owner.rs"]
mod ens_v1_authority_epoch_registry_owner;
#[path = "repair/ens_v1_authority_epoch_resolver_boundary.rs"]
mod ens_v1_authority_epoch_resolver_boundary;
#[path = "repair/ens_v1_registry_event_time.rs"]
mod ens_v1_registry_event_time;
#[path = "repair/ens_v1_registry_event_time_null_resource.rs"]
mod ens_v1_registry_event_time_null_resource;
#[path = "repair/ens_v1_registry_event_time_state.rs"]
mod ens_v1_registry_event_time_state;
#[path = "repair/ens_v1_registry_resolver_before_state.rs"]
mod ens_v1_registry_resolver_before_state;
#[path = "repair/ens_v1_registry_resolver_observation_key.rs"]
mod ens_v1_registry_resolver_observation_key;
#[path = "repair/ens_v1_renewal.rs"]
mod ens_v1_renewal;
#[path = "repair/ens_v1_reverse_resolver_before_state.rs"]
mod ens_v1_reverse_resolver_before_state;
#[path = "repair/ens_v1_same_tx_registration_setup.rs"]
mod ens_v1_same_tx_registration_setup;
#[path = "repair/ens_v1_wrapper_token_before_state.rs"]
mod ens_v1_wrapper_token_before_state;
#[path = "repair/primary_claim_source.rs"]
mod primary_claim_source;

pub(super) use ens_v1_authority_epoch_registry_owner::{
    ens_v1_authority_epoch_registry_owner_after_state_repair_allowed,
    repair_ens_v1_authority_epoch_registry_owner_after_states,
};
pub(super) use ens_v1_authority_epoch_resolver_boundary::{
    ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed,
    repair_ens_v1_authority_epoch_resolver_boundary_after_states,
};
pub(super) use ens_v1_registry_event_time::{
    ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed,
    ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registry_event_time_before_states,
    repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids,
    supersede_basenames_registry_boundary_derivation_change_events,
};
pub(super) use ens_v1_registry_event_time_null_resource::{
    ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registry_event_time_null_resource_ids,
};
pub(super) use ens_v1_registry_resolver_before_state::{
    ens_v1_registry_resolver_before_state_repair_allowed,
    repair_ens_v1_registry_resolver_before_states,
};
pub(super) use ens_v1_registry_resolver_observation_key::{
    ens_v1_registry_resolver_observation_key_after_state_repair_allowed,
    repair_ens_v1_registry_resolver_observation_key_after_states,
};
pub(super) use ens_v1_renewal::{
    ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed,
    ens_v1_unwrapped_authority_renewal_before_state_repair_allowed,
    ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registration_release_before_states,
    repair_ens_v1_unwrapped_authority_renewal_before_states,
    repair_ens_v1_unwrapped_authority_renewal_resource_ids,
};
pub(super) use ens_v1_reverse_resolver_before_state::{
    ens_v1_reverse_resolver_before_state_repair_allowed,
    repair_ens_v1_reverse_resolver_before_states,
};
pub(super) use ens_v1_same_tx_registration_setup::{
    ens_v1_same_tx_registration_setup_before_state_repair_allowed,
    repair_ens_v1_same_tx_registration_setup_before_states,
};
pub(super) use ens_v1_wrapper_token_before_state::{
    ens_v1_wrapper_token_before_state_repair_allowed, repair_ens_v1_wrapper_token_before_states,
};
pub(super) use primary_claim_source::{
    primary_claim_source_after_state_repair_allowed, repair_primary_claim_source_after_states,
};

pub(super) async fn repair_after_state_conflicts(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<usize> {
    let primary_claim_source =
        repair_primary_claim_source_after_states(executor, events, existing_by_identity).await?;
    let resolver_observation_key = repair_ens_v1_registry_resolver_observation_key_after_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let authority_epoch_registry_owner = repair_ens_v1_authority_epoch_registry_owner_after_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let authority_epoch_resolver_boundary =
        repair_ens_v1_authority_epoch_resolver_boundary_after_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let same_tx_registration_setup = repair_ens_v1_same_tx_registration_setup_before_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let wrapper_token_before_state =
        repair_ens_v1_wrapper_token_before_states(executor, events, existing_by_identity).await?;
    let registry_event_time_before_state =
        repair_ens_v1_unwrapped_authority_registry_event_time_before_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let reverse_resolver_before_state =
        repair_ens_v1_reverse_resolver_before_states(executor, events, existing_by_identity)
            .await?;
    let registry_resolver_before_state =
        repair_ens_v1_registry_resolver_before_states(executor, events, existing_by_identity)
            .await?;
    let renewal_before_state = repair_ens_v1_unwrapped_authority_renewal_before_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let registration_release_before_state =
        repair_ens_v1_unwrapped_authority_registration_release_before_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;

    Ok(primary_claim_source.len()
        + resolver_observation_key.len()
        + authority_epoch_registry_owner.len()
        + authority_epoch_resolver_boundary.len()
        + same_tx_registration_setup.len()
        + wrapper_token_before_state.len()
        + registry_event_time_before_state.len()
        + reverse_resolver_before_state.len()
        + registry_resolver_before_state.len()
        + renewal_before_state.len()
        + registration_release_before_state.len())
}

pub(super) async fn repair_resource_id_conflicts(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<usize> {
    let renewal_resource_ids = repair_ens_v1_unwrapped_authority_renewal_resource_ids(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let registry_event_time_resource_ids =
        repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let registry_event_time_null_resource_ids =
        repair_ens_v1_unwrapped_authority_registry_event_time_null_resource_ids(
            executor,
            events,
            existing_by_identity,
        )
        .await?;

    Ok(renewal_resource_ids.len()
        + registry_event_time_resource_ids.len()
        + registry_event_time_null_resource_ids.len())
}

pub(super) fn normalized_event_identity_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    primary_claim_source_after_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_registry_resolver_observation_key_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_authority_epoch_registry_owner_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_same_tx_registration_setup_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_wrapper_token_before_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_reverse_resolver_before_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_registry_resolver_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_boundary_manifest_metadata_mismatch_allowed(
            existing,
            incoming,
            differing_fields,
        )
}

pub(super) fn ens_v1_unwrapped_authority_boundary_manifest_metadata_mismatch_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["manifest_version", "source_manifest_id"] {
        return false;
    }
    existing.namespace == "ens"
        && incoming.namespace == "ens"
        && existing.chain_id.as_deref() == Some("ethereum-mainnet")
        && incoming.chain_id.as_deref() == Some("ethereum-mainnet")
        && existing.derivation_kind == "ens_v1_unwrapped_authority"
        && incoming.derivation_kind == "ens_v1_unwrapped_authority"
        && existing.source_family == "ens_v1_registry_l1"
        && incoming.source_family == "ens_v1_registry_l1"
        && matches!(
            existing.event_kind.as_str(),
            "AuthorityEpochChanged" | "ResolverChanged" | "SurfaceBound" | "SurfaceUnbound"
        )
        && existing.event_kind == incoming.event_kind
        && existing.transaction_hash.is_none()
        && incoming.transaction_hash.is_none()
        && existing.log_index.is_none()
        && incoming.log_index.is_none()
        && existing.source_manifest_id.is_some()
        && incoming.source_manifest_id.is_none()
        && incoming.manifest_version == 1
}
