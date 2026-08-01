use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{NormalizedEvent, SurfaceBinding};
use sqlx::{PgPool, types::Uuid};

use super::{
    constants::ABI_EVENT_SIGNATURES,
    decode::build_registry_observations,
    emitters::load_active_emitters,
    events::{RegistryObservationContext, apply_registry_observation},
    live::RegistryReplayState,
    load::load_registry_raw_log_prefix,
    names::initial_registry_suffixes,
    types::{RegistryNameState, RegistryObservation, RegistryRawLogRow},
};
use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;

pub(super) fn requires_prior_registry_state(
    observations_by_log: &[Vec<RegistryObservation>],
) -> bool {
    // RegistryCreated and Upgraded are contract-scoped history. They do not
    // depend on token or registry-suffix state. Transfers use their separate
    // verified-history hydration path. Every other registry observation can
    // depend on an earlier transition, so restricted calls reconstruct the
    // retained prefix on demand.
    observations_by_log.iter().flatten().any(|observation| {
        !matches!(
            observation,
            RegistryObservation::RegistryCreated { .. }
                | RegistryObservation::Upgraded { .. }
                | RegistryObservation::TokenControlTransferred { .. }
        )
    })
}

pub(super) async fn reconstruct_prior_registry_state(
    pool: &PgPool,
    chain: &str,
    before: &RegistryRawLogRow,
) -> Result<RegistryReplayState> {
    let input_revision =
        load_proven_restricted_history_revision(pool, chain, before.block_number).await?;
    let emitters = load_active_emitters(pool, chain, None, true).await?;
    let manifest_ids = emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .collect::<Vec<_>>();
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &manifest_ids,
        &ABI_EVENT_SIGNATURES,
        "ENSv2 registry history",
    )
    .await?;
    let raw_logs = load_registry_raw_log_prefix(pool, chain, &emitters, before).await?;
    let initial_suffixes = initial_registry_suffixes(&emitters);
    let mut replay_state = RegistryReplayState {
        root_registry_addresses: initial_suffixes.keys().cloned().collect(),
        registry_suffix_by_address: initial_suffixes,
        registry_contract_by_address: emitters
            .iter()
            .map(|emitter| (emitter.address.clone(), emitter.contract_instance_id))
            .collect(),
        ..RegistryReplayState::default()
    };
    let RegistryReplayState {
        registry_suffix_by_address,
        root_registry_addresses,
        registry_contract_by_address,
        current_subregistry_by_parent_label,
        current_parent_claim_by_registry,
        entry_topology_by_registry_token,
        states_by_registry_token,
        state_keys_by_registry_namehash,
        token_aliases,
        current_token_alias_by_canonical_key,
    } = &mut replay_state;
    let mut linked_resource_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut retired_binding_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut closed_bindings = BTreeMap::<Uuid, SurfaceBinding>::new();
    let mut observations = Vec::<DiscoveryObservation>::new();
    let mut graph_events = Vec::<NormalizedEvent>::new();

    for raw_log in &raw_logs {
        let decoded = build_registry_observations(raw_log, &event_topics).with_context(|| {
            format!(
                "failed to decode retained ENSv2 registry history at block {} log {}",
                raw_log.block_number, raw_log.log_index
            )
        })?;
        let mut context = RegistryObservationContext {
            registry_suffix_by_address,
            root_registry_addresses,
            registry_contract_by_address,
            current_subregistry_by_parent_label,
            current_parent_claim_by_registry,
            entry_topology_by_registry_token,
            states_by_registry_token,
            state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases,
            current_token_alias_by_canonical_key,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        for observation in decoded {
            apply_registry_observation(observation, &mut context)?;
        }
        linked_resource_states.clear();
        retired_binding_states.clear();
        closed_bindings.clear();
        observations.clear();
        graph_events.clear();
    }

    let final_input_revision =
        load_proven_restricted_history_revision(pool, chain, before.block_number).await?;
    ensure!(
        input_revision == final_input_revision,
        "ENSv2 retained history changed while reconstructing incremental prior state for {chain} \
         through block {}; retry against a stable input revision",
        before.block_number
    );

    Ok(replay_state)
}

async fn load_proven_restricted_history_revision(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT retained.revision
        FROM raw_log_staging_input_revisions retained
        JOIN discovery_admission_epochs discovery
          ON discovery.chain_id = retained.chain_id
        WHERE retained.chain_id = $1
          AND retained.retained_history_complete
          AND retained.proven_retention_generation = retained.retention_generation
          AND retained.proven_discovery_admission_epoch = discovery.epoch
          AND retained.proven_through_block >= $2
        "#,
    )
    .bind(chain)
    .bind(through_block)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to validate retained ENSv2 history before incremental prior-state reconstruction for {chain}"
        )
    })?
    .with_context(|| {
        format!(
            "ENSv2 incremental prior-state reconstruction requires a current retained-history \
             proof for {chain} through block {through_block}"
        )
    })
}
