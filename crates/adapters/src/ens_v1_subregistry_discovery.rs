use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use bigname_manifests::{FullDiscoveryReconciliationOptions, reconcile_discovery_observations};
use sqlx::PgPool;

use crate::checkpoint_context::{
    AdapterCheckpointContext, StartupAdapterProgress, record_startup_adapter_progress,
};
use crate::registry_migration_cache::MigratedRegistryNodes;

mod application;
mod assignment;
mod checkpoint;
mod constants;
mod emitter;
mod entrypoints;
mod event;
mod hex_topic;
mod loader;
mod migration_guard;
mod mode;
mod reconciliation;
mod replay;
mod scope;
mod startup;
mod types;

use application::apply_registry_raw_logs;
use assignment::{
    ObservedRegistryAssignment, build_registry_assignment, ens_v1_resolver_discovery_source,
    ens_v1_subregistry_discovery_source,
};
use checkpoint::SubregistryReplayCheckpoint;
use constants::*;
use emitter::{emit_registry_changed_events, emit_registry_changed_events_from_checkpoint};
use hex_topic::ZERO_ADDRESS;
use loader::{
    load_active_emitters, load_registry_raw_log_checkpoint_page, load_registry_raw_logs,
    stream_registry_raw_logs, stream_registry_raw_logs_through_block,
};
use migration_guard::{
    current_registry_emitter, registry_migration_guard_action, rewrite_old_registry_assignment,
};
use mode::{DiscoveryEdgeMutation, EnsV1SubregistryDiscoverySyncOutcome};
use reconciliation::{
    count_active_assignments_with_progress,
    reconcile_subregistry_discovery_from_assignments_through_block,
    reconcile_subregistry_discovery_from_checkpoint,
};
use scope::{load_migrated_registry_nodes_before_block, normalized_registry_source_scope_targets};
pub use types::EnsV1SubregistryDiscoverySyncSummary;

pub use crate::checkpoint_context::{
    ReplayAdapterCheckpointContext, StartupAdapterCheckpointContext,
};
pub use checkpoint::clear_replay_adapter_checkpoints;
pub use entrypoints::{
    sync_ens_v1_subregistry_discovery, sync_ens_v1_subregistry_discovery_through_block,
    sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch,
    sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch_and_progress,
};
pub use replay::{
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint,
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit,
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit_and_progress,
};
pub use startup::{
    sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit,
    sync_ens_v1_subregistry_discovery_with_startup_checkpoint_and_log_limit_and_progress,
};

async fn sync_ens_v1_subregistry_discovery_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    discovery_edge_mutation: DiscoveryEdgeMutation,
    full_source_through_block: Option<i64>,
    full_source_expected_admission_epoch: Option<i64>,
    checkpoint_context: Option<&AdapterCheckpointContext>,
    checkpoint_page_limit: i64,
    checkpoint_progress_log_every_pages: Option<usize>,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1SubregistryDiscoverySyncOutcome> {
    ensure!(
        full_source_through_block.is_none()
            || (!restrict_to_block_hashes
                && source_scope.is_none()
                && discovery_edge_mutation.reconciles()
                && checkpoint_context.is_none()),
        "target-bounded ENSv1 registry reconciliation requires an uncheckpointed complete-source pass"
    );
    ensure!(
        full_source_expected_admission_epoch.is_none() || full_source_through_block.is_some(),
        "an expected discovery-admission epoch is valid only for target-bounded ENSv1 registry reconciliation"
    );
    if let Some(through_block) = full_source_through_block {
        ensure!(
            through_block >= 0,
            "target-bounded ENSv1 registry reconciliation block must not be negative"
        );
    }
    let source_scope = source_scope.map(normalized_registry_source_scope_targets);
    let use_replay_checkpoint = !restrict_to_block_hashes
        && source_scope.is_none()
        && discovery_edge_mutation.reconciles()
        && checkpoint_context.is_some();
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok((
            EnsV1SubregistryDiscoverySyncSummary::default(),
            false,
            bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
        ));
    }

    let emitters = load_active_emitters(
        pool,
        chain,
        source_scope.as_deref(),
        full_source_through_block.is_some(),
        startup_progress,
    )
    .await?;
    record_startup_adapter_progress(pool, startup_progress).await?;
    let current_registry = current_registry_emitter(&emitters, full_source_through_block).cloned();
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut scanned_log_count = 0;
    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
    let mut active_checkpoint = if use_replay_checkpoint {
        let checkpoint = checkpoint_context.expect("checkpoint presence was checked");
        let active_checkpoint =
            SubregistryReplayCheckpoint::load_or_start(pool, chain, checkpoint).await?;
        if let Some(summary) = active_checkpoint.completed_summary()? {
            return Ok((
                summary,
                false,
                bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
            ));
        }
        if !active_checkpoint.stream_complete() {
            // Assignments stay DB-staged and page-local; only the migrated-node
            // guard state must be resident while streaming (#168).
            migrated_registry_nodes = active_checkpoint
                .load_staged_migrated_registry_nodes(pool)
                .await?;
        }
        scanned_log_count = active_checkpoint.scanned_log_count();
        matched_log_count = active_checkpoint.matched_log_count();
        Some(active_checkpoint)
    } else {
        None
    };
    if !restrict_to_block_hashes && source_scope.is_none() {
        if let Some(checkpoint) = active_checkpoint.as_mut() {
            if !emitters.is_empty()
                || (checkpoint.context.is_startup() && checkpoint.last_position().is_none())
            {
                let (scanned, matched) = sync_checkpointed_registry_raw_logs(
                    pool,
                    chain,
                    &emitters,
                    current_registry.as_ref(),
                    checkpoint,
                    checkpoint_page_limit,
                    checkpoint_progress_log_every_pages,
                    startup_progress,
                    &mut migrated_registry_nodes,
                )
                .await?;
                scanned_log_count = scanned;
                matched_log_count = matched;
            }
        } else if !emitters.is_empty() {
            if let Some(through_block) = full_source_through_block {
                scanned_log_count = stream_registry_raw_logs_through_block(
                    pool,
                    chain,
                    &emitters,
                    through_block,
                    checkpoint_page_limit,
                    startup_progress,
                    |raw_log| {
                        let applied = apply_registry_raw_log(
                            &raw_log,
                            chain,
                            current_registry.as_ref(),
                            &mut latest_assignments,
                            &mut migrated_registry_nodes,
                        )?;
                        if applied.matched {
                            matched_log_count += 1;
                        }
                        Ok(())
                    },
                )
                .await?;
            } else {
                scanned_log_count = stream_registry_raw_logs(pool, chain, &emitters, |raw_log| {
                    let applied = apply_registry_raw_log(
                        &raw_log,
                        chain,
                        current_registry.as_ref(),
                        &mut latest_assignments,
                        &mut migrated_registry_nodes,
                    )?;
                    if applied.matched {
                        matched_log_count += 1;
                    }
                    Ok(())
                })
                .await?;
            }
        }
    } else {
        let raw_logs = load_registry_raw_logs(
            pool,
            chain,
            &emitters,
            restrict_to_block_hashes,
            block_hashes,
            source_scope.as_deref(),
        )
        .await?;
        if source_scope.is_some() && raw_logs.is_empty() {
            return Ok((
                EnsV1SubregistryDiscoverySyncSummary::default(),
                false,
                bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
            ));
        }
        scanned_log_count = raw_logs.len();
        let preload_migrated_registry_nodes = raw_logs
            .iter()
            .any(|raw_log| raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD));
        if preload_migrated_registry_nodes {
            let first_selected_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
            if let Some(first_selected_block) = first_selected_block {
                migrated_registry_nodes = load_migrated_registry_nodes_before_block(
                    pool,
                    chain,
                    &emitters,
                    first_selected_block,
                )
                .await?
            }
        }
        matched_log_count += apply_registry_raw_logs(
            pool,
            &raw_logs,
            chain,
            current_registry.as_ref(),
            &mut latest_assignments,
            &mut migrated_registry_nodes,
            startup_progress,
        )
        .await?;
    }

    let finalize_from_checkpoint = active_checkpoint
        .as_ref()
        .is_some_and(SubregistryReplayCheckpoint::stream_complete);
    // An incomplete checkpoint stream must never feed any reconcile: falling
    // through to the full-source in-memory path with an empty or partial
    // assignment map would treat every unseen assignment as absent and
    // deactivate the source's edges wholesale (e.g. when the registry
    // manifest was superseded mid-replay and no active emitters remain).
    ensure!(
        active_checkpoint.is_none() || finalize_from_checkpoint,
        "checkpointed ENSv1 registry replay for {chain} has an incomplete stream (no active \
         registry emitters?); refusing to reconcile from a partial staged assignment set"
    );
    if finalize_from_checkpoint {
        // Checkpoint mode never populates `latest_assignments`; release the
        // restored migrated-node guard state before the paged finalize.
        drop(std::mem::replace(
            &mut migrated_registry_nodes,
            MigratedRegistryNodes::empty(),
        ));
    }

    let active_observation_count = if finalize_from_checkpoint {
        let checkpoint = active_checkpoint
            .as_ref()
            .expect("finalizing checkpoint should be present");
        checkpoint.ensure_raw_log_input_current(pool).await?;
        checkpoint
            .active_assignment_count(pool, &discovery_sources)
            .await?
    } else {
        count_active_assignments_with_progress(pool, &latest_assignments, startup_progress).await?
    };

    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count,
        matched_log_count,
        active_observation_count,
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        total_normalized_event_count: 0,
        total_normalized_event_inserted_count: 0,
    };
    if discovery_edge_mutation.reconciles() {
        if finalize_from_checkpoint {
            reconcile_subregistry_discovery_from_checkpoint(
                pool,
                active_checkpoint
                    .as_ref()
                    .expect("finalizing checkpoint should be present"),
                &discovery_sources,
                &mut reconciliation,
                startup_progress,
            )
            .await?;
            if reconciliation.inserted_edge_count > 0 {
                let checkpoint = active_checkpoint
                    .as_ref()
                    .expect("finalizing checkpoint should be present");
                checkpoint::delete_checkpoint(pool, &checkpoint.chain, &checkpoint.context).await?;
                return Ok((
                    reconciliation,
                    true,
                    bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
                ));
            }
        } else if source_scope.is_some() {
            let observations = latest_assignments
                .values()
                .map(ObservedRegistryAssignment::discovery_observation)
                .collect::<Result<Vec<_>>>()?;
            for discovery_source in &discovery_sources {
                let source_observations = observations
                    .iter()
                    .filter(|observation| observation.discovery_source == discovery_source.as_str())
                    .cloned()
                    .collect::<Vec<_>>();
                let source_reconciliation =
                    bigname_manifests::reconcile_scoped_discovery_observations(
                        pool,
                        discovery_source,
                        &source_observations,
                    )
                    .await?;
                reconciliation.active_edge_count += source_reconciliation.active_edge_count;
                reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
                reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
                reconciliation.deactivated_edge_count +=
                    source_reconciliation.deactivated_edge_count;
            }
        } else if let Some(through_block) = full_source_through_block {
            reconcile_subregistry_discovery_from_assignments_through_block(
                pool,
                chain,
                &latest_assignments,
                &discovery_sources,
                through_block,
                full_source_expected_admission_epoch,
                &mut reconciliation,
                startup_progress,
            )
            .await?;
        } else {
            for discovery_source in &discovery_sources {
                let source_observations = latest_assignments
                    .values()
                    .filter(|assignment| assignment.discovery_source == discovery_source.as_str())
                    .map(ObservedRegistryAssignment::discovery_observation)
                    .collect::<Result<Vec<_>>>()?;
                let source_reconciliation = reconcile_discovery_observations(
                    pool,
                    discovery_source,
                    &source_observations,
                    FullDiscoveryReconciliationOptions::default(),
                )
                .await?;
                reconciliation.active_edge_count += source_reconciliation.active_edge_count;
                reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
                reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
                reconciliation.deactivated_edge_count +=
                    source_reconciliation.deactivated_edge_count;
            }
        }
    }

    let event_summary = if finalize_from_checkpoint {
        emit_registry_changed_events_from_checkpoint(
            pool,
            active_checkpoint
                .as_ref()
                .expect("finalizing checkpoint should be present"),
            &discovery_sources,
            startup_progress,
        )
        .await?
    } else {
        emit_registry_changed_events(
            pool,
            &latest_assignments,
            &discovery_sources,
            startup_progress,
        )
        .await?
    };
    reconciliation.total_normalized_event_count = event_summary.synced_count;
    reconciliation.total_normalized_event_inserted_count = event_summary.inserted_count;

    if let Some(checkpoint) = active_checkpoint.as_mut() {
        checkpoint.mark_completed(pool, &reconciliation).await?;
        record_startup_adapter_progress(pool, startup_progress).await?;
    }

    Ok((
        reconciliation,
        false,
        bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
    ))
}

async fn sync_checkpointed_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[loader::ActiveEmitter],
    current_registry: Option<&loader::ActiveEmitter>,
    checkpoint: &mut SubregistryReplayCheckpoint,
    checkpoint_page_limit: i64,
    progress_log_every_pages: Option<usize>,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<(usize, usize)> {
    if checkpoint.stream_complete() {
        return Ok((
            checkpoint.scanned_log_count(),
            checkpoint.matched_log_count(),
        ));
    }

    let mut start_after = checkpoint.last_position();
    let mut scanned_log_count = checkpoint.scanned_log_count();
    let mut matched_log_count = checkpoint.matched_log_count();
    let mut page_count = 0usize;
    loop {
        let page = load_registry_raw_log_checkpoint_page(
            pool,
            chain,
            emitters,
            checkpoint.range_start_block_number(),
            checkpoint.target_block_number(),
            start_after.as_ref(),
            checkpoint_page_limit,
        )
        .await?;
        let Some(last_position) = page.last_position else {
            checkpoint
                .mark_stream_complete(pool, scanned_log_count, matched_log_count)
                .await?;
            record_startup_adapter_progress(pool, startup_progress).await?;
            break;
        };

        // Page-local latest assignments: pages are processed in stream order
        // and `save_progress` upserts each page's changed keys, so
        // last-write-wins across pages is preserved in the staged rows while
        // memory stays page-bounded (#168); only the migrated-node guard accumulates.
        let mut page_assignments =
            BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
        let mut migrated_nodes = Vec::<String>::new();
        for raw_log in &page.raw_logs {
            let applied = apply_registry_raw_log(
                raw_log,
                chain,
                current_registry,
                &mut page_assignments,
                migrated_registry_nodes,
            )?;
            scanned_log_count += 1;
            if applied.matched {
                matched_log_count += 1;
            }
            if let Some(migrated_node) = applied.migrated_node {
                migrated_nodes.push(migrated_node);
            }
        }

        checkpoint
            .save_progress(
                pool,
                &last_position,
                scanned_log_count,
                matched_log_count,
                &page_assignments,
                &migrated_nodes,
                migrated_registry_nodes.delta_nodes().count(),
            )
            .await?;
        page_count += 1;
        startup::log_checkpoint_stream_progress(
            progress_log_every_pages,
            checkpoint,
            page_count,
            checkpoint_page_limit,
            &last_position,
            scanned_log_count,
            matched_log_count,
        );
        record_startup_adapter_progress(pool, startup_progress).await?;
        start_after = Some(last_position);
    }

    Ok((scanned_log_count, matched_log_count))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AppliedRegistryRawLog {
    matched: bool,
    migrated_node: Option<String>,
}

fn apply_registry_raw_log(
    raw_log: &loader::RegistryRawLogRow,
    chain: &str,
    current_registry: Option<&loader::ActiveEmitter>,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<AppliedRegistryRawLog> {
    let migration_guard = registry_migration_guard_action(raw_log)?;
    if migration_guard.suppressed_by(migrated_registry_nodes) {
        return Ok(AppliedRegistryRawLog::default());
    }

    let Some(mut assignment) = build_registry_assignment(raw_log, chain)? else {
        let migrated_node = migration_guard.mark_migrated_node().and_then(|node| {
            migrated_registry_nodes
                .insert(node.to_owned())
                .then(|| node.to_owned())
        });
        return Ok(AppliedRegistryRawLog {
            migrated_node,
            ..AppliedRegistryRawLog::default()
        });
    };
    rewrite_old_registry_assignment(&mut assignment, current_registry, &migration_guard);
    let assignment_key = format!(
        "{}:{}",
        assignment.discovery_source, assignment.observation_key
    );
    latest_assignments.insert(assignment_key, assignment);
    let migrated_node = migration_guard.mark_migrated_node().and_then(|node| {
        migrated_registry_nodes
            .insert(node.to_owned())
            .then(|| node.to_owned())
    });
    Ok(AppliedRegistryRawLog {
        matched: true,
        migrated_node,
    })
}

#[cfg(test)]
mod tests;
