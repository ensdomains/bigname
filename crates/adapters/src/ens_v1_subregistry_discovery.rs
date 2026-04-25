use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use bigname_manifests::reconcile_discovery_observations;
use sqlx::PgPool;

mod assignment;
mod emitter;
mod event;
mod hex_topic;
mod loader;
mod migration_guard;
mod scope;

use assignment::{
    build_registry_assignment, ens_v1_resolver_discovery_source,
    ens_v1_subregistry_discovery_source,
};
use emitter::emit_registry_changed_events;
use hex_topic::{ZERO_ADDRESS, normalize_address};
use loader::{load_active_emitters, load_registry_raw_logs};
use migration_guard::{registry_migration_guard_action, rewrite_old_registry_assignment};
use scope::{
    load_active_registry_edge_observations_excluding_keys,
    load_migrated_registry_nodes_before_block, normalized_registry_source_scope_targets,
};

const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
#[cfg(test)]
const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
#[cfg(test)]
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
const RESOLVER_EDGE_KIND: &str = "resolver";
const CONTRACT_ROLE_REGISTRY: &str = "registry";
const CONTRACT_ROLE_REGISTRY_OLD: &str = "registry_old";
const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED: &str = "ens_v1_subregistry_changed";
const DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED: &str = "ens_v1_registry_resolver_changed";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1SubregistryDiscoverySyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub active_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
}

pub async fn sync_ens_v1_subregistry_discovery(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
    )
    .await
}

impl EnsV1SubregistryDiscoverySyncSummary {
    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            DiscoveryEdgeMutation::Reconcile,
        )
        .await
    }

    pub async fn sync_for_block_hashes_without_discovery_reconciliation(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            DiscoveryEdgeMutation::Skip,
        )
        .await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoveryEdgeMutation {
    Reconcile,
    Skip,
}

async fn sync_ens_v1_subregistry_discovery_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    discovery_edge_mutation: DiscoveryEdgeMutation,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let source_scope = source_scope.map(normalized_registry_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            active_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
        });
    }

    let emitters = load_active_emitters(pool, chain).await?;
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
        return Ok(EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            active_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
        });
    }
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
    let mut migrated_registry_nodes = if source_scope.is_some() || restrict_to_block_hashes {
        let first_selected_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
        if let Some(first_selected_block) = first_selected_block {
            load_migrated_registry_nodes_before_block(pool, chain, &emitters, first_selected_block)
                .await?
        } else {
            HashSet::<String>::new()
        }
    } else {
        HashSet::<String>::new()
    };
    for raw_log in &raw_logs {
        let migration_guard = registry_migration_guard_action(raw_log)?;
        if migration_guard.suppressed_by(&migrated_registry_nodes) {
            continue;
        }

        let Some(mut assignment) = build_registry_assignment(raw_log, chain)? else {
            if let Some(node) = migration_guard.mark_migrated_node() {
                migrated_registry_nodes.insert(node.to_owned());
            }
            continue;
        };
        rewrite_old_registry_assignment(&mut assignment, &emitters, &migration_guard);
        matched_log_count += 1;
        latest_assignments.insert(
            format!(
                "{}:{}",
                assignment.observation.discovery_source, assignment.observation_key
            ),
            assignment,
        );
        if let Some(node) = migration_guard.mark_migrated_node() {
            migrated_registry_nodes.insert(node.to_owned());
        }
    }

    let observations = latest_assignments
        .values()
        .map(|assignment| assignment.observation.clone())
        .collect::<Vec<_>>();
    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count: raw_logs.len(),
        matched_log_count,
        active_observation_count: observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
    };
    if discovery_edge_mutation == DiscoveryEdgeMutation::Reconcile {
        let reconciliation_observations = if source_scope.is_some() {
            let touched_observation_keys = latest_assignments
                .values()
                .map(|assignment| {
                    (
                        assignment.observation.discovery_source.clone(),
                        assignment.observation_key.clone(),
                    )
                })
                .collect::<HashSet<_>>();
            let mut carry_forward = load_active_registry_edge_observations_excluding_keys(
                pool,
                &discovery_sources,
                &touched_observation_keys,
            )
            .await?;
            carry_forward.extend(observations.clone());
            carry_forward
        } else {
            observations.clone()
        };

        for discovery_source in &discovery_sources {
            let source_observations = reconciliation_observations
                .iter()
                .filter(|observation| observation.discovery_source == discovery_source.as_str())
                .cloned()
                .collect::<Vec<_>>();
            let source_reconciliation =
                reconcile_discovery_observations(pool, discovery_source, &source_observations)
                    .await?;
            reconciliation.active_edge_count += source_reconciliation.active_edge_count;
            reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
            reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
            reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
        }
    }

    emit_registry_changed_events(pool, &latest_assignments, &discovery_sources).await?;

    Ok(reconciliation)
}

#[cfg(test)]
mod tests;
