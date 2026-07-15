use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryReconciliationSummary, discovery_observation_evm_event_position,
    reconcile_discovery_observations, reconcile_discovery_observations_through_block,
    reconcile_discovery_observations_through_block_with_expected_admission_epoch,
    reconcile_discovery_observations_with_expected_admission_epoch,
    reconcile_scoped_discovery_observation_transitions,
};
use serde_json::Value;
use sqlx::PgPool;

use super::{constants::ZERO_ADDRESS, util::normalize_address};

type MaterializedObservation = (String, i64, String, Option<i64>, Option<i64>, String);
const DISCOVERY_TRANSITION_CHUNK_SIZE: usize = 128;

pub(super) fn latest_discovery_observations(
    observations: Vec<DiscoveryObservation>,
) -> Result<Vec<DiscoveryObservation>> {
    let mut latest = BTreeMap::<String, DiscoveryObservation>::new();
    for observation in observations {
        let key = observation
            .provenance
            .get("observation_key")
            .and_then(Value::as_str)
            .context("ENSv2 discovery observation missing observation_key")?
            .to_owned();
        latest.insert(key, observation);
    }
    Ok(latest.into_values().collect())
}

#[cfg(test)]
pub(super) async fn reconcile_discovery_observation_history_by_source(
    pool: &PgPool,
    observations: &[DiscoveryObservation],
    reconcile_full_sources: bool,
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observation_history(
        pool,
        observations,
        reconcile_full_sources,
        &[],
        None,
        None,
        None,
    )
    .await
}

pub(super) async fn reconcile_discovery_observation_history_for_chain(
    pool: &PgPool,
    chain: &str,
    observations: &[DiscoveryObservation],
    reconcile_full_sources: bool,
    reconcile_through_block_number: Option<i64>,
    expected_initial_admission_epoch: Option<i64>,
) -> Result<DiscoveryReconciliationSummary> {
    let expected_sources = if reconcile_full_sources {
        vec![
            ens_v2_subregistry_discovery_source(chain),
            ens_v2_resolver_discovery_source(chain),
        ]
    } else {
        Vec::new()
    };
    reconcile_discovery_observation_history(
        pool,
        observations,
        reconcile_full_sources,
        &expected_sources,
        reconcile_through_block_number,
        expected_initial_admission_epoch,
        Some(chain),
    )
    .await
}

async fn reconcile_discovery_observation_history(
    pool: &PgPool,
    observations: &[DiscoveryObservation],
    reconcile_full_sources: bool,
    expected_sources: &[String],
    reconcile_through_block_number: Option<i64>,
    expected_initial_admission_epoch: Option<i64>,
    expected_chain: Option<&str>,
) -> Result<DiscoveryReconciliationSummary> {
    let mut by_source = BTreeMap::<String, Vec<DiscoveryObservation>>::new();
    for observation in observations {
        by_source
            .entry(observation.discovery_source.clone())
            .or_default()
            .push(observation.clone());
    }
    for discovery_source in expected_sources {
        by_source.entry(discovery_source.clone()).or_default();
    }

    let mut summary = DiscoveryReconciliationSummary {
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        admission_epoch_bump_count: 0,
        admitted_edges: Vec::new(),
    };
    for (discovery_source, source_observations) in by_source {
        let materialized_observations =
            load_materialized_observations(pool, &discovery_source).await?;
        let mut source_active_edge_count =
            load_active_discovery_edge_count(pool, &discovery_source).await?;
        let mut source_admitted_edge_count = 0;
        let mut source_admitted_edges = Vec::new();
        let mut source_latest_observations = BTreeMap::<String, DiscoveryObservation>::new();
        let mut transition_states = Vec::<Vec<DiscoveryObservation>>::new();
        for observation in &source_observations {
            let observation_key = observation
                .provenance
                .get("observation_key")
                .and_then(Value::as_str)
                .context("ENSv2 discovery observation missing observation_key")?
                .to_owned();
            let event_position = discovery_observation_evm_event_position(&observation.provenance)?;
            let observation_position = (
                observation_key.clone(),
                observation.active_from_block_number.with_context(|| {
                    format!(
                        "ENSv2 discovery observation {observation_key} is missing active_from_block_number"
                    )
                })?,
                observation.active_from_block_hash.clone().with_context(|| {
                    format!(
                        "ENSv2 discovery observation {observation_key} is missing active_from_block_hash"
                    )
                })?,
                event_position.map(|(transaction_index, _)| transaction_index),
                event_position.map(|(_, log_index)| log_index),
                normalize_address(&observation.to_address),
            );
            source_latest_observations.insert(observation_key.clone(), observation.clone());
            if materialized_observations.contains(&observation_position) {
                continue;
            }
            transition_states.push(vec![observation.clone()]);
        }
        for transitions in transition_states.chunks(DISCOVERY_TRANSITION_CHUNK_SIZE) {
            let source_summary = reconcile_scoped_discovery_observation_transitions(
                pool,
                &discovery_source,
                transitions,
            )
                .await
                .with_context(|| {
                    format!(
                        "failed to reconcile historical discovery transition for discovery_source {discovery_source}"
                    )
                })?;
            source_active_edge_count = source_summary.active_edge_count;
            source_admitted_edge_count = source_summary.admitted_edge_count;
            source_admitted_edges = source_summary.admitted_edges;
            summary.inserted_edge_count += source_summary.inserted_edge_count;
            summary.deactivated_edge_count += source_summary.deactivated_edge_count;
            summary.admission_epoch_bump_count += source_summary.admission_epoch_bump_count;
        }

        if reconcile_full_sources {
            let latest_observations = latest_discovery_observations(source_observations)?;
            let expected_epoch = expected_initial_admission_epoch
                .map(|initial| {
                    i64::try_from(summary.admission_epoch_bump_count)
                        .context("ENSv2 discovery admission-epoch bump count exceeds i64")
                        .and_then(|bumps| {
                            initial
                                .checked_add(bumps)
                                .context("ENSv2 discovery admission epoch overflow")
                        })
                })
                .transpose()?;
            let source_summary = match (reconcile_through_block_number, expected_epoch) {
                (Some(through_block_number), Some(expected_epoch)) => {
                    let chain = expected_chain
                        .context("expected ENSv2 discovery admission epoch is missing its chain")?;
                    reconcile_discovery_observations_through_block_with_expected_admission_epoch(
                        pool,
                        &discovery_source,
                        &latest_observations,
                        through_block_number,
                        chain,
                        expected_epoch,
                    )
                    .await
                }
                (Some(through_block_number), None) => {
                    reconcile_discovery_observations_through_block(
                        pool,
                        &discovery_source,
                        &latest_observations,
                        through_block_number,
                    )
                    .await
                }
                (None, Some(expected_epoch)) => {
                    let chain = expected_chain
                        .context("expected ENSv2 discovery admission epoch is missing its chain")?;
                    reconcile_discovery_observations_with_expected_admission_epoch(
                        pool,
                        &discovery_source,
                        &latest_observations,
                        chain,
                        expected_epoch,
                    )
                    .await
                }
                (None, None) => {
                    reconcile_discovery_observations(pool, &discovery_source, &latest_observations)
                        .await
                }
            }
            .with_context(|| format!("failed to finalize discovery_source {discovery_source}"))?;
            source_active_edge_count = source_summary.active_edge_count;
            source_admitted_edge_count = source_summary.admitted_edge_count;
            source_admitted_edges = source_summary.admitted_edges;
            summary.inserted_edge_count += source_summary.inserted_edge_count;
            summary.deactivated_edge_count += source_summary.deactivated_edge_count;
            summary.admission_epoch_bump_count += source_summary.admission_epoch_bump_count;
        }
        summary.active_edge_count += source_active_edge_count;
        summary.admitted_edge_count += source_admitted_edge_count;
        summary.admitted_edges.extend(source_admitted_edges);
    }
    Ok(summary)
}

async fn load_materialized_observations(
    pool: &PgPool,
    discovery_source: &str,
) -> Result<HashSet<MaterializedObservation>> {
    let rows = sqlx::query_as::<_, MaterializedObservation>(
        r#"
        SELECT
            provenance ->> 'observation_key',
            active_from_block_number,
            active_from_block_hash,
            (provenance ->> 'transaction_index')::BIGINT,
            (provenance ->> 'log_index')::BIGINT,
            lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND provenance ? 'observation_key'
          AND provenance ? 'to_address'
          AND active_from_block_number IS NOT NULL
          AND active_from_block_hash IS NOT NULL

        UNION

        SELECT
            provenance ->> 'observation_key',
            active_to_block_number,
            active_to_block_hash,
            (provenance ->> 'active_to_transaction_index')::BIGINT,
            (provenance ->> 'active_to_log_index')::BIGINT,
            $2::TEXT
        FROM discovery_edges
        WHERE discovery_source = $1
          AND provenance ? 'observation_key'
          AND active_to_block_number IS NOT NULL
          AND active_to_block_hash IS NOT NULL
        "#,
    )
    .bind(discovery_source)
    .bind(ZERO_ADDRESS)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load materialized ENSv2 discovery history for {discovery_source}")
    })?;
    Ok(rows.into_iter().collect())
}

async fn load_active_discovery_edge_count(pool: &PgPool, discovery_source: &str) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL",
    )
    .bind(discovery_source)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count active ENSv2 discovery edges for {discovery_source}")
    })?;
    usize::try_from(count).context("active ENSv2 discovery edge count exceeds usize")
}

pub(super) fn ens_v2_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_subregistry:{chain}")
}

pub(super) fn ens_v2_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_resolver:{chain}")
}
