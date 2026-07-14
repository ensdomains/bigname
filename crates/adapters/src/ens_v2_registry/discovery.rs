use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryReconciliationSummary, reconcile_discovery_observations,
    reconcile_scoped_discovery_observations,
};
use serde_json::Value;
use sqlx::PgPool;

use super::{constants::ZERO_ADDRESS, util::normalize_address};

type MaterializedObservation = (String, i64, String, String);

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
    reconcile_discovery_observation_history(pool, observations, reconcile_full_sources, &[]).await
}

pub(super) async fn reconcile_discovery_observation_history_for_chain(
    pool: &PgPool,
    chain: &str,
    observations: &[DiscoveryObservation],
    reconcile_full_sources: bool,
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
    )
    .await
}

async fn reconcile_discovery_observation_history(
    pool: &PgPool,
    observations: &[DiscoveryObservation],
    reconcile_full_sources: bool,
    expected_sources: &[String],
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
        admitted_edges: Vec::new(),
    };
    for (discovery_source, source_observations) in by_source {
        let materialized_observations =
            load_materialized_observations(pool, &discovery_source).await?;
        let mut source_active_edge_count = 0;
        let mut source_admitted_edge_count = 0;
        let mut source_admitted_edges = Vec::new();
        let mut source_latest_observations = BTreeMap::<String, DiscoveryObservation>::new();
        for observation in &source_observations {
            let observation_key = observation
                .provenance
                .get("observation_key")
                .and_then(Value::as_str)
                .context("ENSv2 discovery observation missing observation_key")?
                .to_owned();
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
                normalize_address(&observation.to_address),
            );
            source_latest_observations.insert(observation_key, observation.clone());
            if materialized_observations.contains(&observation_position) {
                continue;
            }
            let transition_state = source_latest_observations
                .values()
                .cloned()
                .collect::<Vec<_>>();
            let source_summary = reconcile_scoped_discovery_observations(
                pool,
                &discovery_source,
                &transition_state,
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
        }

        if reconcile_full_sources {
            let latest_observations = latest_discovery_observations(source_observations)?;
            let source_summary =
                reconcile_discovery_observations(pool, &discovery_source, &latest_observations)
                    .await
                    .with_context(|| {
                        format!("failed to finalize discovery_source {discovery_source}")
                    })?;
            source_active_edge_count = source_summary.active_edge_count;
            source_admitted_edge_count = source_summary.admitted_edge_count;
            source_admitted_edges = source_summary.admitted_edges;
            summary.inserted_edge_count += source_summary.inserted_edge_count;
            summary.deactivated_edge_count += source_summary.deactivated_edge_count;
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

pub(super) fn ens_v2_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_subregistry:{chain}")
}

pub(super) fn ens_v2_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_resolver:{chain}")
}
