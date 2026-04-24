use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_manifests::{
    DiscoveryObservation, DiscoveryReconciliationSummary, reconcile_discovery_observations,
};
use serde_json::Value;
use sqlx::PgPool;

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

pub(super) async fn reconcile_discovery_observations_by_source(
    pool: &PgPool,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    let mut by_source = BTreeMap::<String, Vec<DiscoveryObservation>>::new();
    for observation in observations {
        by_source
            .entry(observation.discovery_source.clone())
            .or_default()
            .push(observation.clone());
    }

    let mut summary = DiscoveryReconciliationSummary {
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        admitted_edges: Vec::new(),
    };
    for (discovery_source, source_observations) in by_source {
        let source_summary =
            reconcile_discovery_observations(pool, &discovery_source, &source_observations)
                .await
                .with_context(|| {
                    format!("failed to reconcile discovery_source {discovery_source}")
                })?;
        summary.active_edge_count += source_summary.active_edge_count;
        summary.admitted_edge_count += source_summary.admitted_edge_count;
        summary.inserted_edge_count += source_summary.inserted_edge_count;
        summary.deactivated_edge_count += source_summary.deactivated_edge_count;
        summary.admitted_edges.extend(source_summary.admitted_edges);
    }
    Ok(summary)
}

pub(super) fn ens_v2_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_subregistry:{chain}")
}

pub(super) fn ens_v2_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v2_registry_resolver:{chain}")
}
