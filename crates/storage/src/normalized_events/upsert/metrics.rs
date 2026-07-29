use std::{
    collections::BTreeMap,
    sync::{OnceLock, RwLock},
};

use anyhow::{Context, Result, ensure};
use sqlx::PgPool;
use tracing::warn;

use crate::normalized_events::types::NormalizedEvent;

const STARTUP_CHECKPOINT_CURSOR_KIND: &str = "startup_adapter_owned_raw_log_state";
const STARTUP_CHECKPOINT_SCOPE: &str = "startup_adapter_sync";
const STREAM_COMPLETE_STATUS: &str = "stream_complete";
const ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER: &str = "ens_v1_subregistry_discovery";
const OBSERVED_ADAPTERS: &[&str] = &[ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER];
const ENS_V1_SUBREGISTRY_EVENT_DERIVATIONS: &[&str] = &[
    "ens_v1_subregistry_changed",
    "ens_v1_registry_resolver_changed",
];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StartupAdapterReconcileFamily {
    EnsV1SubregistryDiscovery,
}

impl StartupAdapterReconcileFamily {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EnsV1SubregistryDiscovery => ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER,
        }
    }

    fn from_adapter(adapter: &str) -> Option<Self> {
        match adapter {
            ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER => Some(Self::EnsV1SubregistryDiscovery),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupAdapterReconcileCheckpoint {
    pub family: StartupAdapterReconcileFamily,
    pub chain: String,
    pub staged_item_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupAdapterReconcileEventBatch {
    pub family: StartupAdapterReconcileFamily,
    pub chain: String,
    pub normalized_event_count: usize,
    pub staged_item_count: i64,
}

pub type StartupAdapterReconcileEventObserver = fn(&StartupAdapterReconcileEventBatch);

#[derive(Clone)]
struct ObserverConfig {
    deployment_profile: String,
    observer: StartupAdapterReconcileEventObserver,
}

pub fn configure_startup_adapter_reconcile_event_observer(
    deployment_profile: impl Into<String>,
    observer: StartupAdapterReconcileEventObserver,
) -> Result<()> {
    let deployment_profile = deployment_profile.into();
    ensure!(
        !deployment_profile.trim().is_empty(),
        "startup adapter reconcile metrics require a deployment profile"
    );
    *observer_config()
        .write()
        .map_err(|_| anyhow::anyhow!("startup adapter reconcile observer lock is poisoned"))? =
        Some(ObserverConfig {
            deployment_profile,
            observer,
        });
    Ok(())
}

pub async fn load_active_startup_adapter_reconcile_checkpoints(
    pool: &PgPool,
    deployment_profile: &str,
) -> Result<Vec<StartupAdapterReconcileCheckpoint>> {
    let rows = sqlx::query_as::<_, (String, String, i64, String)>(
        r#"
        SELECT adapter, chain_id, staged_item_count, status
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND cursor_kind = $2
          AND checkpoint_scope = $3
          AND adapter = ANY($4::TEXT[])
        ORDER BY adapter, chain_id
        "#,
    )
    .bind(deployment_profile)
    .bind(STARTUP_CHECKPOINT_CURSOR_KIND)
    .bind(STARTUP_CHECKPOINT_SCOPE)
    .bind(OBSERVED_ADAPTERS)
    .fetch_all(pool)
    .await
    .context("failed to load startup adapter reconcile checkpoints")?;

    rows.into_iter()
        .filter_map(|(adapter, chain, staged_item_count, status)| {
            active_checkpoint(adapter, chain, staged_item_count, status).transpose()
        })
        .collect()
}

pub(super) async fn observe_startup_adapter_reconcile_event_batch(
    pool: &PgPool,
    events: &[NormalizedEvent],
) {
    if let Err(error) = observe_startup_adapter_reconcile_event_batch_inner(pool, events).await {
        warn!(
            service = "storage",
            operation = "observe_startup_adapter_reconcile_event_batch",
            error = %format!("{error:#}"),
            "startup adapter reconcile metrics observation failed after normalized-event commit"
        );
    }
}

async fn observe_startup_adapter_reconcile_event_batch_inner(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<()> {
    let Some(config) = observer_config()
        .read()
        .map_err(|_| anyhow::anyhow!("startup adapter reconcile observer lock is poisoned"))?
        .clone()
    else {
        return Ok(());
    };
    let event_counts = candidate_event_counts(events);
    if event_counts.is_empty() {
        return Ok(());
    }
    let checkpoints =
        load_active_startup_adapter_reconcile_checkpoints(pool, &config.deployment_profile).await?;
    for batch in event_batches_for_checkpoints(&event_counts, &checkpoints) {
        (config.observer)(&batch);
    }
    Ok(())
}

fn observer_config() -> &'static RwLock<Option<ObserverConfig>> {
    static CONFIG: OnceLock<RwLock<Option<ObserverConfig>>> = OnceLock::new();
    CONFIG.get_or_init(|| RwLock::new(None))
}

fn active_checkpoint(
    adapter: String,
    chain: String,
    staged_item_count: i64,
    status: String,
) -> Result<Option<StartupAdapterReconcileCheckpoint>> {
    if status != STREAM_COMPLETE_STATUS {
        return Ok(None);
    }
    let Some(family) = StartupAdapterReconcileFamily::from_adapter(&adapter) else {
        return Ok(None);
    };
    ensure!(
        staged_item_count >= 0,
        "startup adapter reconcile staged item count must not be negative"
    );
    Ok(Some(StartupAdapterReconcileCheckpoint {
        family,
        chain,
        staged_item_count,
    }))
}

fn candidate_event_counts(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        if !ENS_V1_SUBREGISTRY_EVENT_DERIVATIONS.contains(&event.derivation_kind.as_str()) {
            continue;
        }
        let Some(chain) = event.chain_id.as_ref() else {
            continue;
        };
        *counts.entry(chain.clone()).or_insert(0) += 1;
    }
    counts
}

fn event_batches_for_checkpoints(
    event_counts: &BTreeMap<String, usize>,
    checkpoints: &[StartupAdapterReconcileCheckpoint],
) -> Vec<StartupAdapterReconcileEventBatch> {
    checkpoints
        .iter()
        .filter_map(|checkpoint| {
            event_counts
                .get(&checkpoint.chain)
                .map(|normalized_event_count| StartupAdapterReconcileEventBatch {
                    family: checkpoint.family,
                    chain: checkpoint.chain.clone(),
                    normalized_event_count: *normalized_event_count,
                    staged_item_count: checkpoint.staged_item_count,
                })
        })
        .collect()
}

pub(super) fn count_normalized_events_by_event_kind(
    events: &[NormalizedEvent],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn count_normalized_events_by_source_family(
    events: &[NormalizedEvent],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.source_family.clone()).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::CanonicalityState;

    #[test]
    fn reconcile_batches_require_stream_complete_and_preserve_family_chain_counts() -> Result<()> {
        let events = vec![
            event(
                "ethereum-mainnet",
                "ens_v1_subregistry_changed",
                "mainnet-subregistry",
            ),
            event(
                "ethereum-mainnet",
                "ens_v1_registry_resolver_changed",
                "mainnet-resolver",
            ),
            event(
                "base-mainnet",
                "ens_v1_subregistry_changed",
                "base-subregistry",
            ),
            event("ethereum-mainnet", "other_derivation", "other"),
        ];
        let counts = candidate_event_counts(&events);
        let active = active_checkpoint(
            ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER.to_owned(),
            "ethereum-mainnet".to_owned(),
            10,
            STREAM_COMPLETE_STATUS.to_owned(),
        )?
        .expect("stream-complete checkpoint should be active");
        let running = active_checkpoint(
            ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER.to_owned(),
            "base-mainnet".to_owned(),
            8,
            "running".to_owned(),
        )?;

        assert!(running.is_none());
        assert_eq!(
            event_batches_for_checkpoints(&counts, &[active]),
            vec![StartupAdapterReconcileEventBatch {
                family: StartupAdapterReconcileFamily::EnsV1SubregistryDiscovery,
                chain: "ethereum-mainnet".to_owned(),
                normalized_event_count: 2,
                staged_item_count: 10,
            }]
        );
        Ok(())
    }

    #[test]
    fn completed_checkpoint_does_not_open_the_reconcile_window() -> Result<()> {
        assert!(
            active_checkpoint(
                ENS_V1_SUBREGISTRY_DISCOVERY_ADAPTER.to_owned(),
                "ethereum-mainnet".to_owned(),
                10,
                "completed".to_owned(),
            )?
            .is_none()
        );
        Ok(())
    }

    fn event(chain: &str, derivation_kind: &str, identity: &str) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: identity.to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "TestEvent".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(1),
            chain_id: Some(chain.to_owned()),
            block_number: Some(1),
            block_hash: Some("0xblock".to_owned()),
            transaction_hash: Some("0xtx".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({}),
            derivation_kind: derivation_kind.to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({}),
        }
    }
}
