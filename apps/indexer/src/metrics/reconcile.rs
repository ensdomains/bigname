use std::{
    collections::BTreeSet,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use bigname_metrics::{IntCounterVec, IntGaugeVec, MetricsRegistry};
use sqlx::PgPool;

const CHECKPOINT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

pub(super) struct ReconcileMetrics {
    normalized_events_processed: IntCounterVec,
    staged_items: IntGaugeVec,
    active_labels: Mutex<BTreeSet<(String, String)>>,
}

impl ReconcileMetrics {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            normalized_events_processed: registry.int_counter_vec(
                "startup_adapter_reconcile_normalized_events_processed_total",
                "Normalized events in batches committed by upsert_normalized_events_with_summary while the adapter's startup checkpoint is stream_complete; includes inserted and unchanged identities.",
                &["adapter", "chain"],
            )?,
            staged_items: registry.int_gauge_vec(
                "startup_adapter_reconcile_staged_items",
                "Staged assignments on an active stream_complete startup checkpoint; this is not an exact event total because an assignment can emit no normalized event.",
                &["adapter", "chain"],
            )?,
            active_labels: Mutex::new(BTreeSet::new()),
        })
    }

    fn record_event_batch(&self, batch: &bigname_storage::StartupAdapterReconcileEventBatch) {
        let labels = [batch.family.as_str(), batch.chain.as_str()];
        self.normalized_events_processed
            .with_label_values(&labels)
            .inc_by(super::count(batch.normalized_event_count));
        self.staged_items
            .with_label_values(&labels)
            .set(batch.staged_item_count);
        self.active_labels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((batch.family.as_str().to_owned(), batch.chain.clone()));
    }

    fn refresh_expected(&self, checkpoints: &[bigname_storage::StartupAdapterReconcileCheckpoint]) {
        let current = checkpoints
            .iter()
            .map(|checkpoint| {
                (
                    checkpoint.family.as_str().to_owned(),
                    checkpoint.chain.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        for checkpoint in checkpoints {
            let labels = [checkpoint.family.as_str(), checkpoint.chain.as_str()];
            self.normalized_events_processed
                .with_label_values(&labels)
                .inc_by(0);
            self.staged_items
                .with_label_values(&labels)
                .set(checkpoint.staged_item_count);
        }

        let mut active_labels = self
            .active_labels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (adapter, chain) in active_labels.difference(&current) {
            self.staged_items
                .with_label_values(&[adapter.as_str(), chain.as_str()])
                .set(0);
        }
        *active_labels = current;
    }
}

pub(super) async fn configure(pool: &PgPool, deployment_profile: &str) -> Result<()> {
    bigname_storage::configure_startup_adapter_reconcile_event_observer(
        deployment_profile,
        record_event_batch,
    )?;
    let generation = configuration_generation().fetch_add(1, Ordering::AcqRel) + 1;
    refresh_expected(pool, deployment_profile)
        .await
        .context("failed to initialize startup adapter reconcile metrics")?;

    let pool = pool.clone();
    let deployment_profile = deployment_profile.to_owned();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(CHECKPOINT_REFRESH_INTERVAL).await;
            if configuration_generation().load(Ordering::Acquire) != generation {
                return;
            }
            if let Err(error) = refresh_expected(&pool, &deployment_profile).await {
                tracing::warn!(
                    service = "indexer",
                    deployment_profile,
                    error = %format!("{error:#}"),
                    "failed to refresh startup adapter reconcile metrics"
                );
            }
        }
    });
    Ok(())
}

async fn refresh_expected(pool: &PgPool, deployment_profile: &str) -> Result<()> {
    let checkpoints = bigname_storage::load_active_startup_adapter_reconcile_checkpoints(
        pool,
        deployment_profile,
    )
    .await?;
    super::indexer_metrics()
        .reconcile
        .refresh_expected(&checkpoints);
    Ok(())
}

fn record_event_batch(batch: &bigname_storage::StartupAdapterReconcileEventBatch) {
    super::indexer_metrics().reconcile.record_event_batch(batch);
}

fn configuration_generation() -> &'static AtomicU64 {
    static GENERATION: AtomicU64 = AtomicU64::new(0);
    &GENERATION
}

#[cfg(test)]
pub(super) fn initialize_endpoint_test_series() {
    record_event_batch(&bigname_storage::StartupAdapterReconcileEventBatch {
        family: bigname_storage::StartupAdapterReconcileFamily::EnsV1SubregistryDiscovery,
        chain: "metrics-test-chain".to_owned(),
        normalized_event_count: 0,
        staged_item_count: 0,
    });
}

#[cfg(test)]
mod tests {
    use bigname_metrics::{BuildInfo, MetricsRegistry};

    use super::*;

    #[test]
    fn event_batches_keep_family_chain_labels_and_counter_monotonicity() -> Result<()> {
        let metrics = test_metrics()?;
        let family = bigname_storage::StartupAdapterReconcileFamily::EnsV1SubregistryDiscovery;
        let first = bigname_storage::StartupAdapterReconcileEventBatch {
            family,
            chain: "ethereum-mainnet".to_owned(),
            normalized_event_count: 3,
            staged_item_count: 10,
        };
        let second = bigname_storage::StartupAdapterReconcileEventBatch {
            normalized_event_count: 2,
            ..first.clone()
        };
        let other_chain = bigname_storage::StartupAdapterReconcileEventBatch {
            family,
            chain: "base-mainnet".to_owned(),
            normalized_event_count: 4,
            staged_item_count: 8,
        };

        metrics.record_event_batch(&first);
        metrics.record_event_batch(&second);
        metrics.record_event_batch(&other_chain);

        assert_eq!(processed(&metrics, family, "ethereum-mainnet"), 5);
        assert_eq!(processed(&metrics, family, "base-mainnet"), 4);
        assert_eq!(expected(&metrics, family, "ethereum-mainnet"), 10);
        assert_eq!(expected(&metrics, family, "base-mainnet"), 8);
        Ok(())
    }

    #[test]
    fn leaving_the_active_window_clears_expected_work_without_incrementing_counter() -> Result<()> {
        let metrics = test_metrics()?;
        let family = bigname_storage::StartupAdapterReconcileFamily::EnsV1SubregistryDiscovery;
        let batch = bigname_storage::StartupAdapterReconcileEventBatch {
            family,
            chain: "ethereum-mainnet".to_owned(),
            normalized_event_count: 2,
            staged_item_count: 10,
        };
        metrics.record_event_batch(&batch);
        let processed_before = processed(&metrics, family, "ethereum-mainnet");

        metrics.refresh_expected(&[]);

        assert_eq!(
            processed(&metrics, family, "ethereum-mainnet"),
            processed_before
        );
        assert_eq!(expected(&metrics, family, "ethereum-mainnet"), 0);
        Ok(())
    }

    fn test_metrics() -> Result<ReconcileMetrics> {
        let registry = MetricsRegistry::new(BuildInfo {
            build_sha: "test",
            replay_version: 1,
            schema_version: 1,
        })?;
        ReconcileMetrics::new(&registry)
    }

    fn processed(
        metrics: &ReconcileMetrics,
        family: bigname_storage::StartupAdapterReconcileFamily,
        chain: &str,
    ) -> u64 {
        metrics
            .normalized_events_processed
            .with_label_values(&[family.as_str(), chain])
            .get()
    }

    fn expected(
        metrics: &ReconcileMetrics,
        family: bigname_storage::StartupAdapterReconcileFamily,
        chain: &str,
    ) -> i64 {
        metrics
            .staged_items
            .with_label_values(&[family.as_str(), chain])
            .get()
    }
}
