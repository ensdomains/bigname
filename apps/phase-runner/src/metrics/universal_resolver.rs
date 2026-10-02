use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use bigname_storage::{ProxyHop, UniversalResolverCutover, load_universal_resolver_cutovers};
use sqlx::PgPool;

#[derive(Clone)]
pub(super) struct UniversalResolverGauges {
    cut_over: IntGaugeVec,
    unadmitted: IntGaugeVec,
    /// Per chain, the unadmitted hop last reported, so each change logs once.
    reported: Arc<Mutex<BTreeMap<String, ProxyHop>>>,
    exported: Arc<Mutex<BTreeSet<String>>>,
}

impl UniversalResolverGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            cut_over: registry.int_gauge_vec(
                "phase_runner_universal_resolver_cut_over",
                "Whether the client-facing Universal Resolver proxy chain ends at an admitted \
                 UniversalResolverV2 implementation at the family publication.",
                &["chain"],
            )?,
            unadmitted: registry.int_gauge_vec(
                "phase_runner_universal_resolver_unadmitted",
                "Whether the client-facing Universal Resolver proxy chain ends at an \
                 implementation the ens_execution manifest does not admit.",
                &["chain"],
            )?,
            reported: Arc::default(),
            exported: Arc::default(),
        })
    }

    /// Exports both gauges for every chain with phase rows; a chain with no proxy row reads 0.
    pub(super) async fn refresh(
        &self,
        pool: &PgPool,
        rows: &[super::PhaseMetricRow],
    ) -> Result<()> {
        let states = load_universal_resolver_cutovers(pool).await?;
        self.apply(rows.iter().map(|row| row.chain_id.as_str()), &states);
        Ok(())
    }

    /// Returns the chains this call warned about.
    pub(super) fn apply<'a>(
        &self,
        chains: impl IntoIterator<Item = &'a str>,
        states: &BTreeMap<String, UniversalResolverCutover>,
    ) -> Vec<String> {
        let mut warned = Vec::new();
        let none = UniversalResolverCutover::default();
        let next: BTreeSet<String> = chains
            .into_iter()
            .map(str::to_owned)
            .chain(states.keys().cloned())
            .collect();
        let mut exported = lock(&self.exported);
        let mut reported = lock(&self.reported);
        for chain in exported.difference(&next) {
            let _ = self.cut_over.remove_label_values(&[chain]);
            let _ = self.unadmitted.remove_label_values(&[chain]);
            reported.remove(chain);
        }
        for chain in &next {
            let state = states.get(chain).unwrap_or(&none);
            let unadmitted = state.unadmitted();
            match (reported.get(chain), unadmitted) {
                (previous, Some(hop)) if previous != Some(hop) => {
                    tracing::warn!(
                        chain_id = chain,
                        proxy = hop.proxy_address,
                        implementation = hop.implementation,
                        block = hop.block_number,
                        "the client-facing Universal Resolver now ends at an implementation the \
                         ens_execution manifest does not admit; the chain is not cut over"
                    );
                    reported.insert(chain.clone(), hop.clone());
                    warned.push(chain.clone());
                }
                (Some(previous), None) => {
                    tracing::info!(
                        chain_id = chain,
                        proxy = previous.proxy_address,
                        implementation = previous.implementation,
                        cut_over = state.cut_over,
                        "the client-facing Universal Resolver no longer ends at an unadmitted \
                         implementation"
                    );
                    reported.remove(chain);
                }
                _ => {}
            }
            let labels = &[chain.as_str()];
            self.cut_over
                .with_label_values(labels)
                .set(i64::from(state.cut_over));
            self.unadmitted
                .with_label_values(labels)
                .set(i64::from(unadmitted.is_some()));
        }
        *exported = next;
        warned
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
