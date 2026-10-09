use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use bigname_storage::{
    ResolutionState, families::control::cutover::load_cut_over_on, load_resolution_state_on,
};
use sqlx::PgPool;

/// One chain's cutover, and where its client-facing proxy forwards when a proxy row exists.
pub(super) struct ChainState {
    pub(super) chain: String,
    pub(super) cut_over: bool,
    pub(super) proxy: Option<ResolutionState>,
}

#[derive(Clone)]
pub(super) struct UniversalResolverGauges {
    cut_over: IntGaugeVec,
    unadmitted: IntGaugeVec,
    /// Per chain, the unadmitted state last warned about, so each change warns once.
    reported: Arc<Mutex<BTreeMap<String, ResolutionState>>>,
    exported: Arc<Mutex<BTreeSet<String>>>,
}

impl UniversalResolverGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            cut_over: registry.int_gauge_vec(
                "phase_runner_universal_resolver_cut_over",
                "Whether the chain is cut over: its deployment profile admits an ENSv2 root \
                 registry. The Universal Resolver proxies are not an input.",
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

    /// Exports both gauges for every chain with phase rows. A chain with no admitted ENSv2 root
    /// registry reads 0 on `cut_over`, and one with no client-facing proxy row reads 0 on
    /// `unadmitted`.
    pub(super) async fn refresh(
        &self,
        pool: &PgPool,
        rows: &[super::PhaseMetricRow],
    ) -> Result<()> {
        let chains: BTreeSet<&str> = rows.iter().map(|row| row.chain_id.as_str()).collect();
        let mut conn = pool
            .acquire()
            .await
            .context("failed to acquire a connection for the resolution state")?;
        let mut states = Vec::with_capacity(chains.len());
        for chain in chains {
            states.push(ChainState {
                chain: chain.to_owned(),
                cut_over: load_cut_over_on(&mut conn, chain).await?,
                proxy: load_resolution_state_on(&mut conn, chain).await?,
            });
        }
        self.apply(&states);
        Ok(())
    }

    /// Returns the chains this call warned about.
    pub(super) fn apply(&self, states: &[ChainState]) -> Vec<String> {
        let mut warned = Vec::new();
        let next: BTreeSet<String> = states.iter().map(|state| state.chain.clone()).collect();
        let mut exported = lock(&self.exported);
        let mut reported = lock(&self.reported);
        for chain in exported.difference(&next) {
            let _ = self.cut_over.remove_label_values(&[chain]);
            let _ = self.unadmitted.remove_label_values(&[chain]);
            reported.remove(chain);
        }
        for ChainState {
            chain,
            cut_over,
            proxy,
        } in states
        {
            let unadmitted = proxy.as_ref().filter(|state| state.unadmitted);
            match (reported.get(chain), unadmitted) {
                (previous, Some(state)) if previous != Some(state) => {
                    tracing::warn!(
                        chain_id = chain,
                        proxy = state.proxy,
                        implementation = state.implementation,
                        block = state.since_block,
                        "the client-facing Universal Resolver now ends at an implementation the \
                         ens_execution manifest does not admit"
                    );
                    reported.insert(chain.clone(), state.clone());
                    warned.push(chain.clone());
                }
                (Some(previous), None) => {
                    tracing::info!(
                        chain_id = chain,
                        proxy = previous.proxy,
                        implementation = previous.implementation,
                        cut_over,
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
                .set(i64::from(*cut_over));
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
