//! JSON-RPC traffic of the Ingest and Live providers, mirrored from the ingest engine's totals.
use std::collections::BTreeMap;

use anyhow::Result;
use bigname_ingest::{RpcCount, RpcCountKind};
use bigname_metrics::{IntCounterVec, MetricsRegistry};

#[derive(Clone)]
pub(super) struct IngestRpcCounters {
    requests: IntCounterVec,
    calls: IntCounterVec,
    null_results: IntCounterVec,
}

impl IngestRpcCounters {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            requests: registry.int_counter_vec(
                "phase_runner_ingest_rpc_requests_total",
                "HTTP attempts Ingest and Live made to the source's RPC endpoint, recorded on \
                 completion or failure, one per single call or batch, retries included, by \
                 outcome (ok or failed); an attempt abandoned in flight is not counted.",
                &["chain", "source", "outcome"],
            )?,
            calls: registry.int_counter_vec(
                "phase_runner_ingest_rpc_calls_total",
                "JSON-RPC calls of the recorded HTTP attempts to the source's RPC endpoint, \
                 each retry counted again.",
                &["chain", "source", "method"],
            )?,
            null_results: registry.int_counter_vec(
                "phase_runner_ingest_provider_null_results_total",
                "Receipts and transactions of selected logs the source's RPC endpoint answered \
                 null, counted on every null answer Ingest accepted, including re-requests.",
                &["chain", "source", "method"],
            )?,
        })
    }

    pub(super) fn apply(&self, totals: BTreeMap<RpcCount, u64>) {
        for (count, total) in totals {
            let family = match count.kind {
                RpcCountKind::Requests => &self.requests,
                RpcCountKind::Calls => &self.calls,
                RpcCountKind::NullResults => &self.null_results,
            };
            let counter = family.with_label_values(&[&count.chain, &count.source, &count.label]);
            counter.inc_by(total.saturating_sub(counter.get()));
        }
    }
}
