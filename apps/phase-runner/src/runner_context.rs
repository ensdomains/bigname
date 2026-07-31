use std::sync::Arc;

use crate::{
    config::ChainConfig,
    error::{RunnerError, RunnerResult},
    heads::{HeadMarkers, load_available_heads, load_marker},
    phase::{PhaseContext, PhaseName, RunMode},
};

use super::PhaseRunner;

impl PhaseRunner {
    pub(super) async fn phase_context(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        mode: RunMode,
    ) -> RunnerResult<PhaseContext> {
        let available_heads = match mode.range() {
            Some(range) => load_marker(self.store.pool(), &chain.chain_id, range.to)
                .await?
                .map(|latest| HeadMarkers {
                    latest,
                    safe: None,
                    finalized: None,
                }),
            None => load_available_heads(self.store.pool(), &chain.chain_id).await?,
        };
        let live_handoff = if phase == PhaseName::Live && matches!(mode, RunMode::Normal) {
            let handoff = self.store.ingest_handoff(&chain.chain_id).await?;
            if handoff.is_none() {
                return Err(RunnerError::data_integrity(format!(
                    "cannot start live phase for chain {} without the ingest handoff block",
                    chain.chain_id
                )));
            }
            handoff
        } else {
            None
        };
        let resume = self
            .store
            .phase_resume(&chain.chain_id, phase, &mode)
            .await?;
        Ok(PhaseContext {
            chain_id: chain.chain_id.clone(),
            phase,
            mode,
            sources: Arc::clone(&chain.sources),
            available_heads,
            live_handoff,
            resume,
        })
    }
}
