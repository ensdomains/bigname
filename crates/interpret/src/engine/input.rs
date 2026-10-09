use super::{BatchRequest, Engine, FullStateReason, StateLoader, speculation::Queue};
use crate::{Result, load};

impl Engine {
    pub(super) async fn load_current_batch(
        &self,
        request: &BatchRequest,
        markers: &[(i64, String)],
        cached_prior: Option<load::CachedPrior>,
        queue: Option<&mut Queue>,
    ) -> Result<load::LoadedBatch> {
        let (batch_from, _) = markers.first().expect("non-empty batch markers");
        let (batch_to, _) = markers.last().expect("non-empty batch markers");
        let resume_marker = request
            .resume_current
            .as_ref()
            .map(|marker| (marker.number, marker.hash.as_str()));
        let speculative = match queue {
            Some(queue) => self.speculative_input(queue, request, markers).await?,
            None => None,
        };
        // Each loader chooses inside its own snapshot. A failed prediction comes back
        // through this same serial path, including the ordinary full-state fallback.
        let lookahead = match speculative {
            Some(attempt) => attempt,
            None if self.force_full_state_loader => {
                load::lookahead::Attempt::FullStateRequired(StateLoader::FullState {
                    reason: FullStateReason::OperatorOverride,
                })
            }
            None => {
                load::lookahead::batch_input(
                    &self.pool,
                    &request.chain_id,
                    *batch_from,
                    *batch_to,
                    resume_marker,
                    self.state_cache_capacity,
                    self.lookahead_statement_timeout_secs,
                )
                .await?
            }
        };
        match lookahead {
            load::lookahead::Attempt::Loaded(loaded) => {
                drop(cached_prior);
                self.loader_choices
                    .record(&request.chain_id, StateLoader::Lookahead)?;
                Ok(*loaded)
            }
            load::lookahead::Attempt::FullStateRequired(choice) => {
                self.loader_choices.record(&request.chain_id, choice)?;
                load::batch_input(
                    &self.pool,
                    &request.chain_id,
                    *batch_from,
                    *batch_to,
                    resume_marker,
                    cached_prior,
                    self.state_cache_capacity,
                )
                .await
            }
        }
    }
}
