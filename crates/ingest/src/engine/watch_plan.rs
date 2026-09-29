use std::sync::Arc;

use crate::{
    BatchOutcome, BatchRequest, IngestError, Result,
    manifest::{WatchFilter, load_persisted_watch_filter, load_watch_filter},
    plan::validate_request,
};

use super::{
    Engine, SourceDescriptor,
    redo::{RedoLoadFuture, RedoWindowLoader},
};

#[cfg(test)]
mod tests;

pub(super) struct RedoWatchPlan {
    generation: i64,
    range: (i64, i64),
    filter: Arc<WatchFilter>,
}

impl Engine {
    /// Reuses persisted watch intervals within one exclusively owned Ingest redo attempt.
    ///
    /// The phase runner supplies its persisted attempt generation and excludes manifest sync,
    /// Interpret, and other chain writers for the attempt. A resumed attempt must supply its
    /// new generation even when it resumes the same range. Uncoordinated library callers use
    /// `run_batch`, which reloads the watch plan for every window.
    pub async fn run_redo_attempt_batch(
        &self,
        request: BatchRequest,
        generation: i64,
    ) -> Result<BatchOutcome> {
        validate_request(&request)?;
        let range = request.redo_range.ok_or_else(|| {
            IngestError::configuration("a prepared Ingest redo requires a redo range")
        })?;
        let chain_id = request.chain_id.clone();
        let filter = self.redo_watch_filter(&chain_id, generation, range).await?;
        let result = self
            .run_redo_batch_with_loader(&PreparedRedoWindowLoader { filter }, request)
            .await;
        if result.as_ref().map_or(true, |outcome| outcome.complete) {
            let mut plans = self.redo_watch_plans.lock().await;
            if plans
                .get(&chain_id)
                .is_some_and(|plan| plan.generation == generation && plan.range == range)
            {
                plans.remove(&chain_id);
            }
        }
        result
    }

    async fn redo_watch_filter(
        &self,
        chain_id: &str,
        generation: i64,
        range: (i64, i64),
    ) -> Result<Arc<WatchFilter>> {
        if let Some(plan) = self.redo_watch_plans.lock().await.get(chain_id)
            && plan.generation == generation
            && plan.range == range
        {
            return Ok(Arc::clone(&plan.filter));
        }
        let filter =
            Arc::new(load_persisted_watch_filter(&self.pool, chain_id, range.0, range.1).await?);
        self.redo_watch_plans.lock().await.insert(
            chain_id.to_owned(),
            RedoWatchPlan {
                generation,
                range,
                filter: Arc::clone(&filter),
            },
        );
        Ok(filter)
    }
}

struct PreparedRedoWindowLoader {
    filter: Arc<WatchFilter>,
}

impl RedoWindowLoader for PreparedRedoWindowLoader {
    fn load<'a>(
        &'a self,
        engine: &'a Engine,
        chain_id: &'a str,
        source: &'a SourceDescriptor,
        all_sources: &'a [SourceDescriptor],
        from: i64,
        to: i64,
    ) -> RedoLoadFuture<'a> {
        Box::pin(async move {
            let mut filter = self.filter.clipped(from, to);
            if filter.has_queries() {
                // Only persisted intervals are reused. Retained creation announcements and
                // announcements fetched inside this window still take their ordinary path.
                filter
                    .supplement_creation_announcements(&engine.pool, chain_id, from, to)
                    .await?;
            } else {
                // Preserve the window loader's empty-admission validation and diagnostics.
                filter = load_watch_filter(&engine.pool, chain_id, from, to).await?;
            }
            // Redo must fetch this window itself, without reusing prefetched logs across forks.
            engine
                .load_filtered_window(chain_id, source, all_sources, (from, to), None, &filter)
                .await
        })
    }
}
