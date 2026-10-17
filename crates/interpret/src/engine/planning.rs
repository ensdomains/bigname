use std::time::Instant;

use super::{Engine, RunMode, adaptive};
use crate::{InterpretError, Result, load};

pub(super) struct CachedWork {
    chain: String,
    target: i64,
    config: adaptive::AdaptiveBatchConfig,
    blocks: Vec<adaptive::BlockWork>,
    consumed: usize,
}

impl CachedWork {
    fn resumes(
        &self,
        chain: &str,
        from: i64,
        target: i64,
        config: adaptive::AdaptiveBatchConfig,
    ) -> bool {
        self.chain == chain
            && self.target == target
            && self.config == config
            && self
                .blocks
                .get(self.consumed)
                .is_some_and(|block| block.number == from)
    }
}

impl Engine {
    /// Size each redo batch by the raw logs and estimated raw input bytes of its whole
    /// blocks instead of a fixed block count. Normal and recompute-flags runs ignore it.
    /// The estimates bound pending input, not retained adapter state or process RSS.
    pub fn with_adaptive_batching(mut self, config: adaptive::AdaptiveBatchConfig) -> Self {
        self.adaptive = Some(config);
        self.adaptive_work = std::sync::Mutex::new(None);
        self
    }

    /// The canonical markers of the next batch, starting at `from` and ending at or before
    /// `to`. Only a redo plans adaptively: Normal follow keeps the fixed batch length.
    pub(super) async fn plan_batch(
        &self,
        chain: &str,
        mode: RunMode,
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, String)>> {
        let Some(config) = self.adaptive.filter(|_| mode == RunMode::Redo) else {
            return load::canonical_markers(
                &self.pool,
                chain,
                from,
                to,
                i64::from(self.blocks_per_batch.get()),
            )
            .await;
        };
        let started = Instant::now();
        // Planning hints only: the batch's own snapshot re-reads and checks every marker.
        // Keeping one bounded metadata span avoids rescanning it for each small batch of
        // dense history. Any request other than the exact next block reloads it.
        let cached = self
            .adaptive_work
            .lock()
            .map_err(|_| InterpretError::transient("adaptive planning cache lock was poisoned"))?
            .take()
            .filter(|cached| cached.resumes(chain, from, to, config));
        let mut cached = match cached {
            Some(cached) => cached,
            None => CachedWork {
                chain: chain.into(),
                target: to,
                config,
                blocks: load::adaptive::block_work(
                    &self.pool,
                    chain,
                    from,
                    to,
                    i64::from(config.max_blocks.get()),
                )
                .await?,
                consumed: 0,
            },
        };
        let Some(batch) =
            adaptive::partition_borrowed(&cached.blocks[cached.consumed..], config, 1)
                .into_iter()
                .next()
        else {
            return Ok(Vec::new());
        };
        cached.consumed += batch.markers.len();
        if cached.consumed < cached.blocks.len() {
            *self.adaptive_work.lock().map_err(|_| {
                InterpretError::transient("adaptive planning cache lock was poisoned")
            })? = Some(cached);
        }
        if let (Some(first), Some(last)) = (batch.markers.first(), batch.markers.last()) {
            tracing::info!(
                target: "bigname_interpret::planning",
                chain,
                from_block = first.0,
                to_block = last.0,
                blocks = batch.markers.len(),
                planned_raw_logs = batch.raw_logs,
                estimated_input_bytes = batch.estimated_input_bytes,
                oversized_single_block = batch.oversized_single_block,
                planning_ms = started.elapsed().as_secs_f64() * 1000.0,
                "planned adaptive whole-block batch (input bytes are an estimate, not RSS)"
            );
        }
        Ok(batch.markers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumed_hints_only_resume_exact_next_request() {
        let config = adaptive::AdaptiveBatchConfig::default();
        let cache = CachedWork {
            chain: "fixture".into(),
            target: 20,
            config,
            consumed: 1,
            blocks: vec![10, 11]
                .into_iter()
                .map(|number| adaptive::BlockWork {
                    number,
                    hash: format!("hash-{number}"),
                    raw_logs: 0,
                    estimated_input_bytes: 256,
                })
                .collect(),
        };
        assert!(cache.resumes("fixture", 11, 20, config));
        assert!(
            !cache.resumes("fixture", 10, 20, config),
            "retry before committed progress reloads hints"
        );
        assert!(!cache.resumes("fixture", 12, 20, config));
        assert!(!cache.resumes("other", 11, 20, config));
        assert!(!cache.resumes("fixture", 11, 21, config));
        let changed = adaptive::AdaptiveBatchConfig {
            max_blocks: std::num::NonZeroU32::new(1).unwrap(),
            ..config
        };
        assert!(!cache.resumes("fixture", 11, 20, changed));
    }
}
