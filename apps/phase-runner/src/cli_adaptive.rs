//! Opt-in whole-block Interpret batch budgets for the one-shot redo command.
use std::num::{NonZeroU32, NonZeroU64};

use bigname_interpret::AdaptiveBatchConfig;
use clap::Args;

use crate::error::{ErrorKind, RunnerError, RunnerResult};

#[derive(Debug, Args)]
pub(super) struct AdaptiveInterpretArgs {
    #[arg(
        long,
        help = "size Interpret redo batches by raw log count and estimated raw input bytes"
    )]
    interpret_adaptive: bool,

    #[arg(
        long,
        requires = "interpret_adaptive",
        value_name = "COUNT",
        help = "target raw logs per adaptive Interpret batch (default: 200000)"
    )]
    interpret_target_logs: Option<NonZeroU64>,

    #[arg(
        long,
        requires = "interpret_adaptive",
        value_name = "MIB",
        help = "target estimated raw input MiB per adaptive Interpret batch (default: 128; not an RSS cap)"
    )]
    interpret_target_input_mib: Option<NonZeroU64>,

    #[arg(
        long,
        requires = "interpret_adaptive",
        value_name = "BLOCKS",
        help = "maximum canonical block span of one adaptive Interpret batch (default: 131072)"
    )]
    interpret_adaptive_max_blocks: Option<NonZeroU32>,
}

impl AdaptiveInterpretArgs {
    /// `explicit_block_limit` is an operator-set `--interpret-blocks-per-batch`, which also
    /// caps the adaptive span. The fixed 500-block default does not.
    pub(super) fn resolve(
        self,
        explicit_block_limit: Option<NonZeroU32>,
    ) -> RunnerResult<Option<AdaptiveBatchConfig>> {
        if !self.interpret_adaptive {
            return Ok(None);
        }
        let mut config = AdaptiveBatchConfig::default();
        if let Some(logs) = self.interpret_target_logs {
            config.target_logs = logs;
        }
        if let Some(mib) = self.interpret_target_input_mib {
            config.target_input_bytes = mib
                .get()
                .checked_mul(1024 * 1024)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| {
                    RunnerError::new(
                        ErrorKind::Configuration,
                        "--interpret-target-input-mib is too large to express in bytes",
                    )
                })?;
        }
        // A serial redo has one pending batch, so the pending budget only has to admit it.
        config.max_pending_input_bytes = config
            .max_pending_input_bytes
            .max(config.target_input_bytes);
        if let Some(blocks) = self.interpret_adaptive_max_blocks {
            config.max_blocks = blocks;
        }
        if let Some(limit) = explicit_block_limit {
            config.max_blocks = config.max_blocks.min(limit);
        }
        Ok(Some(config))
    }
}

#[cfg(test)]
#[path = "cli/adaptive_tests.rs"]
mod tests;
