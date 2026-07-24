use anyhow::{Result, bail};

use super::ActiveEmitter;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LogPosition {
    transaction_index: i64,
    log_index: i64,
}

impl LogPosition {
    pub(crate) fn optional(
        transaction_index: Option<i64>,
        log_index: Option<i64>,
    ) -> Result<Option<Self>> {
        match (transaction_index, log_index) {
            (None, None) => Ok(None),
            (Some(transaction_index), Some(log_index))
                if transaction_index >= 0 && log_index >= 0 =>
            {
                Ok(Some(Self {
                    transaction_index,
                    log_index,
                }))
            }
            _ => bail!("ENSv2 discovery log position must contain two non-negative offsets"),
        }
    }

    const fn new(transaction_index: i64, log_index: i64) -> Self {
        Self {
            transaction_index,
            log_index,
        }
    }
}

/// Preserve the legacy block-only half-open interval selection for callers that
/// do not have an exact raw-log position.
#[cfg(test)]
pub(crate) fn active_emitter_for_block(
    emitters: &[ActiveEmitter],
    block_number: i64,
) -> Option<&ActiveEmitter> {
    emitters.iter().find(|emitter| {
        emitter
            .active_from_block_number
            .is_none_or(|active_from| block_number >= active_from)
            && emitter
                .active_to_block_number
                .is_none_or(|active_to| block_number < active_to)
    })
}

pub(crate) fn active_emitter_for_log(
    emitters: &[ActiveEmitter],
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
) -> Option<&ActiveEmitter> {
    let position = LogPosition::new(transaction_index, log_index);
    emitters.iter().find(|emitter| {
        lower_bound_contains(emitter, block_number, position)
            && upper_bound_contains(emitter, block_number, position)
    })
}

fn lower_bound_contains(emitter: &ActiveEmitter, block_number: i64, position: LogPosition) -> bool {
    emitter.active_from_block_number.is_none_or(|start| {
        block_number > start
            || (block_number == start
                && emitter
                    .active_from_log_position
                    .is_none_or(|start_position| position >= start_position))
    })
}

fn upper_bound_contains(emitter: &ActiveEmitter, block_number: i64, position: LogPosition) -> bool {
    emitter.active_to_block_number.is_none_or(|end| {
        block_number < end
            || (block_number == end
                && match emitter.active_to_log_position {
                    Some(end_position) => position < end_position,
                    None => emitter.discovery_interval,
                })
    })
}
