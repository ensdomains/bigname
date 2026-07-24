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
        lower_bound_contains(emitters, emitter, block_number, position)
            && upper_bound_contains(emitter, block_number, position)
    })
}

fn lower_bound_contains(
    emitters: &[ActiveEmitter],
    emitter: &ActiveEmitter,
    block_number: i64,
    position: LogPosition,
) -> bool {
    emitter.active_from_block_number.is_none_or(|start| {
        block_number > start
            || (block_number == start
                && if emitter.discovery_interval {
                    // Discovery backfill fetches the activation block as a whole. Attribute from
                    // block start unless an exact same-address predecessor close partitions it.
                    // The attach position orders discovery observations; it is not evidence of
                    // the child's deployment position, which the retained facts do not store.
                    predecessor_close_position(emitters, emitter, start)
                        .is_none_or(|predecessor_close| position >= predecessor_close)
                } else {
                    emitter
                        .active_from_log_position
                        .is_none_or(|start_position| position >= start_position)
                })
    })
}

fn predecessor_close_position(
    emitters: &[ActiveEmitter],
    emitter: &ActiveEmitter,
    start_block: i64,
) -> Option<LogPosition> {
    emitters
        .iter()
        .filter(|candidate| !std::ptr::eq(*candidate, emitter))
        .filter(|candidate| {
            candidate.discovery_interval
                && candidate.address == emitter.address
                && candidate.source_family == emitter.source_family
                && candidate.contract_instance_id == emitter.contract_instance_id
                && candidate.active_to_block_number == Some(start_block)
        })
        .filter_map(|candidate| candidate.active_to_log_position)
        .filter(|close_position| {
            emitter
                .active_from_log_position
                .is_none_or(|start_position| *close_position <= start_position)
        })
        .max()
}

fn upper_bound_contains(emitter: &ActiveEmitter, block_number: i64, position: LogPosition) -> bool {
    emitter.active_to_block_number.is_none_or(|end| {
        block_number < end
            || (block_number == end
                && match emitter.active_to_log_position {
                    Some(end_position) => position < end_position,
                    // Legacy and non-log discovery evidence has no exact close position. Keep
                    // the entire terminal block on the closed interval, deliberately preferring
                    // that predecessor when a positionless successor reactivates the same address
                    // in this block. Exact reactivation needs stored positions to partition it.
                    None => emitter.discovery_interval,
                })
    })
}

#[cfg(test)]
mod tests {
    use sqlx::types::Uuid;

    use super::*;

    fn emitter(
        label: &str,
        active_from_block_number: Option<i64>,
        active_from_log_position: Option<LogPosition>,
        active_to_block_number: Option<i64>,
        active_to_log_position: Option<LogPosition>,
        discovery_interval: bool,
    ) -> ActiveEmitter {
        ActiveEmitter {
            address: "0x0000000000000000000000000000000000000001".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            active_from_block_number,
            active_to_block_number,
            active_from_log_position,
            active_to_log_position,
            discovery_interval,
            source_manifest_id: 1,
            contract_instance_id: Uuid::from_u128(1),
            namespace: label.to_owned(),
            manifest_version: 1,
        }
    }

    #[test]
    fn active_emitter_for_log_applies_position_and_fallback_bounds() {
        let positioned = emitter(
            "positioned",
            Some(10),
            Some(LogPosition::new(10, 5)),
            Some(20),
            Some(LogPosition::new(0, 7)),
            true,
        );
        let positionless = emitter("positionless", Some(30), None, Some(40), None, true);
        let manifest = emitter("manifest", Some(50), None, Some(60), None, false);
        let cases = [
            (
                "before positioned start block",
                &positioned,
                9,
                0,
                99,
                false,
            ),
            (
                "earlier transaction in positioned activation block",
                &positioned,
                10,
                0,
                99,
                true,
            ),
            (
                "same transaction before positioned attach",
                &positioned,
                10,
                10,
                4,
                true,
            ),
            ("positioned activation point", &positioned, 10, 10, 5, true),
            (
                "positioned terminal block before close",
                &positioned,
                20,
                0,
                6,
                true,
            ),
            (
                "positioned terminal close point",
                &positioned,
                20,
                0,
                7,
                false,
            ),
            (
                "positionless activation block",
                &positionless,
                30,
                0,
                0,
                true,
            ),
            (
                "positionless terminal block",
                &positionless,
                40,
                99,
                99,
                true,
            ),
            (
                "after positionless terminal block",
                &positionless,
                41,
                0,
                0,
                false,
            ),
            ("manifest activation block", &manifest, 50, 0, 0, true),
            ("manifest terminal block", &manifest, 60, 0, 0, false),
        ];

        for (name, emitter, block_number, transaction_index, log_index, expected) in cases {
            assert_eq!(
                active_emitter_for_log(
                    std::slice::from_ref(emitter),
                    block_number,
                    transaction_index,
                    log_index,
                )
                .is_some(),
                expected,
                "{name}",
            );
        }
    }

    #[test]
    fn same_block_close_then_reattach_partitions_at_predecessor_close() {
        let predecessor = emitter(
            "predecessor",
            Some(100),
            Some(LogPosition::new(0, 10)),
            Some(200),
            Some(LogPosition::new(0, 5)),
            true,
        );
        let successor = emitter(
            "successor",
            Some(200),
            Some(LogPosition::new(0, 10)),
            None,
            None,
            true,
        );
        let emitters = [predecessor, successor];
        let cases = [
            (4, "predecessor"),
            (5, "successor"),
            (9, "successor"),
            (10, "successor"),
        ];

        for (log_index, expected) in cases {
            assert_eq!(
                active_emitter_for_log(&emitters, 200, 0, log_index)
                    .map(|emitter| emitter.namespace.as_str()),
                Some(expected),
                "log {log_index} must have exactly one interval owner",
            );
        }
    }

    #[test]
    fn positionless_close_prefers_closed_predecessor_for_terminal_block() {
        let predecessor = emitter("predecessor", Some(100), None, Some(200), None, true);
        let successor = emitter("successor", Some(200), None, None, None, true);

        assert_eq!(
            active_emitter_for_log(&[predecessor, successor], 200, 7, 11)
                .map(|emitter| emitter.namespace.as_str()),
            Some("predecessor"),
        );
    }
}
