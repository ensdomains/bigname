use std::collections::BTreeMap;

use anyhow::{Result, bail};

use super::watch::{Snapshot, WatchEmitter, watch_is_covered};

type WatchTuple = (String, String, String, String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoverageInterval {
    pub(super) start: u64,
    pub(super) end: Option<u64>,
}

/// In-memory [persisted Ingest coverage](../../../docs/glossary.md#persisted-ingest-coverage)
/// used only while manifest synchronization validates a compiled watch plan.
pub(super) type PersistedWatchCoverage = BTreeMap<WatchTuple, Vec<CoverageInterval>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UncoveredInterval {
    Finite { start: u64, end: u64 },
    Tail { start: u128 },
}

impl std::fmt::Display for UncoveredInterval {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finite { start, end } => write!(formatter, "{start}..={end}"),
            Self::Tail { start } => write!(formatter, "{start}..=unbounded"),
        }
    }
}

pub(super) fn normalize_coverage(intervals: &mut Vec<CoverageInterval>) {
    intervals.retain(|interval| interval.end.is_none_or(|end| interval.start <= end));
    intervals.sort_by_key(|interval| {
        (
            interval.start,
            interval.end.is_none(),
            interval.end.unwrap_or(u64::MAX),
        )
    });
    let mut merged = Vec::<CoverageInterval>::with_capacity(intervals.len());
    for next in intervals.drain(..) {
        let Some(current) = merged.last_mut() else {
            merged.push(next);
            continue;
        };
        let Some(current_end) = current.end else {
            continue;
        };
        let overlaps = next.start <= current_end;
        let adjacent = current_end
            .checked_add(1)
            .is_some_and(|after_current| next.start <= after_current);
        if overlaps || adjacent {
            current.end = match (current.end, next.end) {
                (None, _) | (_, None) => None,
                (Some(left), Some(right)) => Some(left.max(right)),
            };
        } else {
            merged.push(next);
        }
    }
    *intervals = merged;
}

pub(super) fn widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    persisted_coverage: &PersistedWatchCoverage,
    required_ingest_redo_pending: bool,
) -> Result<Option<u64>> {
    let previous = previous.watch_by_chain.get(chain_id);
    let Some(desired) = desired.watch_by_chain.get(chain_id) else {
        return Ok(None);
    };
    let mut widened_from = None;
    for (key, start) in desired {
        let desired_all_emitter_covers = desired_all_emitter_covers(desired, key, *start);
        let is_widening = !watch_is_covered(previous, key, *start) && !desired_all_emitter_covers;
        let pending_all_emitter_removal = required_ingest_redo_pending
            && !desired_all_emitter_covers
            && previous_all_emitter_covers(previous, key, *start);
        if !is_widening && !pending_all_emitter_removal {
            continue;
        }
        if let WatchEmitter::Address { family, address } = &key.emitter
            && let Some(intervals) = persisted_coverage.get(&(
                chain_id.to_owned(),
                family.clone(),
                address.clone(),
                key.topic0.clone(),
            ))
            && let Some(uncovered) = first_uncovered(intervals, *start)
        {
            bail!(
                "compiled-watch comparison refused promised coverage start {start}: persisted \
                 ingest coverage has an uncovered interval {uncovered} for chain {chain_id}, \
                 source family {family}, address {address}, topic {}. Raise the declaration to \
                 the first continuously covered start, or rebuild from a fresh database/from-zero \
                 ingest when coverage from {start} is required. A retained database that still \
                 requires that coverage needs a separately planned repair which fetches the \
                 uncovered range before making the wider promise; ordinary address-scoped redo \
                 follows the persisted epochs and cannot fill this gap. Manifest sync did not mark \
                 these blocks repaired",
                key.topic0,
            );
        }
        if is_widening {
            widened_from = Some(widened_from.map_or(*start, |current: u64| current.min(*start)));
        }
    }
    Ok(widened_from)
}

fn previous_all_emitter_covers(
    previous: Option<&BTreeMap<super::watch::WatchKey, u64>>,
    key: &super::watch::WatchKey,
    start: u64,
) -> bool {
    if matches!(key.emitter, WatchEmitter::All) {
        return false;
    }
    previous
        .and_then(|watch| {
            watch.get(&super::watch::WatchKey {
                emitter: WatchEmitter::All,
                topic0: key.topic0.clone(),
            })
        })
        .is_some_and(|all_start| *all_start <= start)
}

fn first_uncovered(
    intervals: &[CoverageInterval],
    promised_start: u64,
) -> Option<UncoveredInterval> {
    let mut candidates = intervals.iter().skip_while(|interval| {
        interval
            .end
            .is_some_and(|interval_end| interval_end < promised_start)
    });
    let Some(first) = candidates.next() else {
        return Some(UncoveredInterval::Tail {
            start: promised_start.into(),
        });
    };
    if first.start > promised_start {
        return Some(UncoveredInterval::Finite {
            start: promised_start,
            end: first.start - 1,
        });
    }
    let mut covered_through = first.end?;
    for interval in candidates {
        let next_required = covered_through.checked_add(1)?;
        if interval.start > next_required {
            return Some(UncoveredInterval::Finite {
                start: next_required,
                end: interval.start - 1,
            });
        }
        let interval_end = interval.end?;
        covered_through = covered_through.max(interval_end);
    }
    Some(UncoveredInterval::Tail {
        start: u128::from(covered_through) + 1,
    })
}

fn desired_all_emitter_covers(
    desired: &BTreeMap<super::watch::WatchKey, u64>,
    key: &super::watch::WatchKey,
    start: u64,
) -> bool {
    if matches!(key.emitter, WatchEmitter::All) {
        return false;
    }
    desired
        .get(&super::watch::WatchKey {
            emitter: WatchEmitter::All,
            topic0: key.topic0.clone(),
        })
        .is_some_and(|all_start| *all_start <= start)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval(start: u64, end: Option<u64>) -> CoverageInterval {
        CoverageInterval { start, end }
    }

    #[test]
    fn normalization_merges_overlapping_intervals() {
        let mut intervals = vec![interval(8, Some(12)), interval(5, Some(10))];
        normalize_coverage(&mut intervals);
        assert_eq!(intervals, [interval(5, Some(12))]);
    }

    #[test]
    fn normalization_merges_adjacent_intervals() {
        let mut intervals = vec![interval(10, Some(10)), interval(11, Some(15))];
        normalize_coverage(&mut intervals);
        assert_eq!(intervals, [interval(10, Some(15))]);
    }

    #[test]
    fn normalization_merges_a_finite_interval_into_an_open_interval() {
        let mut intervals = vec![interval(11, None), interval(10, Some(10))];
        normalize_coverage(&mut intervals);
        assert_eq!(intervals, [interval(10, None)]);
    }

    #[test]
    fn validation_reports_the_first_leading_or_internal_gap() {
        assert_eq!(
            first_uncovered(&[interval(6, None)], 5),
            Some(UncoveredInterval::Finite { start: 5, end: 5 })
        );
        assert_eq!(
            first_uncovered(&[interval(5, Some(5)), interval(10, None)], 5),
            Some(UncoveredInterval::Finite { start: 6, end: 9 })
        );
    }

    #[test]
    fn validation_reports_a_finite_tail() {
        assert_eq!(
            first_uncovered(&[interval(5, Some(10))], 5),
            Some(UncoveredInterval::Tail { start: 11 })
        );
    }

    #[test]
    fn normalization_excludes_empty_epochs_and_does_not_overflow_at_u64_max() {
        let mut intervals = vec![interval(10, Some(9)), interval(u64::MAX, Some(u64::MAX))];
        normalize_coverage(&mut intervals);
        assert_eq!(intervals, [interval(u64::MAX, Some(u64::MAX))]);
    }

    #[test]
    fn validation_accepts_a_continuous_union_from_the_promised_start() {
        assert_eq!(
            first_uncovered(&[interval(5, Some(5)), interval(10, None)], 10),
            None
        );
    }

    #[test]
    fn desired_all_emitter_behavior_is_unchanged() {
        let topic0 = "0xtopic".to_owned();
        let address_key = super::super::watch::WatchKey {
            emitter: WatchEmitter::Address {
                family: "family".to_owned(),
                address: "0xaddress".to_owned(),
            },
            topic0: topic0.clone(),
        };
        let desired = BTreeMap::from([(
            super::super::watch::WatchKey {
                emitter: WatchEmitter::All,
                topic0,
            },
            5,
        )]);
        assert!(desired_all_emitter_covers(&desired, &address_key, 5));
    }
}
