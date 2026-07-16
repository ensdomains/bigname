//! Pure inclusive block-range arithmetic.
//!
//! Callers retain ownership of tuple identity, source admission, and which
//! facts count as coverage; this module only composes already-selected ranges.

/// An inclusive interval of block numbers.
///
/// Construction rejects inverted bounds so coverage callers share one range
/// invariant instead of each cursor walk handling malformed intervals
/// differently.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InclusiveBlockInterval {
    from_block: i64,
    through_block: i64,
}

impl InclusiveBlockInterval {
    pub const fn new(from_block: i64, through_block: i64) -> Option<Self> {
        if from_block <= through_block {
            Some(Self {
                from_block,
                through_block,
            })
        } else {
            None
        }
    }

    pub const fn from_block(self) -> i64 {
        self.from_block
    }

    pub const fn through_block(self) -> i64 {
        self.through_block
    }

    /// Return the portions of this interval not contained by `covered`.
    ///
    /// Input coverage may be unsorted, overlapping, adjacent, or extend beyond
    /// this interval. The returned gaps are sorted, disjoint inclusive ranges.
    pub fn uncovered_by(
        self,
        covered: impl IntoIterator<Item = Self>,
    ) -> Vec<InclusiveBlockInterval> {
        let mut gaps = Vec::new();
        let mut next_required = Some(self.from_block);

        for interval in coalesce_inclusive_block_intervals(covered) {
            let Some(cursor) = next_required else {
                break;
            };
            if interval.through_block < cursor {
                continue;
            }
            if interval.from_block > self.through_block {
                break;
            }
            if interval.from_block > cursor {
                gaps.push(Self {
                    from_block: cursor,
                    through_block: self
                        .through_block
                        .min(interval.from_block.saturating_sub(1)),
                });
            }
            if interval.through_block >= self.through_block {
                next_required = None;
                break;
            }
            next_required = interval
                .through_block
                .checked_add(1)
                .map(|next| next.max(cursor));
        }

        if let Some(cursor) = next_required
            && cursor <= self.through_block
        {
            gaps.push(Self {
                from_block: cursor,
                through_block: self.through_block,
            });
        }
        gaps
    }

    pub fn is_covered_by(self, covered: impl IntoIterator<Item = Self>) -> bool {
        self.uncovered_by(covered).is_empty()
    }
}

/// Sort and coalesce overlapping or adjacent inclusive block intervals.
pub fn coalesce_inclusive_block_intervals(
    intervals: impl IntoIterator<Item = InclusiveBlockInterval>,
) -> Vec<InclusiveBlockInterval> {
    let mut intervals = intervals.into_iter().collect::<Vec<_>>();
    intervals.sort_unstable();

    let mut coalesced: Vec<InclusiveBlockInterval> = Vec::with_capacity(intervals.len());
    for interval in intervals {
        if let Some(previous) = coalesced.last_mut()
            && (interval.from_block <= previous.through_block
                || previous
                    .through_block
                    .checked_add(1)
                    .is_some_and(|next| interval.from_block <= next))
        {
            previous.through_block = previous.through_block.max(interval.through_block);
        } else {
            coalesced.push(interval);
        }
    }
    coalesced
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interval(from: i64, through: i64) -> InclusiveBlockInterval {
        InclusiveBlockInterval::new(from, through).expect("test interval must be ordered")
    }

    #[test]
    fn coalesces_unsorted_overlapping_and_adjacent_intervals() {
        assert_eq!(InclusiveBlockInterval::new(2, 1), None);
        assert_eq!(
            coalesce_inclusive_block_intervals([
                interval(20, 25),
                interval(5, 10),
                interval(1, 4),
                interval(8, 15),
                interval(30, 30),
            ]),
            vec![interval(1, 15), interval(20, 25), interval(30, 30)]
        );
    }

    #[test]
    fn terminal_block_does_not_overflow_adjacency_or_subtraction() {
        let terminal = interval(i64::MAX - 1, i64::MAX);
        assert_eq!(
            coalesce_inclusive_block_intervals([
                interval(i64::MAX, i64::MAX),
                interval(i64::MAX - 1, i64::MAX - 1),
            ]),
            vec![terminal]
        );
        assert!(terminal.is_covered_by([
            interval(i64::MAX, i64::MAX),
            interval(i64::MAX - 1, i64::MAX - 1)
        ]));
    }

    #[test]
    fn subtraction_matches_point_membership_on_small_ranges() {
        let intervals = (0..=5)
            .flat_map(|from| (from..=5).map(move |through| interval(from, through)))
            .collect::<Vec<_>>();

        for &required in &intervals {
            for &first in &intervals {
                for &second in &intervals {
                    let covered = [first, second];
                    let gaps = required.uncovered_by(covered);
                    assert_eq!(required.is_covered_by(covered), gaps.is_empty());

                    for block in required.from_block()..=required.through_block() {
                        let expected_gap = !covered.iter().any(|interval| {
                            block >= interval.from_block() && block <= interval.through_block()
                        });
                        let actual_gap = gaps.iter().any(|interval| {
                            block >= interval.from_block() && block <= interval.through_block()
                        });
                        assert_eq!(
                            actual_gap, expected_gap,
                            "block {block}, required {required:?}, covered {covered:?}"
                        );
                    }
                }
            }
        }
    }
}
