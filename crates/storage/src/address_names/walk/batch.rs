//! How many candidates each walk step composes (`super`). The sizes decide only how much a
//! page composes and in how many steps: the walk's stopping rule reads neither.
//!
//! The walk first composed four pages of candidates, at least 32, and doubled. Its steps now
//! start at about one page for name grouping and never carry it past where one of those steps ended, so a page
//! composes no more candidates than it did then. The second step is at least a sixteenth of
//! that first step and every later one at least doubles before it is cut to such an end, so a
//! walk takes at most five more steps than it did for the same candidates and request.

/// The most candidates one walk step composes before it drops the rows the filters reject, so
/// a filter that matches little holds at most this many names' rows at a time.
const MAX_WALK_BATCH: usize = 1024;

const FIRST_STEP_FLOOR: usize = 8;
const SECOND_STEP_FLOOR: usize = 16;

/// The size of the walk's next step.
pub(super) struct Steps {
    /// The page and the one group after it that shows more follow.
    wanted: usize,
    /// Registration grouping and the independent old-policy oracle keep the old boundaries.
    old: bool,
    /// The next step's size before it is cut to where a step of the first sizes ended.
    size: usize,
    taken: usize,
    walked: usize,
    /// The step of the first sizes that the walk is inside, and the candidates walked by its end.
    first_sizes_step: usize,
    first_sizes_end: usize,
}

impl Steps {
    /// The first step: the wanted groups and, on a resumed page, the cursor's own candidate,
    /// which the walk starts on and the page statement drops.
    pub(super) fn first(page_size: usize, resumed: bool, old: bool) -> Self {
        let wanted = page_size.saturating_add(1);
        let first_sizes_step = wanted.saturating_mul(4).max(32);
        Self {
            wanted,
            old,
            size: wanted
                .saturating_add(usize::from(resumed))
                .max(FIRST_STEP_FLOOR),
            taken: 0,
            walked: 0,
            first_sizes_step,
            first_sizes_end: first_sizes_step,
        }
    }

    pub(super) fn size(&self) -> usize {
        let to_end = self.first_sizes_end - self.walked;
        if self.old {
            return to_end;
        }
        self.size.min(to_end)
    }

    /// Sizes the step after one that left the page open, the `walked` candidates having
    /// yielded `found` of the wanted groups. It is the candidates the missing groups need at
    /// that yield, and at least a quarter of the first step, since a page a few groups short
    /// is the common case, or twice any later step.
    pub(super) fn grow(&mut self, walked: usize, found: usize) {
        self.taken += 1;
        self.walked = walked;
        while self.first_sizes_end <= walked {
            self.first_sizes_step = self.first_sizes_step.saturating_mul(2).min(MAX_WALK_BATCH);
            self.first_sizes_end = self.first_sizes_end.saturating_add(self.first_sizes_step);
        }
        let least = if self.taken == 1 {
            self.size.div_ceil(4).max(SECOND_STEP_FLOOR)
        } else {
            self.size.saturating_mul(2)
        };
        self.size = self
            .wanted
            .saturating_sub(found)
            .saturating_mul(walked)
            .div_ceil(found.max(1))
            .max(least)
            .min(MAX_WALK_BATCH);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The step sizes of a walk whose page is final once `final_at` candidates are walked and
    /// whose steps before that have found `found` groups.
    fn sizes(page_size: usize, resumed: bool, final_at: usize, found: usize) -> Vec<usize> {
        let mut steps = Steps::first(page_size, resumed, false);
        let (mut sizes, mut walked) = (Vec::new(), 0);
        loop {
            sizes.push(steps.size());
            walked += steps.size();
            if walked >= final_at {
                return sizes;
            }
            steps.grow(walked, found);
        }
    }

    /// The same walk's step sizes as the walk first took them.
    fn first_sizes(page_size: usize, final_at: usize) -> Vec<usize> {
        let mut sizes = vec![(4 * (page_size + 1)).max(32)];
        while sizes.iter().sum::<usize>() < final_at {
            sizes.push((sizes[sizes.len() - 1] * 2).min(MAX_WALK_BATCH));
        }
        sizes
    }

    #[test]
    fn first_step_is_the_page_and_what_ends_it() {
        assert_eq!(sizes(200, false, 1, 0), [201]);
        assert_eq!(sizes(200, true, 1, 0), [202]);
        assert_eq!(sizes(7, false, 1, 0), [8]);
        assert_eq!(sizes(7, true, 1, 0), [9]);
        assert_eq!(sizes(1, false, 1, 0), [FIRST_STEP_FLOOR]);
        assert_eq!(sizes(1, true, 1, 0), [FIRST_STEP_FLOOR]);
    }

    #[test]
    fn later_steps_follow_the_shortfall_and_stop_where_the_first_sizes_did() {
        // One group short of a 200-name page: a quarter of the first step.
        assert_eq!(sizes(200, false, 252, 200), [201, 51]);
        // Still one short: doubling, cut to the first sizes' ends at 804 and 1,828.
        assert_eq!(
            sizes(200, false, 1900, 200),
            [201, 51, 102, 204, 246, 816, 208, 1024]
        );
        // Every wanted group is gathered but the walk has not passed the page's last one.
        assert_eq!(sizes(1, false, 61, 2), [8, 16, 8, 64]);
        // Nothing found: every wanted group at one per walked candidates, cut to each end.
        assert_eq!(sizes(5, false, 306, 0), [8, 24, 64, 128, 256]);
        // Half the candidates yield a group: the 101 missing groups need 204 more.
        assert_eq!(sizes(200, false, 405, 100), [201, 204]);
    }

    /// These finite constant-yield cases bound name-mode prefixes and extra walk steps.
    #[test]
    fn no_walk_composes_more_than_the_first_sizes_did() {
        for page_size in [1, 5, 25, 116, 200] {
            for resumed in [false, true] {
                for final_at in 1..=3000 {
                    let first = first_sizes(page_size, final_at);
                    // No group, all but one of the wanted groups, and all of them.
                    for found in [0, page_size, page_size + 1] {
                        let current = sizes(page_size, resumed, final_at, found);
                        let case = format!("{page_size} {resumed} {final_at} {found}");
                        assert!(
                            current.iter().sum::<usize>() <= first.iter().sum::<usize>(),
                            "{case}: {current:?} {first:?}"
                        );
                        assert!(current.len() <= first.len() + 5, "{case}: {current:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn old_policy_keeps_first_doubling_cap_and_resumed_inputs() {
        for page_size in [1, 5, 25, 116, 200] {
            for resumed in [false, true] {
                let mut steps = Steps::first(page_size, resumed, true);
                let mut actual = Vec::new();
                let mut walked = 0;
                while walked < 5000 {
                    actual.push(steps.size());
                    walked += steps.size();
                    steps.grow(walked, page_size);
                }
                assert_eq!(actual, first_sizes(page_size, 5000));
            }
        }
    }

    #[test]
    fn changing_yields_keep_name_prefixes_and_step_bounds() {
        // The positions yielding groups are independent of the issued partitions. Stop once
        // the wanted groups are found, then compare with the old boundary that contains that
        // last group. These cases have changing yields, including empty and nearly full steps.
        for (page_size, resumed, candidates, group_positions) in [
            (200, false, 1000, (0..200).chain([700]).collect::<Vec<_>>()),
            (
                85,
                true,
                1100,
                (1..=80).chain([200, 400, 500, 700, 800, 900]).collect(),
            ),
            (5, false, 600, vec![1, 2, 40, 150, 200, 450]),
            (1, false, 120, vec![40, 60]),
        ] {
            let final_at = group_positions[page_size] + 1;
            let old = first_sizes(page_size, final_at);
            let mut steps = Steps::first(page_size, resumed, false);
            let mut walked = 0;
            let mut taken = 0;
            loop {
                walked = (walked + steps.size()).min(candidates);
                taken += 1;
                let found = group_positions
                    .iter()
                    .filter(|position| **position < walked)
                    .count();
                if found > page_size || walked == candidates {
                    break;
                }
                steps.grow(walked, found);
            }
            assert!(walked <= old.iter().sum::<usize>().min(candidates));
            assert!(taken <= old.len() + 5);
        }
    }
}
