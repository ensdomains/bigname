use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::RequiredWatchedTuple;

pub(super) type Topic0sByFamily = BTreeMap<String, BTreeSet<String>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct WatchedTupleKey {
    source_family: String,
    address: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlockInterval {
    from_block: i64,
    through_block: i64,
}

type RequirementSnapshot = BTreeMap<WatchedTupleKey, Vec<BlockInterval>>;

/// Process-local proof state for one chain. Requirements retain exactly which
/// watched tuple intervals were covered, allowing an admission-epoch change
/// to verify only newly required intervals instead of discarding all history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VerifiedCoverageState {
    pub(super) from_block: i64,
    pub(super) through_block: i64,
    pub(super) topic0s_by_family: Topic0sByFamily,
    pub(super) discovery_admission_epoch: i64,
    requirements: RequirementSnapshot,
}

impl VerifiedCoverageState {
    pub(super) fn empty(
        from_block: i64,
        topic0s_by_family: Topic0sByFamily,
        discovery_admission_epoch: i64,
    ) -> Self {
        Self {
            from_block,
            through_block: from_block.saturating_sub(1),
            topic0s_by_family,
            discovery_admission_epoch,
            requirements: BTreeMap::new(),
        }
    }

    /// Return only intervals not proved by the previous requirement snapshot.
    /// A topic-selector change invalidates every current interval in that
    /// family while leaving unrelated families' proofs intact.
    pub(super) fn differential_requirements(
        &self,
        current: &[RequiredWatchedTuple],
        current_topic0s_by_family: &Topic0sByFamily,
    ) -> Vec<RequiredWatchedTuple> {
        let current = normalize_requirements(current);
        let changed_topic_families =
            changed_topic_families(&self.topic0s_by_family, current_topic0s_by_family);
        let mut differential = Vec::new();

        for (key, current_intervals) in &current {
            if changed_topic_families.contains(&key.source_family) {
                differential.extend(required_tuples(key, current_intervals));
                continue;
            }
            let previous = self.requirements.get(key).map(Vec::as_slice).unwrap_or(&[]);
            differential.extend(required_tuples(
                key,
                &subtract_intervals(current_intervals, previous),
            ));
        }
        differential
    }

    pub(super) fn replace_requirements(
        &mut self,
        from_block: i64,
        current: &[RequiredWatchedTuple],
        current_topic0s_by_family: Topic0sByFamily,
        discovery_admission_epoch: i64,
    ) {
        self.from_block = self.from_block.min(from_block);
        self.requirements = normalize_requirements(current);
        self.topic0s_by_family = current_topic0s_by_family;
        self.discovery_admission_epoch = discovery_admission_epoch;
    }

    pub(super) fn extend_requirements(
        &mut self,
        requirements: &[RequiredWatchedTuple],
        through_block: i64,
        current_topic0s_by_family: Topic0sByFamily,
        discovery_admission_epoch: i64,
    ) {
        let mut combined = self.requirements_as_tuples();
        combined.extend_from_slice(requirements);
        self.requirements = normalize_requirements(&combined);
        self.through_block = through_block;
        self.topic0s_by_family = current_topic0s_by_family;
        self.discovery_admission_epoch = discovery_admission_epoch;
    }

    fn requirements_as_tuples(&self) -> Vec<RequiredWatchedTuple> {
        self.requirements
            .iter()
            .flat_map(|(key, intervals)| required_tuples(key, intervals))
            .collect()
    }
}

fn changed_topic_families(
    previous: &Topic0sByFamily,
    current: &Topic0sByFamily,
) -> BTreeSet<String> {
    current
        .iter()
        .filter_map(|(family, topics)| {
            (previous.get(family) != Some(topics)).then_some(family.clone())
        })
        .collect()
}

fn normalize_requirements(requirements: &[RequiredWatchedTuple]) -> RequirementSnapshot {
    let mut by_tuple = RequirementSnapshot::new();
    for requirement in requirements {
        by_tuple
            .entry(WatchedTupleKey {
                source_family: requirement.source_family.clone(),
                address: requirement.address.to_ascii_lowercase(),
            })
            .or_default()
            .push(BlockInterval {
                from_block: requirement.required_from_block,
                through_block: requirement.required_to_block,
            });
    }
    for intervals in by_tuple.values_mut() {
        *intervals = merge_intervals(std::mem::take(intervals));
    }
    by_tuple
}

fn merge_intervals(mut intervals: Vec<BlockInterval>) -> Vec<BlockInterval> {
    intervals.sort_by_key(|interval| (interval.from_block, interval.through_block));
    let mut merged: Vec<BlockInterval> = Vec::with_capacity(intervals.len());
    for interval in intervals {
        if let Some(previous) = merged.last_mut()
            && interval.from_block <= previous.through_block.saturating_add(1)
        {
            previous.through_block = previous.through_block.max(interval.through_block);
        } else {
            merged.push(interval);
        }
    }
    merged
}

fn subtract_intervals(current: &[BlockInterval], previous: &[BlockInterval]) -> Vec<BlockInterval> {
    let mut added = Vec::new();
    for interval in current {
        let mut cursor = interval.from_block;
        for proved in previous {
            if proved.through_block < cursor {
                continue;
            }
            if proved.from_block > interval.through_block {
                break;
            }
            if proved.from_block > cursor {
                added.push(BlockInterval {
                    from_block: cursor,
                    through_block: interval
                        .through_block
                        .min(proved.from_block.saturating_sub(1)),
                });
            }
            cursor = cursor.max(proved.through_block.saturating_add(1));
            if cursor > interval.through_block {
                break;
            }
        }
        if cursor <= interval.through_block {
            added.push(BlockInterval {
                from_block: cursor,
                through_block: interval.through_block,
            });
        }
    }
    added
}

fn required_tuples(
    key: &WatchedTupleKey,
    intervals: &[BlockInterval],
) -> Vec<RequiredWatchedTuple> {
    intervals
        .iter()
        .map(|interval| RequiredWatchedTuple {
            source_family: key.source_family.clone(),
            address: key.address.clone(),
            required_from_block: interval.from_block,
            required_to_block: interval.through_block,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirement(family: &str, address: &str, from: i64, through: i64) -> RequiredWatchedTuple {
        RequiredWatchedTuple {
            source_family: family.to_owned(),
            address: address.to_owned(),
            required_from_block: from,
            required_to_block: through,
        }
    }

    #[test]
    fn differential_keeps_only_added_intervals_and_changed_topic_families() {
        let mut state = VerifiedCoverageState::empty(
            10,
            BTreeMap::from([
                ("unchanged".to_owned(), BTreeSet::from(["0x01".to_owned()])),
                ("changed".to_owned(), BTreeSet::from(["0x02".to_owned()])),
            ]),
            1,
        );
        state.through_block = 100;
        state.replace_requirements(
            10,
            &[
                requirement("unchanged", "0xA", 10, 100),
                requirement("changed", "0xB", 20, 100),
            ],
            state.topic0s_by_family.clone(),
            1,
        );

        let differential = state.differential_requirements(
            &[
                requirement("unchanged", "0xA", 10, 100),
                requirement("unchanged", "0xC", 90, 100),
                requirement("changed", "0xB", 20, 100),
            ],
            &BTreeMap::from([
                ("unchanged".to_owned(), BTreeSet::from(["0x01".to_owned()])),
                ("changed".to_owned(), BTreeSet::from(["0x03".to_owned()])),
            ]),
        );

        assert_eq!(
            differential,
            vec![
                requirement("changed", "0xb", 20, 100),
                requirement("unchanged", "0xc", 90, 100),
            ]
        );
    }

    #[test]
    fn removing_or_shortening_requirements_adds_no_verification_work() {
        let topics = BTreeMap::from([("family".to_owned(), BTreeSet::from(["0x01".to_owned()]))]);
        let mut state = VerifiedCoverageState::empty(1, topics.clone(), 1);
        state.through_block = 100;
        state.replace_requirements(
            1,
            &[
                requirement("family", "0xA", 1, 100),
                requirement("family", "0xB", 20, 80),
            ],
            topics.clone(),
            1,
        );

        assert!(
            state
                .differential_requirements(&[requirement("family", "0xA", 1, 50)], &topics,)
                .is_empty()
        );
    }

    #[test]
    fn removing_all_family_topics_drops_proofs_before_later_readmission() {
        let family = "family";
        let requirements = [requirement(family, "0xA", 1, 100)];
        let topics = BTreeMap::from([(family.to_owned(), BTreeSet::from(["0x01".to_owned()]))]);
        let mut state = VerifiedCoverageState::empty(1, topics.clone(), 1);
        state.through_block = 100;
        state.replace_requirements(1, &requirements, topics.clone(), 1);

        // With no topic-bearing event the family is no longer log-producing:
        // it has no current coverage requirement, but its old proof must also
        // leave the snapshot rather than surviving for later reuse.
        assert!(
            state
                .differential_requirements(&[], &BTreeMap::new())
                .is_empty()
        );
        state.replace_requirements(1, &[], BTreeMap::new(), 2);

        assert_eq!(
            state.differential_requirements(&requirements, &topics),
            vec![requirement(family, "0xa", 1, 100)]
        );
    }
}
