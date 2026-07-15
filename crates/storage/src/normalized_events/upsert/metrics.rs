use std::collections::BTreeMap;

use crate::normalized_events::types::NormalizedEvent;

pub(super) fn count_normalized_events_by_event_kind(
    events: &[NormalizedEvent],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn count_normalized_events_by_source_family(
    events: &[NormalizedEvent],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.source_family.clone()).or_insert(0) += 1;
    }
    counts
}
