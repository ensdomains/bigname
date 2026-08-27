use std::collections::BTreeMap;

use anyhow::{Result, bail};

use super::watch::{Snapshot, WatchEmitter, watch_is_covered};

pub(super) type PersistedWatchFloors = BTreeMap<(String, String, String, String), u64>;

pub(super) fn widening_start(
    previous: &Snapshot,
    desired: &Snapshot,
    chain_id: &str,
    persisted_floors: &PersistedWatchFloors,
) -> Result<Option<u64>> {
    let previous = previous.watch_by_chain.get(chain_id);
    let Some(desired) = desired.watch_by_chain.get(chain_id) else {
        return Ok(None);
    };
    let widenings = desired.iter().filter(|(key, start)| {
        !watch_is_covered(previous, key, **start)
            && !desired_all_emitter_covers(desired, key, **start)
    });
    let mut widened_from = None;
    for (key, start) in widenings {
        if let WatchEmitter::Address { family, address } = &key.emitter
            && let Some(floor) = persisted_floors.get(&(
                chain_id.to_owned(),
                family.clone(),
                address.clone(),
                key.topic0.clone(),
            ))
            && start < floor
        {
            bail!(
                "compiled-watch comparison refused promised coverage start {start} below persisted ingest floor {floor} for chain {chain_id}, source family {family}, address {address}, topic {}",
                key.topic0
            );
        }
        widened_from = Some(widened_from.map_or(*start, |current: u64| current.min(*start)));
    }
    Ok(widened_from)
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
