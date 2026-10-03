//! The whole ENSv2 registries the lookahead loader keeps between batches. For each registry
//! whose `<registry>:*` [state key](../../../../docs/glossary.md#ensv2-state-key) a batch
//! requested, it holds what `events.sql` returns for that key alone before `valid_before`: the
//! latest readable event per retained state key. A later batch validates them as the
//! full-state loader validates its retained session, then reads only the blocks since
//! `valid_before` to bring them forward, instead of reloading each registry's history.
use std::collections::{BTreeMap, HashMap};

use sqlx::PgConnection;

use super::{PriorCache, lookahead_query::OrderedEvent, lookahead_query::v2_registry_delta};
use crate::{InterpretError, Result};

/// What the lookahead loader retains for a chain between batches.
pub(crate) struct LookaheadPrior {
    pub(crate) cache: PriorCache,
    pub(crate) registries: WholeRegistries,
}

#[derive(Default)]
pub(crate) struct WholeRegistries {
    valid_before: i64,
    /// Events keyed by `retained_state_key`, the key the adapter's restore compacts by.
    registries: BTreeMap<String, HashMap<String, OrderedEvent>>,
}

impl WholeRegistries {
    pub(crate) fn is_empty(&self) -> bool {
        self.registries.is_empty()
    }

    pub(super) fn contains(&self, registry: &str) -> bool {
        self.registries.contains_key(registry)
    }

    /// Makes every retained registry exact for a batch starting at `before`. Interpret is the
    /// sole writer of normalized events and writes a batch's events at the batch's own blocks,
    /// so between batches a registry's history changes only in `[valid_before, before)`; every
    /// retained event lies below `valid_before`, so any event of the same state key read here
    /// replaces it.
    pub(super) async fn bring_forward(
        &mut self,
        connection: &mut PgConnection,
        chain_id: &str,
        before: i64,
    ) -> Result<()> {
        if !self.registries.is_empty() && before > self.valid_before {
            let keys: Vec<String> = self.registries.keys().cloned().collect();
            for (registry, ordered) in
                v2_registry_delta(connection, chain_id, self.valid_before, before, &keys).await?
            {
                let events = self.registries.get_mut(&registry).ok_or_else(|| {
                    InterpretError::data_integrity(format!(
                        "retained ENSv2 registry read returned unrequested key {registry}"
                    ))
                })?;
                events.insert(ordered.event.retained_state_key.clone(), ordered);
            }
        }
        self.valid_before = before;
        Ok(())
    }

    pub(super) fn insert(&mut self, registry: String, loaded: Vec<OrderedEvent>) {
        let events = loaded
            .into_iter()
            .map(|ordered| (ordered.event.retained_state_key.clone(), ordered))
            .collect();
        self.registries.insert(registry, events);
    }

    pub(super) fn events<'a>(
        &'a self,
        registries: &'a [String],
    ) -> impl Iterator<Item = &'a OrderedEvent> + 'a {
        registries
            .iter()
            .filter_map(|registry| self.registries.get(registry))
            .flat_map(HashMap::values)
    }
}
