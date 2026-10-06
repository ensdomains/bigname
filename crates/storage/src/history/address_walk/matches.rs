//! Fixed-batch exact membership. Cache only compact answers, never composed facts or history.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    AddressRead, Witness,
    seams::{self, Live},
};
use crate::{
    address_names::push_address_names_current_query,
    families::{name::servable_publication, records::AddressComposer},
    history::attribution::matching_attribution_pairs,
};

type NameKey = (String, String);

pub(super) struct Membership {
    current: Cache<NameKey, Option<Uuid>>,
    attribution: Cache<(Uuid, i64), bool>,
}

impl Membership {
    pub(super) fn new() -> Self {
        Self {
            current: Cache::new("cached_memberships"),
            attribution: Cache::new("cached_attribution"),
        }
    }

    pub(super) async fn validate(
        &mut self,
        connection: &mut PgConnection,
        read: &AddressRead<'_>,
        witnesses: &[Witness],
        already_matched: Option<i64>,
    ) -> Result<Vec<bool>> {
        // Kind 0 already passed the historical matcher. A direct witness makes every other
        // witness for that event unnecessary, including witnesses fetched in a later batch.
        let mut accepted: BTreeSet<i64> = witnesses
            .iter()
            .filter(|w| w.witness_kind == 0)
            .map(|w| w.normalized_event_id)
            .collect();
        accepted.extend(already_matched);
        let mut accepted_live = Live::new("accepted_witness_ids", accepted.len());
        let mut missing = BTreeMap::<String, BTreeSet<String>>::new();
        for witness in witnesses.iter().filter(|w| {
            matches!(w.witness_kind, 1 | 2 | 4) && !accepted.contains(&w.normalized_event_id)
        }) {
            let key = witness.name_key()?;
            if self.current.get(&key).is_none() {
                missing.entry(key.0).or_default().insert(key.1);
            } else {
                seams::count("membership_cache_hits", 1);
            }
        }
        self.load_current(connection, read, missing).await?;
        for witness in witnesses.iter().filter(|w| matches!(w.witness_kind, 1 | 2)) {
            if accepted.contains(&witness.normalized_event_id) {
                continue;
            }
            if let Some(Some(resource)) = self.current.get(&witness.name_key()?)
                && (witness.witness_kind == 1 || Some(*resource) == witness.witness_resource)
            {
                accepted.insert(witness.normalized_event_id);
            }
        }
        let mut needed = BTreeSet::new();
        accepted_live.set(accepted.len());
        for witness in witnesses.iter().filter(|w| matches!(w.witness_kind, 3 | 4)) {
            if accepted.contains(&witness.normalized_event_id) {
                continue;
            }
            let resource = witness
                .witness_resource
                .context("missing attribution witness resource")?;
            if witness.witness_kind == 4
                && self.current.get(&witness.name_key()?).copied().flatten() != Some(resource)
            {
                continue;
            }
            let pair = (resource, witness.normalized_event_id);
            match self.attribution.get(&pair) {
                Some(true) => {
                    accepted.insert(witness.normalized_event_id);
                    seams::count("attribution_cache_hits", 1);
                }
                Some(false) => {
                    seams::count("attribution_cache_hits", 1);
                }
                None => {
                    needed.insert(pair);
                }
            }
        }
        if !needed.is_empty() {
            accepted_live.set(accepted.len());
            let pairs: Vec<_> = needed.into_iter().collect();
            let _input = Live::new("attribution_inputs", pairs.len());
            let matched = matching_attribution_pairs(connection, &pairs, read.published).await?;
            let _output = Live::new("attribution_results", matched.len());
            seams::count("attribution_batches", 1);
            for pair in pairs {
                let valid = matched.contains(&pair);
                self.attribution.insert(pair, valid);
                if valid {
                    accepted.insert(pair.1);
                }
            }
        }
        accepted_live.set(accepted.len());
        Ok(witnesses
            .iter()
            .map(|w| accepted.contains(&w.normalized_event_id))
            .collect())
    }

    async fn load_current(
        &mut self,
        connection: &mut PgConnection,
        read: &AddressRead<'_>,
        missing: BTreeMap<String, BTreeSet<String>>,
    ) -> Result<()> {
        let composer = AddressComposer {
            address: read.address,
            include_roles: crate::families::records::includes_roles(read.relations),
            with_history_evidence: true,
        };
        for (chain, names) in missing {
            let publication = servable_publication(connection, &chain).await?;
            let names: Vec<_> = names.into_iter().collect();
            let _input = Live::new("composed_inputs", names.len());
            seams::count("names_composed", names.len());
            seams::count("composition_batches", 1);
            let rows = composer.compose(connection, &publication, &names).await?;
            let _composed = Live::new("composed_results", rows.len());
            let composed = Value::Array(rows.into_iter().flat_map(|name| name.rows).collect());
            let _relations = Live::new(
                "composed_relation_rows",
                composed.as_array().map_or(0, Vec::len),
            );
            let mut builder = QueryBuilder::<Postgres>::new(
                "SELECT DISTINCT logical_name_id, resource_id FROM (",
            );
            push_address_names_current_query(
                &mut builder,
                &composed,
                read.address,
                read.namespace,
                read.relations,
                !read.canonical_only,
                read.published,
            );
            builder.push(") current_anchors");
            let current: Vec<(String, Uuid)> = builder
                .build_query_as()
                .fetch_all(&mut *connection)
                .await
                .context("failed to validate current address-history membership")?;
            let _anchors = Live::new("current_anchors", current.len());
            let mut resources: BTreeMap<String, Uuid> = current.into_iter().collect();
            for name in names {
                let resource = resources.remove(&name);
                self.current.insert((chain.clone(), name), resource);
            }
        }
        Ok(())
    }
}

impl Witness {
    fn name_key(&self) -> Result<NameKey> {
        Ok((
            self.current_chain
                .clone()
                .context("missing current witness chain")?,
            self.current_name
                .clone()
                .context("missing current witness name")?,
        ))
    }
}

/// A fixed-size LRU with one key allocation per entry. Eviction scans at most the fixed
/// capacity; no queue of duplicate hits or historical answers grows during a long count.
struct Cache<K, V> {
    values: BTreeMap<K, (V, u64)>,
    tick: u64,
    live: Live,
}
impl<K: Ord + Clone, V> Cache<K, V> {
    fn new(kind: &'static str) -> Self {
        Self {
            values: BTreeMap::new(),
            tick: 0,
            live: Live::new(kind, 0),
        }
    }
    fn get(&mut self, key: &K) -> Option<&V> {
        self.tick += 1;
        self.values.get_mut(key).map(|(value, tick)| {
            *tick = self.tick;
            &*value
        })
    }
    fn insert(&mut self, key: K, value: V) {
        if self.values.len() >= seams::cache_capacity() && !self.values.contains_key(&key) {
            let oldest = self
                .values
                .iter()
                .min_by_key(|(_, (_, tick))| tick)
                .map(|(key, _)| key.clone())
                .expect("nonempty cache");
            self.values.remove(&oldest);
        }
        self.tick += 1;
        self.values.insert(key, (value, self.tick));
        self.live.set(self.values.len());
    }
}
