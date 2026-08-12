use imbl::{ordmap::OrdMap, ordset::OrdSet};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{state::State, state_key::interpreter_state_key};

/// Maximum number of persisted per-key JSON values retained in process for one chain.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StateCacheCapacity {
    #[default]
    Unlimited,
    Entries(usize),
}

#[derive(Clone, Debug)]
pub(super) struct StateResidency {
    capacity: StateCacheCapacity,
    values: OrdMap<String, Value>,
    accessed_at: OrdMap<String, u64>,
    least_recent: OrdSet<(u64, String)>,
    clock: u64,
}

impl PartialEq for StateResidency {
    fn eq(&self, other: &Self) -> bool {
        self.capacity == other.capacity && self.values == other.values
    }
}

impl Eq for StateResidency {}

impl StateResidency {
    pub(super) fn new(capacity: StateCacheCapacity) -> Self {
        Self {
            capacity,
            values: OrdMap::new(),
            accessed_at: OrdMap::new(),
            least_recent: OrdSet::new(),
            clock: 0,
        }
    }

    pub(super) fn get(&mut self, key: &str) -> Option<Value> {
        let value = self.values.get(key).cloned()?;
        self.touch(key);
        Some(value)
    }

    pub(super) fn insert(&mut self, key: String, value: Value) {
        self.values.insert(key.clone(), value);
        self.touch(&key);
        self.evict();
    }

    fn touch(&mut self, key: &str) {
        if let Some(previous) = self.accessed_at.remove(key) {
            self.least_recent.remove(&(previous, key.to_owned()));
        }
        self.clock = self.clock.checked_add(1).unwrap_or_else(|| {
            self.renumber();
            self.clock + 1
        });
        self.accessed_at.insert(key.to_owned(), self.clock);
        self.least_recent.insert((self.clock, key.to_owned()));
    }

    fn evict(&mut self) {
        let StateCacheCapacity::Entries(capacity) = self.capacity else {
            return;
        };
        while self.values.len() > capacity {
            let Some((accessed_at, key)) = self.least_recent.iter().next().cloned() else {
                break;
            };
            self.least_recent.remove(&(accessed_at, key.clone()));
            self.accessed_at.remove(&key);
            self.values.remove(&key);
        }
    }

    fn renumber(&mut self) {
        let keys = self
            .least_recent
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        self.accessed_at.clear();
        self.least_recent.clear();
        self.clock = 0;
        for key in keys {
            self.clock += 1;
            self.accessed_at.insert(key.clone(), self.clock);
            self.least_recent.insert((self.clock, key));
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.values.len()
    }
}

impl State {
    pub(super) fn value_tail(&mut self, key: &str) -> Option<Value> {
        self.values.get(key)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn transition(
        &mut self,
        namespace: &str,
        logical_name_id: Option<&str>,
        resource_id: Option<Uuid>,
        event_kind: &str,
        source_family: &str,
        state_scope: &str,
        explicit_before: Option<Value>,
        after: Value,
    ) -> Value {
        let key = interpreter_state_key(
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            state_scope,
        );
        let before = explicit_before
            .or_else(|| self.provisional_values.get(&key).cloned())
            .or_else(|| self.values.get(&key))
            .unwrap_or_else(|| json!({}));
        self.provisional_values.insert(key, after);
        before
    }

    pub(super) fn clear_provisional_values(&mut self) {
        self.provisional_values.clear();
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn evicts_the_least_recently_used_value() {
        let mut cache = StateResidency::new(StateCacheCapacity::Entries(2));
        cache.insert("old".to_owned(), json!({"value": 1}));
        cache.insert("hot".to_owned(), json!({"value": 2}));
        assert!(cache.get("old").is_some());

        cache.insert("new".to_owned(), json!({"value": 3}));

        assert!(cache.get("hot").is_none());
        assert!(cache.get("old").is_some());
        assert!(cache.get("new").is_some());
    }

    #[test]
    fn zero_capacity_retains_no_values() {
        let mut cache = StateResidency::new(StateCacheCapacity::Entries(0));
        cache.insert("key".to_owned(), json!({"value": 1}));

        assert_eq!(cache.len(), 0);
        assert!(cache.get("key").is_none());
    }
}
