use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use uuid::Uuid;

use super::AddressAdmissionInput;

#[cfg(test)]
#[path = "catalog_admissions_tests.rs"]
mod tests;

type RetirementKey = (String, Uuid, String);

/// Stable sequence numbers preserve the original admission order after removals.
/// Secondary indexes contain only live entries, including during discovery churn.
#[derive(Clone, Default)]
pub(super) struct Admissions {
    entries: BTreeMap<usize, AddressAdmissionInput>,
    by_address: HashMap<String, BTreeSet<usize>>,
    by_instance: HashMap<Uuid, BTreeSet<usize>>,
    by_role: HashMap<String, BTreeSet<usize>>,
    by_observation: HashMap<RetirementKey, BTreeSet<usize>>,
    next_id: usize,
    generation: u64,
    /// The block of the latest discovery admission, and the entries admitted in it.
    admitted_block: Option<i64>,
    admitted: HashSet<AddressAdmissionInput>,
    /// The entries the current block pass retired, by sequence number.
    pass_retirements: Vec<(usize, AddressAdmissionInput)>,
}

impl Admissions {
    pub(super) fn new(admissions: Vec<AddressAdmissionInput>) -> Self {
        let mut index = Self::default();
        // Historical inputs can contain several ranges for the same observation.
        // Replacement applies only to subsequent calls to Catalog::admit.
        for admission in admissions {
            index.push(admission);
        }
        index
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &AddressAdmissionInput> {
        self.entries.values()
    }

    pub(super) fn for_address(
        &self,
        address: &str,
    ) -> impl Iterator<Item = &AddressAdmissionInput> {
        self.lookup(self.by_address.get(&address.to_ascii_lowercase()))
    }

    pub(super) fn for_instance(
        &self,
        instance: Uuid,
    ) -> impl Iterator<Item = &AddressAdmissionInput> {
        self.lookup(self.by_instance.get(&instance))
    }

    pub(super) fn for_role(&self, role: &str) -> impl Iterator<Item = &AddressAdmissionInput> {
        self.lookup(self.by_role.get(role))
    }

    fn lookup<'a>(
        &'a self,
        ids: Option<&'a BTreeSet<usize>>,
    ) -> impl Iterator<Item = &'a AddressAdmissionInput> {
        ids.into_iter().flatten().map(|id| &self.entries[id])
    }

    /// Admits a discovered address, replacing the live entries of its discovery observation.
    /// Counts a new generation only for an entry that is neither live nor admitted before in its
    /// block, so re-admitting an entry never counts and each counts at most once per block.
    pub(super) fn admit(&mut self, admission: AddressAdmissionInput) {
        if self.admitted_block != admission.active_from_block {
            self.admitted_block = admission.active_from_block;
            self.admitted.clear();
        }
        let live = self
            .for_address(&admission.address)
            .any(|entry| *entry == admission);
        if !live && self.admitted.insert(admission.clone()) {
            self.generation += 1;
        }
        if let Some((edge_kind, from, observation_key)) = retirement_key(&admission) {
            self.retire(&edge_kind, from, &observation_key);
        }
        self.push(admission);
    }

    /// Starts a pass over a block and returns the generation it starts from.
    pub(super) fn begin_pass(&mut self) -> u64 {
        self.pass_retirements.clear();
        self.generation
    }

    /// Ends a pass over a block and says whether it is the block's last: the first pass that
    /// admits no new entry. A pass that did admit one is discarded. Every entry it retired is put
    /// back under its own sequence number and its admissions stay, so the next pass starts from
    /// the block's starting admissions plus every entry admitted in the block, and a retirement
    /// takes effect only at its log in the last pass.
    pub(super) fn end_pass(&mut self, generation: u64) -> bool {
        if self.generation == generation {
            return true;
        }
        for (_, admission) in std::mem::take(&mut self.pass_retirements) {
            self.push(admission);
        }
        false
    }

    pub(super) fn push(&mut self, admission: AddressAdmissionInput) {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("admission sequence overflow");
        self.insert(id, admission);
    }

    fn insert(&mut self, id: usize, admission: AddressAdmissionInput) {
        self.by_address
            .entry(admission.address.to_ascii_lowercase())
            .or_default()
            .insert(id);
        self.by_instance
            .entry(admission.contract_instance_id)
            .or_default()
            .insert(id);
        if let Some(role) = &admission.role {
            self.by_role.entry(role.clone()).or_default().insert(id);
        }
        if let Some(key) = retirement_key(&admission) {
            self.by_observation.entry(key).or_default().insert(id);
        }
        self.entries.insert(id, admission);
    }

    pub(super) fn retire(&mut self, edge_kind: &str, from: Uuid, observation_key: &str) {
        let key = (edge_kind.to_owned(), from, observation_key.to_owned());
        let Some(ids) = self.by_observation.remove(&key) else {
            return;
        };
        for id in ids {
            let admission = self.entries.remove(&id).expect("indexed admission exists");
            remove_id(
                &mut self.by_address,
                &admission.address.to_ascii_lowercase(),
                id,
            );
            remove_id(&mut self.by_instance, &admission.contract_instance_id, id);
            if let Some(role) = &admission.role {
                remove_id(&mut self.by_role, role, id);
            }
            self.pass_retirements.push((id, admission));
        }
    }
}

fn retirement_key(admission: &AddressAdmissionInput) -> Option<RetirementKey> {
    Some((
        admission.discovery_edge_kind.clone()?,
        admission.discovery_from_contract_instance_id?,
        admission.discovery_observation_key.clone()?,
    ))
}

fn remove_id<K: std::hash::Hash + Eq>(index: &mut HashMap<K, BTreeSet<usize>>, key: &K, id: usize) {
    if let Some(ids) = index.get_mut(key) {
        ids.remove(&id);
        if ids.is_empty() {
            index.remove(key);
        }
    }
}
