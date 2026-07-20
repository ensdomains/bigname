use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};
use bigname_storage::ResolverProfileInputChange;

#[path = "resolver_profile_convergence/authority.rs"]
mod authority;
#[path = "resolver_profile_convergence/drain.rs"]
mod drain;
#[path = "resolver_profile_convergence/invalidations.rs"]
mod invalidations;

use authority::ResolverProfileAuthorityEntry;
#[cfg(test)]
pub(crate) use authority::ResolverProfileAuthoritySnapshot;
pub(crate) use authority::{
    journal_resolver_profile_authority, journal_resolver_profile_authority_if_epoch_changed,
};
pub(crate) use drain::drain_resolver_profile_input_changes;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResolverProfileConvergenceSummary {
    pub(crate) loaded_input_count: usize,
    pub(crate) authority_target_read_statement_count: usize,
    pub(crate) max_authority_target_read_batch_size: usize,
    pub(crate) family_target_read_statement_count: usize,
    pub(crate) max_family_target_page_size: usize,
    #[cfg(test)]
    pub(crate) adapter_reconciliation_call_count: usize,
    pub(crate) reconciled_target_count: usize,
    pub(crate) invalidated_projection_key_count: u64,
    pub(crate) acknowledged_input_count: usize,
    pub(crate) concurrent_input_count: usize,
    pub(crate) deferred_input_count: usize,
    pub(crate) deferred_chains: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct ResolverProfileAuthorityIndex {
    entries_by_chain_and_address:
        BTreeMap<String, BTreeMap<String, Vec<ResolverProfileAuthorityEntry>>>,
    #[cfg(test)]
    addresses_by_chain_and_source_family: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    #[cfg(test)]
    indexed_entry_count: usize,
}

impl ResolverProfileAuthorityIndex {
    #[cfg(test)]
    fn from_entries(entries: impl IntoIterator<Item = ResolverProfileAuthorityEntry>) -> Self {
        let mut index = Self::default();
        for entry in entries {
            index.insert(entry);
        }
        index
    }

    fn insert(&mut self, entry: ResolverProfileAuthorityEntry) {
        #[cfg(test)]
        self.addresses_by_chain_and_source_family
            .entry(entry.chain.clone())
            .or_default()
            .entry(entry.source_family.clone())
            .or_default()
            .insert(entry.address.clone());
        self.entries_by_chain_and_address
            .entry(entry.chain.clone())
            .or_default()
            .entry(entry.address.clone())
            .or_default()
            .push(entry);
        #[cfg(test)]
        {
            self.indexed_entry_count += 1;
        }
    }

    #[cfg(test)]
    fn from_snapshot(authority: ResolverProfileAuthoritySnapshot) -> Self {
        Self::from_entries(authority.entries)
    }

    fn entries_for(&self, chain: &str, address: &str) -> Option<&[ResolverProfileAuthorityEntry]> {
        self.entries_by_chain_and_address
            .get(chain)?
            .get(address)
            .map(Vec::as_slice)
    }

    #[cfg(test)]
    fn addresses_for_family(&self, chain: &str, source_family: &str) -> Option<&BTreeSet<String>> {
        self.addresses_by_chain_and_source_family
            .get(chain)?
            .get(source_family)
    }
}

#[derive(Debug, Default)]
struct ResolverProfileReconciliationScope {
    direct_targets_by_chain: BTreeMap<String, BTreeSet<String>>,
    seed_families: BTreeSet<(String, String)>,
}

impl ResolverProfileConvergenceSummary {
    pub(crate) fn ensure_chain_completion_allowed(
        &self,
        chain: &str,
        completion: &str,
    ) -> Result<()> {
        if self.deferred_chains.contains(chain) {
            bail!(
                "resolver-profile reconciliation on chain {chain} remains deferred without resolver absence-replay authority; refusing {completion} until the database is fully rebuilt as a generation-zero raw-log corpus"
            );
        }
        Ok(())
    }
}

fn input_requires_reconciliation(
    input: &ResolverProfileInputChange,
    authority: &ResolverProfileAuthorityIndex,
) -> bool {
    input.force_reconciliation
        || authority
            .entries_for(&input.chain_id, &input.contract_address)
            .is_some()
}

fn reconciliation_scope(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthorityIndex,
) -> ResolverProfileReconciliationScope {
    let mut scope = ResolverProfileReconciliationScope::default();
    for input in pending {
        let current_entries = authority.entries_for(&input.chain_id, &input.contract_address);
        if current_entries.is_none() && !input.force_reconciliation {
            continue;
        }
        scope
            .direct_targets_by_chain
            .entry(input.chain_id.clone())
            .or_default()
            .insert(input.contract_address.clone());
        if let Some(current_entries) = current_entries {
            for seed in current_entries.iter().filter(|entry| entry.is_seed) {
                scope
                    .seed_families
                    .insert((seed.chain.clone(), seed.source_family.clone()));
            }
        }
    }
    scope
}

#[cfg(test)]
fn expanded_reconciliation_targets(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthorityIndex,
) -> BTreeMap<String, BTreeSet<String>> {
    expanded_reconciliation_targets_with_family_count(pending, authority).0
}

#[cfg(test)]
fn expanded_reconciliation_targets_with_family_count(
    pending: &[ResolverProfileInputChange],
    authority: &ResolverProfileAuthorityIndex,
) -> (BTreeMap<String, BTreeSet<String>>, usize) {
    let scope = reconciliation_scope(pending, authority);
    let mut targets = scope.direct_targets_by_chain;
    let expanded_seed_family_count = scope.seed_families.len();
    for (chain, source_family) in scope.seed_families {
        let Some(addresses) = authority.addresses_for_family(&chain, &source_family) else {
            continue;
        };
        targets
            .entry(chain)
            .or_default()
            .extend(addresses.iter().cloned());
    }

    (targets, expanded_seed_family_count)
}

#[cfg(test)]
#[path = "resolver_profile_convergence/tests.rs"]
mod tests;
