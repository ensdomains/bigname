use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{CanonicalityState, RawBlock, normalize_evm_address, normalize_evm_b256};

use crate::provider::ProviderBlockCodeObservationRequest;

#[derive(Default)]
pub(super) struct SparseCodeObservationPlan {
    addresses_by_block: BTreeMap<(i64, String), BTreeSet<String>>,
    canonicality_by_block: BTreeMap<(i64, String), CanonicalityState>,
}

impl SparseCodeObservationPlan {
    pub(super) fn record(&mut self, raw_block: &RawBlock, addresses: &BTreeSet<String>) {
        let key = (raw_block.block_number, raw_block.block_hash.clone());
        self.addresses_by_block
            .entry(key.clone())
            .or_default()
            .extend(addresses.iter().cloned());
        self.canonicality_by_block
            .entry(key)
            .and_modify(|state| {
                if state.rank() < raw_block.canonicality_state.rank() {
                    *state = raw_block.canonicality_state;
                }
            })
            .or_insert(raw_block.canonicality_state);
    }

    pub(super) fn block_hashes(&self) -> Vec<String> {
        self.addresses_by_block
            .keys()
            .map(|(_, block_hash)| block_hash.clone())
            .collect()
    }

    pub(super) fn contract_addresses(&self) -> Vec<String> {
        self.addresses_by_block
            .values()
            .flat_map(|addresses| {
                addresses
                    .iter()
                    .map(|address| normalize_evm_address(address))
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(super) fn retain_missing_stored_observations(
        &mut self,
        stored_observations: &BTreeMap<(String, String), CanonicalityState>,
    ) {
        self.addresses_by_block
            .retain(|(block_number, block_hash), addresses| {
                let desired_state = self
                    .canonicality_by_block
                    .get(&(*block_number, block_hash.clone()))
                    .copied()
                    .unwrap_or(CanonicalityState::Observed);
                let block_hash = normalize_evm_b256(block_hash);
                addresses.retain(|address| {
                    let address = normalize_evm_address(address);
                    let stored_state = stored_observations.get(&(block_hash.clone(), address));
                    !stored_state.is_some_and(|state| {
                        *state != CanonicalityState::Orphaned
                            && state.rank() >= desired_state.rank()
                    })
                });
                !addresses.is_empty()
            });
    }

    pub(super) fn requests(&self) -> Vec<ProviderBlockCodeObservationRequest> {
        self.addresses_by_block
            .clone()
            .into_iter()
            .map(
                |((block_number, block_hash), addresses)| ProviderBlockCodeObservationRequest {
                    block_number,
                    block_hash,
                    addresses: addresses.into_iter().collect(),
                },
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_missing_stored_observations_keeps_missing_address_on_partially_observed_block() {
        let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let selected_address = "0x0000000000000000000000000000000000000002";
        let mut plan = SparseCodeObservationPlan {
            addresses_by_block: BTreeMap::from([(
                (42, block_hash.to_owned()),
                BTreeSet::from([selected_address.to_owned()]),
            )]),
            canonicality_by_block: BTreeMap::from([(
                (42, block_hash.to_owned()),
                CanonicalityState::Canonical,
            )]),
        };
        let stored_observations = BTreeMap::from([(
            (
                block_hash.to_owned(),
                "0x0000000000000000000000000000000000000001".to_owned(),
            ),
            CanonicalityState::Canonical,
        )]);

        plan.retain_missing_stored_observations(&stored_observations);

        assert_eq!(plan.requests().len(), 1);
        assert_eq!(plan.requests()[0].addresses, vec![selected_address]);
    }

    #[test]
    fn retain_missing_stored_observations_keeps_weaker_stored_canonicality() {
        let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let selected_address = "0x0000000000000000000000000000000000000002";
        let mut plan = SparseCodeObservationPlan {
            addresses_by_block: BTreeMap::from([(
                (42, block_hash.to_owned()),
                BTreeSet::from([selected_address.to_owned()]),
            )]),
            canonicality_by_block: BTreeMap::from([(
                (42, block_hash.to_owned()),
                CanonicalityState::Canonical,
            )]),
        };
        let stored_observations = BTreeMap::from([(
            (block_hash.to_owned(), selected_address.to_owned()),
            CanonicalityState::Observed,
        )]);

        plan.retain_missing_stored_observations(&stored_observations);

        assert_eq!(plan.requests().len(), 1);
        assert_eq!(plan.requests()[0].addresses, vec![selected_address]);
    }
}
