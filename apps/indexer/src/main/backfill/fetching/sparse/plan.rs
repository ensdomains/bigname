use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::RawBlock;

use crate::provider::ProviderBlockCodeObservationRequest;

#[derive(Default)]
pub(super) struct SparseCodeObservationPlan {
    addresses_by_block: BTreeMap<(i64, String), BTreeSet<String>>,
}

impl SparseCodeObservationPlan {
    pub(super) fn record(&mut self, raw_block: &RawBlock, addresses: &BTreeSet<String>) {
        self.addresses_by_block
            .entry((raw_block.block_number, raw_block.block_hash.clone()))
            .or_default()
            .extend(addresses.iter().cloned());
    }

    pub(super) fn requests(&self) -> Vec<ProviderBlockCodeObservationRequest> {
        self.addresses_by_block
            .clone()
            .into_iter()
            .map(
                |((_block_number, block_hash), addresses)| ProviderBlockCodeObservationRequest {
                    block_hash,
                    addresses: addresses.into_iter().collect(),
                },
            )
            .collect()
    }
}
