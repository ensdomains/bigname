//! The event logs a committed batch skipped because they contradict themselves or their
//! emitter's own earlier logs. Each gets one warning, and the batch outcome counts them by the
//! check that failed.
use std::collections::BTreeMap;

use bigname_adapters::schema_v2::DecodeSkip;

pub(super) fn report(chain_id: &str, skips: &[DecodeSkip]) -> BTreeMap<&'static str, u64> {
    let mut counts = BTreeMap::new();
    for skip in skips {
        let Some(site) = skip.inconsistency_site else {
            continue;
        };
        tracing::warn!(
            chain_id,
            block_number = skip.block_number,
            block_hash = %skip.block_hash,
            transaction_hash = %skip.transaction_hash,
            log_index = skip.log_index,
            emitting_address = %skip.emitting_address,
            source_family = %skip.source_family,
            site,
            decode_context = %skip.decode_context,
            "skipped an inconsistent event log from an emitter no manifest declares"
        );
        *counts.entry(site).or_default() += 1;
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skip(log_index: i64, inconsistency_site: Option<&'static str>) -> DecodeSkip {
        DecodeSkip {
            chain_id: "test-chain".to_owned(),
            block_hash: "block-1".to_owned(),
            block_number: 1,
            transaction_hash: "transaction-1".to_owned(),
            log_index,
            emitting_address: "0x0000000000000000000000000000000000000042".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            selection_topic0: "0x01".to_owned(),
            match_all: false,
            decode_context: "context".to_owned(),
            inconsistency_site,
        }
    }

    #[test]
    fn counts_only_inconsistent_skips_by_site() {
        let counts = report(
            "test-chain",
            &[
                skip(0, None),
                skip(1, Some("label_hash_mismatch")),
                skip(2, Some("token_resource_conflict")),
                skip(3, Some("label_hash_mismatch")),
            ],
        );
        assert_eq!(
            counts,
            BTreeMap::from([("label_hash_mismatch", 2), ("token_resource_conflict", 1)])
        );
    }
}
