use bigname_storage::CanonicalityState;

use super::super::types::ObservationRef;

pub(super) fn stable_row_anchor_for_reobservation(
    existing_state: CanonicalityState,
    existing_chain_id: &str,
    existing_block_hash: &str,
    existing_block_number: i64,
    incoming: &ObservationRef,
) -> (String, String, i64) {
    if existing_state == CanonicalityState::Orphaned {
        (
            incoming.chain_id.clone(),
            incoming.block_hash.clone(),
            incoming.block_number,
        )
    } else {
        (
            existing_chain_id.to_owned(),
            existing_block_hash.to_owned(),
            existing_block_number,
        )
    }
}

#[cfg(test)]
mod tests {
    use sqlx::types::{Uuid, time::OffsetDateTime};

    use super::*;

    #[test]
    fn stable_identity_anchor_refreshes_only_after_orphaning() {
        let incoming = ObservationRef {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xwinning".to_owned(),
            block_number: 10,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_710)
                .expect("test timestamp should fit"),
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000a1".to_owned(),
            emitting_contract_instance_id: Uuid::from_u128(0x12f1),
            source_manifest_id: 1,
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 1,
            namespace: "ens".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
        };

        assert_eq!(
            stable_row_anchor_for_reobservation(
                CanonicalityState::Finalized,
                "ethereum-sepolia",
                "0xstored",
                9,
                &incoming,
            ),
            ("ethereum-sepolia".to_owned(), "0xstored".to_owned(), 9),
            "a readable stable row must retain its first observation anchor"
        );
        assert_eq!(
            stable_row_anchor_for_reobservation(
                CanonicalityState::Orphaned,
                "ethereum-sepolia",
                "0xlosing",
                9,
                &incoming,
            ),
            (
                incoming.chain_id.clone(),
                incoming.block_hash.clone(),
                incoming.block_number,
            ),
            "an orphaned stable row must adopt the winning observation anchor"
        );
    }
}
