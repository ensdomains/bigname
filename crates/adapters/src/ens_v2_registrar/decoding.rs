use alloy_sol_types::sol;
use anyhow::Result;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
pub(super) use crate::ens_v2_common::{hex_string, normalize_address};
use crate::evm_abi::{
    address_hex, decode_event_log, hex_string as prefixed_hex_string, saturating_seconds_i64,
    u256_word_hex,
};

use super::{
    ABI_EVENT_NAME_REGISTERED_SIGNATURE, ABI_EVENT_NAME_RENEWED_SIGNATURE,
    raw_logs::RegistrarRawLogRow,
};

mod legacy_events {
    use alloy_sol_types::sol;

    sol! {
        #[derive(Debug)]
        event NameRegistered(
            uint256 indexed tokenId,
            string label,
            address owner,
            address subregistry,
            address resolver,
            uint64 duration,
            address paymentToken,
            bytes32 referrer,
            uint256 base,
            uint256 premium
        );

        #[derive(Debug)]
        event NameRenewed(
            uint256 indexed tokenId,
            string label,
            uint64 duration,
            uint64 newExpiry,
            address paymentToken,
            bytes32 referrer,
            uint256 base
        );
    }
}

sol! {
    #[derive(Debug)]
    event NameRegistered(
        uint256 indexed tokenId,
        string label,
        address owner,
        address subregistry,
        address resolver,
        uint64 duration,
        address paymentToken,
        bytes32 indexed referrer,
        uint256 base,
        uint256 premium
    );

    #[derive(Debug)]
    event NameRenewed(
        uint256 indexed tokenId,
        string label,
        uint64 duration,
        uint64 newExpiry,
        address paymentToken,
        bytes32 indexed referrer,
        uint256 amount
    );
}

pub(super) enum RegistrarObservation {
    NameRegistered {
        token_id: String,
        label: String,
        owner: String,
        subregistry: String,
        resolver: String,
        duration: i64,
        payment_token: String,
        referrer: String,
        base: String,
        premium: String,
    },
    NameRenewed {
        token_id: String,
        label: String,
        duration: i64,
        new_expiry: u64,
        payment_token: String,
        referrer: String,
        payment: RenewalPayment,
    },
}

pub(super) enum RenewalPayment {
    LegacyBase(String),
    PostAuditAmount(String),
}

pub(super) fn build_registrar_observation(
    raw_log: &RegistrarRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if event_topics.matches(ABI_EVENT_NAME_REGISTERED_SIGNATURE, topic0)? {
        if raw_log.topics.len() == 2 {
            let event = decode_event_log::<legacy_events::NameRegistered>(
                &raw_log.topics,
                &raw_log.data,
                "legacy NameRegistered log is malformed",
            )?;
            return Ok(Some(RegistrarObservation::NameRegistered {
                token_id: u256_word_hex(event.tokenId),
                label: event.label,
                owner: address_hex(event.owner),
                subregistry: address_hex(event.subregistry),
                resolver: address_hex(event.resolver),
                duration: saturating_seconds_i64(event.duration),
                payment_token: address_hex(event.paymentToken),
                referrer: prefixed_hex_string(event.referrer.as_slice()),
                base: u256_word_hex(event.base),
                premium: u256_word_hex(event.premium),
            }));
        }
        let event = decode_event_log::<NameRegistered>(
            &raw_log.topics,
            &raw_log.data,
            "NameRegistered log is malformed",
        )?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id: u256_word_hex(event.tokenId),
            label: event.label,
            owner: address_hex(event.owner),
            subregistry: address_hex(event.subregistry),
            resolver: address_hex(event.resolver),
            duration: saturating_seconds_i64(event.duration),
            payment_token: address_hex(event.paymentToken),
            referrer: prefixed_hex_string(event.referrer.as_slice()),
            base: u256_word_hex(event.base),
            premium: u256_word_hex(event.premium),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAME_RENEWED_SIGNATURE, topic0)? {
        if raw_log.topics.len() == 2 {
            let event = decode_event_log::<legacy_events::NameRenewed>(
                &raw_log.topics,
                &raw_log.data,
                "legacy NameRenewed log is malformed",
            )?;
            return Ok(Some(RegistrarObservation::NameRenewed {
                token_id: u256_word_hex(event.tokenId),
                label: event.label,
                duration: saturating_seconds_i64(event.duration),
                new_expiry: event.newExpiry,
                payment_token: address_hex(event.paymentToken),
                referrer: prefixed_hex_string(event.referrer.as_slice()),
                payment: RenewalPayment::LegacyBase(u256_word_hex(event.base)),
            }));
        }
        let event = decode_event_log::<NameRenewed>(
            &raw_log.topics,
            &raw_log.data,
            "NameRenewed log is malformed",
        )?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id: u256_word_hex(event.tokenId),
            label: event.label,
            duration: saturating_seconds_i64(event.duration),
            new_expiry: event.newExpiry,
            payment_token: address_hex(event.paymentToken),
            referrer: prefixed_hex_string(event.referrer.as_slice()),
            payment: RenewalPayment::PostAuditAmount(u256_word_hex(event.amount)),
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_primitives::{Address, B256, U256};
    use alloy_sol_types::SolEvent;

    use super::*;
    use crate::ens_v2_registrar::{
        ABI_EVENT_NAME_REGISTERED_SIGNATURE, ABI_EVENT_NAME_RENEWED_SIGNATURE,
    };

    #[test]
    fn decodes_legacy_and_post_audit_referrer_layouts() -> Result<()> {
        let referrer = B256::repeat_byte(0xa5);
        let owner = Address::repeat_byte(0x11);
        let payment_token = Address::repeat_byte(0x22);
        let topics = ActiveManifestEventTopic0sBySignature::new(HashMap::from([
            (
                ABI_EVENT_NAME_REGISTERED_SIGNATURE.to_owned(),
                format!("{:#x}", NameRegistered::SIGNATURE_HASH),
            ),
            (
                ABI_EVENT_NAME_RENEWED_SIGNATURE.to_owned(),
                format!("{:#x}", NameRenewed::SIGNATURE_HASH),
            ),
        ]));

        let registered = NameRegistered {
            tokenId: U256::from(7),
            label: "postaudit".to_owned(),
            owner,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 2_419_200,
            paymentToken: payment_token,
            referrer,
            base: U256::from(10),
            premium: U256::from(3),
        }
        .encode_log_data();
        assert_eq!(registered.topics().len(), 3);
        let registered = build_registrar_observation(&raw_log(registered), &topics)?;
        assert!(matches!(
            registered,
            Some(RegistrarObservation::NameRegistered {
                label,
                referrer: decoded,
                ..
            }) if label == "postaudit" && decoded == format!("{referrer:#x}")
        ));

        let legacy_registered = legacy_events::NameRegistered {
            tokenId: U256::from(6),
            label: "legacy".to_owned(),
            owner,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
            duration: 2_419_200,
            paymentToken: payment_token,
            referrer,
            base: U256::from(8),
            premium: U256::from(2),
        }
        .encode_log_data();
        assert_eq!(legacy_registered.topics().len(), 2);
        let legacy_registered = build_registrar_observation(&raw_log(legacy_registered), &topics)?;
        assert!(matches!(
            legacy_registered,
            Some(RegistrarObservation::NameRegistered {
                label,
                referrer: decoded,
                ..
            }) if label == "legacy" && decoded == format!("{referrer:#x}")
        ));

        let renewed = NameRenewed {
            tokenId: U256::from(7),
            label: "postaudit".to_owned(),
            duration: 86_400,
            newExpiry: 2_000_000_000,
            paymentToken: payment_token,
            referrer,
            amount: U256::from(4),
        }
        .encode_log_data();
        assert_eq!(renewed.topics().len(), 3);
        let renewed = build_registrar_observation(&raw_log(renewed), &topics)?;
        assert!(matches!(
            renewed,
            Some(RegistrarObservation::NameRenewed {
                label,
                referrer: decoded,
                payment: RenewalPayment::PostAuditAmount(amount),
                ..
            }) if label == "postaudit"
                && decoded == format!("{referrer:#x}")
                && amount == format!("0x{:064x}", 4)
        ));

        let legacy_renewed = legacy_events::NameRenewed {
            tokenId: U256::from(6),
            label: "legacy".to_owned(),
            duration: 86_400,
            newExpiry: 1_900_000_000,
            paymentToken: payment_token,
            referrer,
            base: U256::from(5),
        }
        .encode_log_data();
        assert_eq!(legacy_renewed.topics().len(), 2);
        let legacy_renewed = build_registrar_observation(&raw_log(legacy_renewed), &topics)?;
        assert!(matches!(
            legacy_renewed,
            Some(RegistrarObservation::NameRenewed {
                label,
                referrer: decoded,
                payment: RenewalPayment::LegacyBase(amount),
                ..
            }) if label == "legacy"
                && decoded == format!("{referrer:#x}")
                && amount == format!("0x{:064x}", 5)
        ));

        Ok(())
    }

    fn raw_log(log: alloy_primitives::LogData) -> RegistrarRawLogRow {
        RegistrarRawLogRow {
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 1,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: format!("{:#x}", Address::repeat_byte(0xee)),
            topics: log
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: log.data.to_vec(),
            canonicality_state: bigname_storage::CanonicalityState::Canonical,
            source_manifest_id: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_registrar_l1".to_owned(),
            manifest_version: 3,
        }
    }
}
