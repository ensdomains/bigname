use anyhow::{Context, Result};

pub(super) use crate::ens_v2_common::{hex_string, keccak_signature_hex, normalize_address};
use crate::evm_abi::{
    address_word_hex, dynamic_string, normalize_hex_32, u64_word, word_at, word_hex,
};

use super::{NAME_REGISTERED_SIGNATURE, NAME_RENEWED_SIGNATURE, raw_logs::RegistrarRawLogRow};

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
        new_expiry: i64,
        payment_token: String,
        referrer: String,
        base: String,
    },
}

pub(super) fn build_registrar_observation(
    raw_log: &RegistrarRawLogRow,
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id,
            label: dynamic_string(&raw_log.data, 0)?,
            owner: address_word_hex(&raw_log.data, 1)?,
            subregistry: address_word_hex(&raw_log.data, 2)?,
            resolver: address_word_hex(&raw_log.data, 3)?,
            duration: u64_word(&raw_log.data, 4)?,
            payment_token: address_word_hex(&raw_log.data, 5)?,
            referrer: word_hex(word_at(&raw_log.data, 6)?)?,
            base: word_hex(word_at(&raw_log.data, 7)?)?,
            premium: word_hex(word_at(&raw_log.data, 8)?)?,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_RENEWED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id,
            label: dynamic_string(&raw_log.data, 0)?,
            duration: u64_word(&raw_log.data, 1)?,
            new_expiry: u64_word(&raw_log.data, 2)?,
            payment_token: address_word_hex(&raw_log.data, 3)?,
            referrer: word_hex(word_at(&raw_log.data, 4)?)?,
            base: word_hex(word_at(&raw_log.data, 5)?)?,
        }));
    }

    Ok(None)
}
