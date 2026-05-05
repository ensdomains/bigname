use alloy_primitives::U256;
use alloy_sol_types::{SolType, sol_data};
use anyhow::{Context, Result};

pub(super) use crate::ens_v2_common::{hex_string, keccak_signature_hex, normalize_address};
use crate::evm_abi::{hex_string as prefixed_hex_string, normalize_hex_32};

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
        let (label, owner, subregistry, resolver, duration, payment_token, referrer, base, premium) =
            <(
                sol_data::String,
                sol_data::Address,
                sol_data::Address,
                sol_data::Address,
                sol_data::Uint<64>,
                sol_data::Address,
                sol_data::FixedBytes<32>,
                sol_data::Uint<256>,
                sol_data::Uint<256>,
            )>::abi_decode_params_validate(&raw_log.data)
            .context("NameRegistered data is malformed")?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id,
            label,
            owner: prefixed_hex_string(owner.as_slice()),
            subregistry: prefixed_hex_string(subregistry.as_slice()),
            resolver: prefixed_hex_string(resolver.as_slice()),
            duration: i64::try_from(duration).context("NameRegistered duration exceeds i64")?,
            payment_token: prefixed_hex_string(payment_token.as_slice()),
            referrer: prefixed_hex_string(referrer.as_slice()),
            base: u256_word_hex(base),
            premium: u256_word_hex(premium),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_RENEWED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed missing tokenId topic")?,
        )?;
        let (label, duration, new_expiry, payment_token, referrer, base) =
            <(
                sol_data::String,
                sol_data::Uint<64>,
                sol_data::Uint<64>,
                sol_data::Address,
                sol_data::FixedBytes<32>,
                sol_data::Uint<256>,
            )>::abi_decode_params_validate(&raw_log.data)
            .context("NameRenewed data is malformed")?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id,
            label,
            duration: i64::try_from(duration).context("NameRenewed duration exceeds i64")?,
            new_expiry: i64::try_from(new_expiry).context("NameRenewed expiry exceeds i64")?,
            payment_token: prefixed_hex_string(payment_token.as_slice()),
            referrer: prefixed_hex_string(referrer.as_slice()),
            base: u256_word_hex(base),
        }));
    }

    Ok(None)
}

fn u256_word_hex(value: U256) -> String {
    prefixed_hex_string(value.to_be_bytes::<32>())
}
