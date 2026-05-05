use anyhow::{Context, Result};

use crate::evm_abi::{
    dynamic_bytes as decode_dynamic_bytes, dynamic_string as decode_dynamic_string,
    u64_word as decode_u64_word, u256_word_decimal as decode_u256_word_decimal,
};

use super::{
    constants::*,
    types::{ResolverObservation, ResolverRawLogRow},
    util::{keccak_signature_hex, normalize_hex_32},
};

pub(super) fn build_resolver_observation(
    raw_log: &ResolverRawLogRow,
) -> Result<Option<ResolverObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ADDRESS_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("AddressChanged missing node topic")?,
        )?;
        let coin_type = decode_u256_word_decimal(&raw_log.data, 0)?;
        let address_bytes = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node,
            coin_type,
            address_bytes,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TEXT_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TextChanged missing node topic")?,
        )?;
        let key = decode_dynamic_string(&raw_log.data, 0)?;
        let value = decode_dynamic_string(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::TextChanged { node, key, value }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(CONTENTHASH_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ContenthashChanged missing node topic")?,
        )?;
        let hash = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::ContenthashChanged { node, hash }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameChanged missing node topic")?,
        )?;
        let name = decode_dynamic_string(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NameChanged { node, name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(VERSION_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("VersionChanged missing node topic")?,
        )?;
        let version = decode_u64_word(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::VersionChanged { node, version }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ALIAS_CHANGED_SIGNATURE)) {
        let from_name = decode_dynamic_bytes(&raw_log.data, 0)?;
        let to_name = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name,
            to_name,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedTextResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedAddrResource { name }));
    }

    Ok(None)
}
