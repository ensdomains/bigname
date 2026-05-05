use alloy_sol_types::sol_data::{Bytes as SolBytes, String as SolString, Uint};
use anyhow::{Context, Result};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::{abi_decode_params, normalize_hex_32, u256_decimal};

use super::{
    constants::*,
    types::{ResolverObservation, ResolverRawLogRow},
};

pub(super) fn build_resolver_observation(
    raw_log: &ResolverRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<ResolverObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if event_topics.matches(ABI_EVENT_ADDRESS_CHANGED_SIGNATURE, topic0)? {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("AddressChanged missing node topic")?,
        )?;
        let (coin_type, address_bytes) = abi_decode_params::<(Uint<256>, SolBytes)>(
            &raw_log.data,
            "AddressChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node,
            coin_type: u256_decimal(coin_type),
            address_bytes: address_bytes.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_TEXT_CHANGED_SIGNATURE, topic0)? {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TextChanged missing node topic")?,
        )?;
        let (key, value) = abi_decode_params::<(SolString, SolString)>(
            &raw_log.data,
            "TextChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::TextChanged { node, key, value }));
    }

    if event_topics.matches(ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE, topic0)? {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ContenthashChanged missing node topic")?,
        )?;
        let (hash,) = abi_decode_params::<(SolBytes,)>(
            &raw_log.data,
            "ContenthashChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::ContenthashChanged {
            node,
            hash: hash.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAME_CHANGED_SIGNATURE, topic0)? {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameChanged missing node topic")?,
        )?;
        let (name,) =
            abi_decode_params::<(SolString,)>(&raw_log.data, "NameChanged data is malformed")?;
        return Ok(Some(ResolverObservation::NameChanged { node, name }));
    }

    if event_topics.matches(ABI_EVENT_VERSION_CHANGED_SIGNATURE, topic0)? {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("VersionChanged missing node topic")?,
        )?;
        let (version,) =
            abi_decode_params::<(Uint<64>,)>(&raw_log.data, "VersionChanged data is malformed")?;
        return Ok(Some(ResolverObservation::VersionChanged {
            node,
            version: i64::try_from(version).context("VersionChanged version exceeds i64")?,
        }));
    }

    if event_topics.matches(ABI_EVENT_ALIAS_CHANGED_SIGNATURE, topic0)? {
        let (from_name, to_name) = abi_decode_params::<(SolBytes, SolBytes)>(
            &raw_log.data,
            "AliasChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name: from_name.to_vec(),
            to_name: to_name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_RESOURCE_SIGNATURE, topic0)? {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedResource {
            name: name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE, topic0)? {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedTextResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedTextResource {
            name: name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE, topic0)? {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedAddrResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedAddrResource {
            name: name.to_vec(),
        }));
    }

    Ok(None)
}
