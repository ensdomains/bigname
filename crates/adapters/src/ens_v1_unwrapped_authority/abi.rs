use super::*;
use crate::evm_abi;
use alloy_sol_types::sol_data::{
    Address as SolAddress, Bytes as SolBytes, FixedBytes, String as SolString, Uint,
};

pub(super) struct RegistrarLabelEventData {
    pub(super) label: String,
    pub(super) expiry: i64,
}

pub(super) struct ResolverAddressChangedData {
    pub(super) coin_type: i64,
    pub(super) address_bytes: Vec<u8>,
}

pub(super) struct TextChangedData {
    pub(super) key: String,
    pub(super) value: Option<String>,
}

pub(super) struct WrapperNameWrappedData {
    pub(super) dns_name: Vec<u8>,
    pub(super) owner: String,
    pub(super) fuses: i64,
    pub(super) expiry: i64,
}

pub(super) struct WrapperTokenTransferData {
    pub(super) namehash: String,
    pub(super) value: i64,
}

pub(super) fn decode_registrar_name_registered_data(
    source_family: &str,
    topic0: &str,
    data: &[u8],
    event_topics: &AuthorityEventTopics,
) -> Result<Option<RegistrarLabelEventData>> {
    if source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 {
        if event_topics.matches(NAME_REGISTERED_SIGNATURE, topic0)? {
            let Ok((label, _cost, expiry)) =
                evm_abi::abi_decode_params::<(SolString, Uint<256>, Uint<256>)>(
                    data,
                    "NameRegistered data is malformed",
                )
            else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label,
                expiry: evm_abi::u256_i64(expiry, "NameRegistered expiry")?,
            }));
        }
        if event_topics.matches(WRAPPED_NAME_REGISTERED_SIGNATURE, topic0)? {
            let Ok((label, _base_cost, _premium, expiry)) =
                evm_abi::abi_decode_params::<(SolString, Uint<256>, Uint<256>, Uint<256>)>(
                    data,
                    "wrapped NameRegistered data is malformed",
                )
            else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label,
                expiry: evm_abi::u256_i64(expiry, "wrapped NameRegistered expiry")?,
            }));
        }
        if event_topics.matches(UNWRAPPED_NAME_REGISTERED_SIGNATURE, topic0)? {
            let Ok((label, _base_cost, _premium, expiry, _referrer)) = evm_abi::abi_decode_params::<
                (SolString, Uint<256>, Uint<256>, Uint<256>, FixedBytes<32>),
            >(
                data,
                "unwrapped NameRegistered data is malformed",
            ) else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label,
                expiry: evm_abi::u256_i64(expiry, "unwrapped NameRegistered expiry")?,
            }));
        }
    }

    if source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        && event_topics.matches(BASENAMES_NAME_REGISTERED_SIGNATURE, topic0)?
    {
        let Ok((label, expiry)) = evm_abi::abi_decode_params::<(SolString, Uint<256>)>(
            data,
            "Basenames NameRegistered data is malformed",
        ) else {
            return Ok(None);
        };
        return Ok(Some(RegistrarLabelEventData {
            label,
            expiry: evm_abi::u256_i64(expiry, "Basenames NameRegistered expiry")?,
        }));
    }

    Ok(None)
}

pub(super) fn decode_registrar_name_renewed_data(
    source_family: &str,
    topic0: &str,
    data: &[u8],
    event_topics: &AuthorityEventTopics,
) -> Result<Option<RegistrarLabelEventData>> {
    if source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 {
        if event_topics.matches(NAME_RENEWED_SIGNATURE, topic0)? {
            let Ok((label, _cost, expiry)) = evm_abi::abi_decode_params::<(
                SolString,
                Uint<256>,
                Uint<256>,
            )>(data, "NameRenewed data is malformed") else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label,
                expiry: evm_abi::u256_i64(expiry, "NameRenewed expiry")?,
            }));
        }
        if event_topics.matches(UNWRAPPED_NAME_RENEWED_SIGNATURE, topic0)? {
            let Ok((label, _cost, expiry, _referrer)) =
                evm_abi::abi_decode_params::<(SolString, Uint<256>, Uint<256>, FixedBytes<32>)>(
                    data,
                    "unwrapped NameRenewed data is malformed",
                )
            else {
                return Ok(None);
            };
            return Ok(Some(RegistrarLabelEventData {
                label,
                expiry: evm_abi::u256_i64(expiry, "unwrapped NameRenewed expiry")?,
            }));
        }
    }

    if source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        && event_topics.matches(BASENAMES_NAME_RENEWED_SIGNATURE, topic0)?
    {
        let Ok((label, expiry)) = evm_abi::abi_decode_params::<(SolString, Uint<256>)>(
            data,
            "Basenames NameRenewed data is malformed",
        ) else {
            return Ok(None);
        };
        return Ok(Some(RegistrarLabelEventData {
            label,
            expiry: evm_abi::u256_i64(expiry, "Basenames NameRenewed expiry")?,
        }));
    }

    Ok(None)
}

pub(super) fn decode_name_changed_data(data: &[u8]) -> Option<String> {
    evm_abi::abi_decode_params::<(SolString,)>(data, "NameChanged data is malformed")
        .map(|(name,)| name)
        .ok()
}

pub(super) fn decode_addr_changed_data(data: &[u8]) -> Option<String> {
    decode_owner_address(data).ok()
}

pub(super) fn decode_address_changed_data(data: &[u8]) -> Option<ResolverAddressChangedData> {
    let (coin_type, address_bytes) = evm_abi::abi_decode_params::<(Uint<256>, SolBytes)>(
        data,
        "AddressChanged data is malformed",
    )
    .ok()?;
    Some(ResolverAddressChangedData {
        coin_type: evm_abi::u256_i64(coin_type, "AddressChanged coin type").ok()?,
        address_bytes: address_bytes.to_vec(),
    })
}

pub(super) fn decode_version_changed_data(data: &[u8]) -> Option<i64> {
    let (version,) =
        evm_abi::abi_decode_params::<(Uint<64>,)>(data, "VersionChanged data is malformed").ok()?;
    i64::try_from(version).ok()
}

pub(super) fn decode_text_changed_data(
    source_family: &str,
    topic0: &str,
    data: &[u8],
    event_topics: &AuthorityEventTopics,
) -> Result<Option<TextChangedData>> {
    if source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        && event_topics.matches(TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE, topic0)?
    {
        let Ok((key,)) =
            evm_abi::abi_decode_params::<(SolString,)>(data, "TextChanged data is malformed")
        else {
            return Ok(None);
        };
        return Ok(Some(TextChangedData { key, value: None }));
    }

    let Ok((key, value)) =
        evm_abi::abi_decode_params::<(SolString, SolString)>(data, "TextChanged data is malformed")
    else {
        return Ok(None);
    };
    Ok(Some(TextChangedData {
        key,
        value: Some(value),
    }))
}

pub(super) fn decode_wrapper_name_wrapped_data(data: &[u8]) -> Result<WrapperNameWrappedData> {
    let (dns_name, owner, fuses, expiry) =
        evm_abi::abi_decode_params::<(SolBytes, SolAddress, Uint<32>, Uint<64>)>(
            data,
            "NameWrapped data is malformed",
        )?;
    Ok(WrapperNameWrappedData {
        dns_name: dns_name.to_vec(),
        owner: evm_abi::address_hex(owner),
        fuses: i64::from(fuses),
        expiry: i64::try_from(expiry).context("NameWrapped expiry exceeds i64")?,
    })
}

pub(super) fn decode_wrapper_fuses_set_data(data: &[u8]) -> Result<i64> {
    let (fuses,) = evm_abi::abi_decode_params::<(Uint<32>,)>(data, "FusesSet data is malformed")?;
    Ok(i64::from(fuses))
}

pub(super) fn decode_wrapper_expiry_extended_data(data: &[u8]) -> Result<i64> {
    let (expiry,) =
        evm_abi::abi_decode_params::<(Uint<64>,)>(data, "ExpiryExtended data is malformed")?;
    i64::try_from(expiry).context("ExpiryExtended expiry exceeds i64")
}

pub(super) fn decode_wrapper_transfer_single_data(data: &[u8]) -> Result<WrapperTokenTransferData> {
    let (id, value) = evm_abi::abi_decode_params::<(Uint<256>, Uint<256>)>(
        data,
        "TransferSingle data is malformed",
    )?;
    Ok(WrapperTokenTransferData {
        namehash: evm_abi::u256_word_hex(id),
        value: evm_abi::u256_i64(value, "TransferSingle value")?,
    })
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    let (address,) =
        evm_abi::abi_decode_params::<(SolAddress,)>(data, "owner address payload is malformed")?;
    Ok(evm_abi::address_hex(address))
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    evm_abi::topic_address_hex(value)
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}
