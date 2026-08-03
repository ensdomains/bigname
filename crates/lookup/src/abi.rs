use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};
use anyhow::{Context, Result, bail};
use bigname_domain::normalization::normalize_name;

use crate::RecordSelector;

/// Result ABI for the manifest-selected execution entrypoint. ENS returns
/// `(bytes,address)` (upstream: .refs/ens_v1/contracts/universalResolver/IUniversalResolver.sol:L55 @ ens_v1@91c966f),
/// while the Basenames L1 resolver and its callback return one `bytes` value
/// (upstream: .refs/basenames/src/L1/L1Resolver.sol:L164 @ basenames@1809bbc)
/// (upstream: .refs/basenames/src/L1/L1Resolver.sol:L191 @ basenames@1809bbc).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionResultAbi {
    EnsUniversalResolver,
    BasenamesL1Resolver,
}

mod contracts {
    use super::*;

    sol! {
        function resolve(bytes name, bytes data) external view returns (bytes result, address resolver);
        function addr(bytes32 node) external view returns (address);
        function addr(bytes32 node, uint256 coin_type) external view returns (bytes);
        function text(bytes32 node, string key) external view returns (string);
        function contenthash(bytes32 node) external view returns (bytes);
        function resolver(bytes32 node) external view returns (address);
        function name(bytes32 node) external view returns (string);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedCall {
    selector: [u8; 4],
    calldata: Vec<u8>,
}

impl EncodedCall {
    #[cfg(test)]
    pub(crate) fn selector_hex(&self) -> String {
        hex_string(&self.selector)
    }

    pub(crate) fn calldata(&self) -> &[u8] {
        &self.calldata
    }

    pub(crate) fn calldata_hex(&self) -> String {
        hex_string(&self.calldata)
    }
}

pub(crate) fn dns_encode_name(name: &str) -> Result<Vec<u8>> {
    if name.is_empty() {
        return Ok(vec![0]);
    }
    normalize_name(name)
        .map(|normalized| normalized.dns_encoded_name)
        .map_err(anyhow::Error::from)
}

pub(crate) fn namehash(name: &str) -> Result<[u8; 32]> {
    let mut node = [0_u8; 32];
    if name.is_empty() {
        return Ok(node);
    }
    let normalized = normalize_name(name).map_err(anyhow::Error::from)?;
    for label in normalized.normalized_labels.iter().rev() {
        let mut combined = [0_u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        node.copy_from_slice(keccak256(combined).as_slice());
    }
    Ok(node)
}

pub(crate) fn parse_node(value: &str) -> Result<[u8; 32]> {
    let bytes = hex_to_bytes(value)?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .with_context(|| format!("namehash {value} must contain exactly 32 bytes"))
}

pub(crate) fn resolver_record_call(record: &RecordSelector, node: [u8; 32]) -> Result<EncodedCall> {
    match record.record_family.as_str() {
        "addr" if record.selector_key.as_deref() == Some("60") => {
            Ok(encoded_call(contracts::addr_0Call {
                node: B256::from(node),
            }))
        }
        "addr" => {
            let coin_type = record
                .selector_key
                .as_deref()
                .context("address record selector is missing coin type")?
                .parse::<u64>()
                .with_context(|| format!("{} has invalid coin type", record.record_key))?;
            Ok(encoded_call(contracts::addr_1Call {
                node: B256::from(node),
                coin_type: U256::from(coin_type),
            }))
        }
        "text" => text_call(
            node,
            record
                .selector_key
                .as_deref()
                .context("text record selector is missing its key")?,
        ),
        "avatar" => text_call(node, "avatar"),
        "contenthash" => Ok(encoded_call(contracts::contenthashCall {
            node: B256::from(node),
        })),
        family => bail!("unsupported resolver record family {family}"),
    }
}

pub(crate) fn universal_resolver_call(dns_name: &[u8], resolver_data: &[u8]) -> EncodedCall {
    encoded_call(contracts::resolveCall {
        name: Bytes::copy_from_slice(dns_name),
        data: Bytes::copy_from_slice(resolver_data),
    })
}

pub(crate) fn registry_resolver_call(node: [u8; 32]) -> EncodedCall {
    encoded_call(contracts::resolverCall {
        node: B256::from(node),
    })
}

pub(crate) fn resolver_name_call(node: [u8; 32]) -> EncodedCall {
    encoded_call(contracts::nameCall {
        node: B256::from(node),
    })
}

pub(crate) fn decode_registry_resolver(return_data: &[u8]) -> Result<Option<String>> {
    let address = contracts::resolverCall::abi_decode_returns_validate(return_data)
        .context("registry resolver return data is malformed")?;
    Ok((address != Address::ZERO).then(|| hex_string(address.as_slice())))
}

pub(crate) fn decode_resolver_name(return_data: &[u8]) -> Result<Option<String>> {
    let name = contracts::nameCall::abi_decode_returns_validate(return_data)
        .context("resolver name return data is malformed")?;
    Ok((!name.is_empty()).then_some(name))
}

pub(crate) fn decode_resolution_result(
    result_abi: ResolutionResultAbi,
    return_data: &[u8],
) -> Result<Vec<u8>> {
    match result_abi {
        ResolutionResultAbi::EnsUniversalResolver => Ok(
            contracts::resolveCall::abi_decode_returns_validate(return_data)
                .context("Universal Resolver return data is malformed")?
                .result
                .to_vec(),
        ),
        ResolutionResultAbi::BasenamesL1Resolver => {
            let (result,) = <(Bytes,)>::abi_decode_params_validate(return_data)
                .context("Basenames L1Resolver return data is malformed")?;
            Ok(result.to_vec())
        }
    }
}

pub(crate) fn decode_record_result(
    record: &RecordSelector,
    return_data: &[u8],
) -> Result<Option<String>> {
    match record.record_family.as_str() {
        "addr" if record.selector_key.as_deref() == Some("60") => {
            let address = contracts::addr_0Call::abi_decode_returns_validate(return_data)
                .context("addr(bytes32) return data is malformed")?;
            Ok((address != Address::ZERO).then(|| hex_string(address.as_slice())))
        }
        "addr" => {
            let bytes = contracts::addr_1Call::abi_decode_returns_validate(return_data)
                .context("addr(bytes32,uint256) return data is malformed")?;
            Ok((!bytes.is_empty()).then(|| hex_string(&bytes)))
        }
        "text" | "avatar" => {
            let text = contracts::textCall::abi_decode_returns_validate(return_data)
                .context("text(bytes32,string) return data is malformed")?;
            Ok((!text.is_empty()).then_some(text))
        }
        "contenthash" => {
            let bytes = contracts::contenthashCall::abi_decode_returns_validate(return_data)
                .context("contenthash(bytes32) return data is malformed")?;
            Ok((!bytes.is_empty()).then(|| hex_string(&bytes)))
        }
        family => bail!("unsupported resolver record family {family}"),
    }
}

pub(crate) fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let payload = value
        .strip_prefix("0x")
        .context("hex value must start with 0x")?;
    if payload.len() % 2 != 0 {
        bail!("hex value must contain an even number of digits");
    }
    hex::decode(payload).context("hex value contains non-hex digits")
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn encoded_call<T: SolCall>(call: T) -> EncodedCall {
    EncodedCall {
        selector: T::SELECTOR,
        calldata: call.abi_encode(),
    }
}

fn text_call(node: [u8; 32], key: &str) -> Result<EncodedCall> {
    if key.is_empty() {
        bail!("text record key must not be empty");
    }
    Ok(encoded_call(contracts::textCall {
        node: B256::from(node),
        key: key.to_owned(),
    }))
}

#[cfg(test)]
mod tests {
    use alloy_sol_types::SolValue;

    use super::*;

    #[test]
    fn portable_name_and_selector_vectors_match_legacy_execution() -> Result<()> {
        assert_eq!(dns_encode_name("Alice.eth")?, b"\x05alice\x03eth\0");
        assert_eq!(
            hex_string(&namehash("eth")?),
            "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
        );
        assert_eq!(
            resolver_record_call(&RecordSelector::parse("addr:60")?, [0; 32])?.selector_hex(),
            "0x3b3b57de"
        );
        assert_eq!(
            resolver_record_call(&RecordSelector::parse("text:url")?, [0; 32])?.selector_hex(),
            "0x59d1d43c"
        );
        Ok(())
    }

    #[test]
    fn portable_dns_normalization_and_universal_selector_match_legacy_execution() -> Result<()> {
        assert_eq!(dns_encode_name("")?, vec![0]);
        assert!(dns_encode_name(".alice.eth").is_err());
        assert!(namehash("alice.eth.").is_err());
        assert_eq!(
            universal_resolver_call(b"\x05alice\x03eth\0", &[0x12, 0x34]).selector_hex(),
            "0x9061b923"
        );
        Ok(())
    }

    #[test]
    fn execution_entrypoints_decode_their_declared_result_abis() -> Result<()> {
        let record_result = Bytes::from(vec![0x12, 0x34]);
        let ens_result = (record_result.clone(), Address::ZERO).abi_encode_params();
        let basenames_result = (record_result.clone(),).abi_encode_params();

        assert_eq!(
            decode_resolution_result(ResolutionResultAbi::EnsUniversalResolver, &ens_result)?,
            record_result
        );
        assert_eq!(
            decode_resolution_result(ResolutionResultAbi::BasenamesL1Resolver, &basenames_result)?,
            record_result
        );
        Ok(())
    }

    #[test]
    fn portable_resolver_selectors_and_zero_address_decode_match_legacy_execution() -> Result<()> {
        assert_eq!(
            resolver_record_call(&RecordSelector::parse("addr:0")?, [0; 32])?.selector_hex(),
            "0xf1cb7e06"
        );
        assert_eq!(
            resolver_record_call(&RecordSelector::parse("contenthash")?, [0; 32])?.selector_hex(),
            "0xbc1c58d1"
        );
        let zero = (Address::ZERO,).abi_encode_params();
        assert_eq!(
            decode_record_result(&RecordSelector::parse("addr:60")?, &zero)?,
            None
        );
        Ok(())
    }
}
