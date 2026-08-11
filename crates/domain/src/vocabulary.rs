//! Shared closed vocabularies used by resolution projection and lookup.

use std::{fmt, str::FromStr};

use alloy_primitives::{Address, hex};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

macro_rules! string_vocabulary {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
        kind = $kind:literal;
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = VocabularyParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant)),+,
                    _ => Err(VocabularyParseError::new($kind, value)),
                }
            }
        }
    };
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VocabularyParseError {
    kind: &'static str,
    value: String,
}

impl VocabularyParseError {
    fn new(kind: &'static str, value: &str) -> Self {
        Self {
            kind,
            value: value.to_owned(),
        }
    }
}

impl fmt::Display for VocabularyParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown {} {}", self.kind, self.value)
    }
}

impl std::error::Error for VocabularyParseError {}

string_vocabulary! {
    /// Chains admitted by production manifests or fixed topology test deployments.
    pub enum ChainId {
        BaseMainnet => "base-mainnet",
        BaseE2eComposedReorg => "base-e2e-composed-reorg",
        EthereumMainnet => "ethereum-mainnet",
        EthereumSepolia => "ethereum-sepolia",
        EthereumE2eRpc => "ethereum-e2e-rpc",
        EthereumE2eReorg => "ethereum-e2e-reorg",
        EthereumE2eComposedReorg => "ethereum-e2e-composed-reorg",
        ProjectFixture => "project-fixture",
    }
    kind = "chain id";
}

string_vocabulary! {
    /// Public name-system namespaces admitted by bigname.
    pub enum Namespace {
        Ens => "ens",
        Basenames => "basenames",
    }
    kind = "namespace";
}

string_vocabulary! {
    /// [Source families](../../../docs/glossary.md#source-family) admitted by the checked-in
    /// deployment manifests.
    pub enum SourceFamily {
        BasenamesBasePrimary => "basenames_base_primary",
        BasenamesBaseRegistrar => "basenames_base_registrar",
        BasenamesBaseRegistry => "basenames_base_registry",
        BasenamesBaseResolver => "basenames_base_resolver",
        BasenamesExecution => "basenames_execution",
        BasenamesL1Compat => "basenames_l1_compat",
        EnsExecution => "ens_execution",
        EnsV1RegistrarL1 => "ens_v1_registrar_l1",
        EnsV1RegistryL1 => "ens_v1_registry_l1",
        EnsV1ResolverL1 => "ens_v1_resolver_l1",
        EnsV1ReverseL1 => "ens_v1_reverse_l1",
        EnsV1WrapperL1 => "ens_v1_wrapper_l1",
        EnsV2RegistrarL1 => "ens_v2_registrar_l1",
        EnsV2RegistryL1 => "ens_v2_registry_l1",
        EnsV2ResolverL1 => "ens_v2_resolver_l1",
        EnsV2RootL1 => "ens_v2_root_l1",
    }
    kind = "source family";
}

/// A strict 20-byte, `0x`-prefixed EVM address.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EvmAddress(Address);

impl EvmAddress {
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(Address::new(bytes))
    }

    pub const fn as_alloy(self) -> Address {
        self.0
    }

    pub fn to_canonical_string(self) -> String {
        format!("0x{}", hex::encode(self.0.as_slice()))
    }
}

impl fmt::Display for EvmAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_canonical_string())
    }
}

impl FromStr for EvmAddress {
    type Err = EvmAddressParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 42 || (!value.starts_with("0x") && !value.starts_with("0X")) {
            return Err(EvmAddressParseError(value.to_owned()));
        }
        value
            .parse::<Address>()
            .map(Self)
            .map_err(|_| EvmAddressParseError(value.to_owned()))
    }
}

impl Serialize for EvmAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_canonical_string())
    }
}

impl<'de> Deserialize<'de> for EvmAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmAddressParseError(String);

impl fmt::Display for EvmAddressParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid 20-byte EVM address {}", self.0)
    }
}

impl std::error::Error for EvmAddressParseError {}

/// Parse the broader address grammar accepted by Alloy, including an unprefixed
/// 40-digit hexadecimal address.
pub fn parse_alloy_evm_address(value: &str) -> Result<EvmAddress, EvmAddressParseError> {
    value
        .parse::<Address>()
        .map(EvmAddress)
        .map_err(|_| EvmAddressParseError(value.to_owned()))
}

fn canonicalize_prefixed_evm_address(value: &str) -> Option<String> {
    value
        .parse::<EvmAddress>()
        .ok()
        .map(EvmAddress::to_canonical_string)
}

/// Preserve the historical storage policy: canonicalize prefixed addresses and lowercase
/// sentinels.
pub fn canonicalize_prefixed_evm_address_or_ascii_lowercase(value: &str) -> String {
    canonicalize_prefixed_evm_address(value).unwrap_or_else(|| value.to_ascii_lowercase())
}
