//! Schema-v2 live name lookup with divergence-only persistence.

use bigname_domain::vocabulary::{ChainId, Namespace, SourceFamily};

mod abi;
mod call;
mod ccip;
mod engine;
mod error;
mod json_rpc_envelope;
mod primary_name;
mod record_selector;
mod reverse_names;
mod rpc;
mod store;
mod text_records;
mod types;

pub use engine::LookupEngine;
pub use error::{ErrorKind, LookupError, LookupRefusal, Result};
pub use primary_name::{EnsPrimaryNameLookup, EnsPrimaryNameStatus};
pub use record_selector::RecordSelector;
pub use reverse_names::{
    EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest, EnsReverseNameMulticallResult,
    execute_ens_reverse_name_multicall,
};
pub use rpc::{ChainRpcUrls, fetch_network_head_block_number};
pub use store::{
    VerifiedExecutionEntrypoint, admitted_verified_authority_arms, verified_execution_entrypoint,
};
pub use text_records::{
    EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest, EnsTextRecordMulticallResult,
    MULTICALL3_ADDRESS, ens_namehash_hex, execute_ens_text_record_multicall,
};
pub use types::{
    LedgerAction, LookupPosition, LookupRecordResult, LookupRecordStatus, LookupRequest,
    LookupResponse,
};

pub const ENS_NAMESPACE: &str = Namespace::Ens.as_str();
pub const BASENAMES_NAMESPACE: &str = Namespace::Basenames.as_str();
pub const BASE_MAINNET_CHAIN_ID: &str = ChainId::BaseMainnet.as_str();
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = ChainId::EthereumMainnet.as_str();
pub const ETHEREUM_SEPOLIA_CHAIN_ID: &str = ChainId::EthereumSepolia.as_str();
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = SourceFamily::EnsExecution.as_str();
pub const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = SourceFamily::EnsV1RegistryL1.as_str();
pub const BASENAMES_EXECUTION_SOURCE_FAMILY: &str = SourceFamily::BasenamesExecution.as_str();
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const BASENAMES_L1_RESOLVER_ROLE: &str = "l1_resolver";
pub const ENS_REGISTRY_ROLE: &str = "registry";

/// The Ethereum L1 chains an ENS deployment profile can execute against: Mainnet for the
/// `mainnet` profile and Sepolia for the `sepolia` profile. Verified records and verified
/// primary-name lookup run on exactly the chain the profile projects, with the same safety rules
/// on both; any other chain has no ENS execution entrypoint.
pub fn ens_l1_chain(chain_id: &str) -> Option<ChainId> {
    match chain_id {
        ETHEREUM_MAINNET_CHAIN_ID => Some(ChainId::EthereumMainnet),
        ETHEREUM_SEPOLIA_CHAIN_ID => Some(ChainId::EthereumSepolia),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
