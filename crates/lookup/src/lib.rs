//! Schema-v2 live name lookup with divergence-only persistence.

mod abi;
mod call;
mod ccip;
mod engine;
mod error;
mod primary_name;
mod reverse_names;
mod rpc;
mod store;
mod text_records;
mod types;

pub use engine::LookupEngine;
pub use error::{ErrorKind, LookupError, Result};
pub use primary_name::{EnsPrimaryNameLookup, EnsPrimaryNameStatus};
pub use reverse_names::{
    EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest, EnsReverseNameMulticallResult,
    execute_ens_reverse_name_multicall,
};
pub use rpc::{ChainRpcUrls, fetch_network_head_block_number};
pub use text_records::{
    EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest, EnsTextRecordMulticallResult,
    MULTICALL3_ADDRESS, ens_namehash_hex, execute_ens_text_record_multicall,
};
pub use types::{
    LedgerAction, LookupPosition, LookupRecordResult, LookupRecordStatus, LookupRequest,
    LookupResponse, RecordSelector,
};

pub const ENS_NAMESPACE: &str = "ens";
pub const BASENAMES_NAMESPACE: &str = "basenames";
pub const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = "ens_execution";
pub const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
pub const BASENAMES_EXECUTION_SOURCE_FAMILY: &str = "basenames_execution";
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const BASENAMES_L1_RESOLVER_ROLE: &str = "l1_resolver";
pub const ENS_REGISTRY_ROLE: &str = "registry";

#[cfg(test)]
mod tests;
