//! The chain identity an RPC endpoint must report for each admitted chain.
//!
//! The numeric chain id also names the chain of the resolver pointer a stored name summary
//! carries (`ens_v1.resolver`), so this file is an interpreter content hash input
//! (`crates/content-hash/src/compute.rs`). The genesis hashes only gate which endpoints the
//! services accept.

use crate::vocabulary::ChainId;

impl ChainId {
    /// EIP-155 chain id an RPC endpoint for this chain must report from `eth_chainId`, or
    /// `None` for a chain no RPC endpoint serves. The e2e chains run on local nodes started
    /// with the chain id of the production chain they stand in for.
    pub const fn numeric_chain_id(self) -> Option<u64> {
        match self {
            // (upstream: .refs/ens_v1/deployments/base/.chainId:L1 @ ens_v1@91c966f)
            Self::BaseMainnet | Self::BaseE2eComposedReorg => Some(8453),
            // (upstream: .refs/ens_v1/deployments/mainnet/.chain:L1 @ ens_v1@91c966f)
            Self::EthereumMainnet
            | Self::EthereumE2eRpc
            | Self::EthereumE2eReorg
            | Self::EthereumE2eComposedReorg => Some(1),
            // (upstream: .refs/ens_v1/deployments/sepolia/.chain:L1 @ ens_v1@91c966f)
            Self::EthereumSepolia => Some(11_155_111),
            Self::ProjectFixture => None,
        }
    }

    /// Hash of the chain's block 0, when one is pinned. The e2e chains have none because
    /// every local node has its own genesis, and no pinned reference records Base's.
    pub const fn genesis_hash(self) -> Option<&'static str> {
        match self {
            // (upstream: .refs/ens_v1/deployments/mainnet/.chain:L1 @ ens_v1@91c966f)
            Self::EthereumMainnet => {
                Some("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
            }
            // (upstream: .refs/ens_v1/deployments/sepolia/.chain:L1 @ ens_v1@91c966f)
            Self::EthereumSepolia => {
                Some("0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9")
            }
            Self::BaseMainnet
            | Self::BaseE2eComposedReorg
            | Self::EthereumE2eRpc
            | Self::EthereumE2eReorg
            | Self::EthereumE2eComposedReorg
            | Self::ProjectFixture => None,
        }
    }
}
