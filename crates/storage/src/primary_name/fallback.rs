//! Shared eligibility domain for route-local primary-name fallback execution.

/// Eligible namespace.
pub const NAMESPACE: &str = crate::resolution_support::ENS_NAMESPACE;

/// Eligible coin type.
pub const COIN_TYPE: &str = "60";

/// Chain that anchors fallback execution and retention.
pub const CHAIN_ID: &str = crate::resolution_support::ETHEREUM_MAINNET_CHAIN_ID;

/// Whether one requested tuple belongs to the fallback domain.
pub fn contains(namespace: &str, coin_type: &str) -> bool {
    namespace == NAMESPACE && coin_type == COIN_TYPE
}
