//! Decode core for the `ens_gas_sponsorship_l1` source family: ERC-4337
//! EntryPoint observations, `handleOps` calldata, ERC-7579 account execution
//! unwrapping, and sponsored-write classification.

mod account_execution;
mod calldata;
mod decoding;
mod write_classifier;

#[cfg(test)]
mod tests;

pub(super) const SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1: &str = "ens_gas_sponsorship_l1";
pub(super) const DERIVATION_KIND_ENTRYPOINT_USER_OPERATION: &str = "entrypoint_user_operation";

pub(super) const EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED: &str =
    "SponsoredUserOperationObserved";
pub(super) const EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED: &str = "SponsoredNameWriteObserved";
pub(super) const EVENT_KIND_PRICE_FEED_ANSWER_UPDATED: &str = "PriceFeedAnswerUpdated";

// (upstream: .refs/erc4337/contracts/interfaces/IEntryPoint.sol:L29 @ erc4337@7af70c8)
pub(super) const ABI_EVENT_USER_OPERATION_EVENT_SIGNATURE: &str =
    "UserOperationEvent(bytes32,address,address,uint256,bool,uint256,uint256)";
// (upstream: .refs/erc4337/contracts/interfaces/IEntryPoint.sol:L97 @ erc4337@7af70c8)
pub(super) const ABI_EVENT_BEFORE_EXECUTION_SIGNATURE: &str = "BeforeExecution()";
// (upstream: .refs/chainlink/contracts/src/v0.8/shared/interfaces/AggregatorInterface.sol:L16 @ chainlink@05ead33)
pub(super) const ABI_EVENT_ANSWER_UPDATED_SIGNATURE: &str = "AnswerUpdated(int256,uint256,uint256)";

pub(super) const CONTRACT_ROLE_ENTRYPOINT: &str = "entrypoint";
pub(super) const CONTRACT_ROLE_SPONSORING_PAYMASTER: &str = "sponsoring_paymaster";
pub(super) const CONTRACT_ROLE_ETH_USD_FEED: &str = "eth_usd_feed";
