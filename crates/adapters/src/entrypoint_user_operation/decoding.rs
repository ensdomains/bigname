use alloy_primitives::{I256, U256};
use alloy_sol_types::sol;
use anyhow::Result;

use crate::evm_abi::{address_hex, decode_event_log, hex_string};

sol! {
    // (upstream: .refs/erc4337/contracts/interfaces/IEntryPoint.sol:L29 @ erc4337@7af70c8)
    #[derive(Debug)]
    event UserOperationEvent(
        bytes32 indexed userOpHash,
        address indexed sender,
        address indexed paymaster,
        uint256 nonce,
        bool success,
        uint256 actualGasCost,
        uint256 actualGasUsed
    );

    // (upstream: .refs/chainlink/contracts/src/v0.8/shared/interfaces/AggregatorInterface.sol:L16 @ chainlink@05ead33)
    #[derive(Debug)]
    event AnswerUpdated(
        int256 indexed current,
        uint256 indexed roundId,
        uint256 updatedAt
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UserOperationObservation {
    pub(crate) user_op_hash: String,
    pub(crate) sender: String,
    pub(crate) paymaster: String,
    pub(crate) nonce: U256,
    pub(crate) success: bool,
    pub(crate) actual_gas_cost: U256,
    pub(crate) actual_gas_used: U256,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PriceFeedAnswerObservation {
    pub(crate) answer: I256,
    pub(crate) round_id: U256,
    pub(crate) updated_at: U256,
}

pub(crate) fn decode_user_operation_event(
    topics: &[String],
    data: &[u8],
) -> Result<UserOperationObservation> {
    let event = decode_event_log::<UserOperationEvent>(
        topics,
        data,
        "UserOperationEvent log is malformed",
    )?;
    Ok(UserOperationObservation {
        user_op_hash: hex_string(event.userOpHash.as_slice()),
        sender: address_hex(event.sender),
        paymaster: address_hex(event.paymaster),
        nonce: event.nonce,
        success: event.success,
        actual_gas_cost: event.actualGasCost,
        actual_gas_used: event.actualGasUsed,
    })
}

pub(crate) fn decode_answer_updated_event(
    topics: &[String],
    data: &[u8],
) -> Result<PriceFeedAnswerObservation> {
    let event = decode_event_log::<AnswerUpdated>(topics, data, "AnswerUpdated log is malformed")?;
    Ok(PriceFeedAnswerObservation {
        answer: event.current,
        round_id: event.roundId,
        updated_at: event.updatedAt,
    })
}
