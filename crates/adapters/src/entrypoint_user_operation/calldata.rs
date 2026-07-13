use alloy_primitives::U256;
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result};

use crate::evm_abi::{address_hex, hex_string};

sol! {
    // (upstream: .refs/erc4337/contracts/interfaces/PackedUserOperation.sol:L18 @ erc4337@7af70c8)
    #[derive(Debug)]
    struct PackedUserOperation {
        address sender;
        uint256 nonce;
        bytes initCode;
        bytes callData;
        bytes32 accountGasLimits;
        uint256 preVerificationGas;
        bytes32 gasFees;
        bytes paymasterAndData;
        bytes signature;
    }

    // (upstream: .refs/erc4337/contracts/interfaces/IEntryPoint.sol:L154 @ erc4337@7af70c8)
    function handleOps(PackedUserOperation[] calldata ops, address payable beneficiary) external;
}

const EVM_ADDRESS_BYTES: usize = 20;

/// One user operation recovered from `handleOps` transaction input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedUserOperation {
    pub(crate) sender: String,
    pub(crate) nonce: U256,
    pub(crate) call_data: Vec<u8>,
    /// First 20 bytes of `paymasterAndData`; `None` when the operation
    /// declares no paymaster.
    pub(crate) paymaster: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntryPointCalldata {
    HandleOps(Vec<DecodedUserOperation>),
    /// The transaction called the EntryPoint through a selector this decoder
    /// does not support (e.g. `handleAggregatedOps`); operations in it stay
    /// op-level only.
    UnsupportedSelector {
        selector: Option<String>,
    },
}

pub(crate) fn decode_entry_point_calldata(input: &[u8]) -> Result<EntryPointCalldata> {
    let Some(selector) = input.get(..4) else {
        return Ok(EntryPointCalldata::UnsupportedSelector { selector: None });
    };
    if selector != handleOpsCall::SELECTOR {
        return Ok(EntryPointCalldata::UnsupportedSelector {
            selector: Some(hex_string(selector)),
        });
    }

    let call =
        handleOpsCall::abi_decode_validate(input).context("handleOps calldata is malformed")?;
    let operations = call
        .ops
        .into_iter()
        .map(|operation| DecodedUserOperation {
            sender: address_hex(operation.sender),
            nonce: operation.nonce,
            call_data: operation.callData.to_vec(),
            paymaster: paymaster_address(&operation.paymasterAndData),
        })
        .collect();
    Ok(EntryPointCalldata::HandleOps(operations))
}

/// Match a `UserOperationEvent` back to its operation struct inside the
/// decoded bundle. `(sender, nonce)` is unique within one transaction because
/// each executed operation consumes its nonce; the paymaster field is a
/// cross-check against `paymasterAndData`.
pub(crate) fn find_user_operation<'ops>(
    operations: &'ops [DecodedUserOperation],
    sender: &str,
    nonce: U256,
    paymaster: &str,
) -> Option<&'ops DecodedUserOperation> {
    operations.iter().find(|operation| {
        operation.sender.eq_ignore_ascii_case(sender)
            && operation.nonce == nonce
            && operation
                .paymaster
                .as_deref()
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(paymaster))
    })
}

fn paymaster_address(paymaster_and_data: &[u8]) -> Option<String> {
    if paymaster_and_data.len() < EVM_ADDRESS_BYTES {
        return None;
    }
    Some(hex_string(&paymaster_and_data[..EVM_ADDRESS_BYTES]))
}
