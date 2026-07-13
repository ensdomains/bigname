use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result};

use crate::evm_abi::{address_hex, hex_string};

sol! {
    // (upstream: .refs/erc7579/src/interfaces/IERC7579Account.sol:L6 @ erc7579@99cbd34)
    #[derive(Debug)]
    struct Execution {
        address target;
        uint256 value;
        bytes callData;
    }

    // (upstream: .refs/erc7579/src/interfaces/IERC7579Account.sol:L26 @ erc7579@99cbd34)
    function execute(bytes32 mode, bytes calldata executionCalldata) external payable;

    // Batch execution calldata is `abi.encode(Execution[])`.
    // (upstream: .refs/erc7579/src/lib/ExecutionLib.sol:L19 @ erc7579@99cbd34)
    function decodeBatchShim(Execution[] calldata executions) external;
}

// (upstream: .refs/erc7579/src/lib/ModeLib.sol:L66 @ erc7579@99cbd34)
const CALL_TYPE_SINGLE: u8 = 0x00;
// (upstream: .refs/erc7579/src/lib/ModeLib.sol:L68 @ erc7579@99cbd34)
const CALL_TYPE_BATCH: u8 = 0x01;

const EVM_ADDRESS_BYTES: usize = 20;
const EVM_WORD_BYTES: usize = 32;
/// Packed single execution: `target (20) ++ value (32) ++ callData`.
/// (upstream: .refs/erc7579/src/lib/ExecutionLib.sol:L64 @ erc7579@99cbd34)
const SINGLE_EXECUTION_HEADER_BYTES: usize = EVM_ADDRESS_BYTES + EVM_WORD_BYTES;

/// One inner call an account execution dispatches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InnerCall {
    pub(crate) target: String,
    pub(crate) data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AccountExecution {
    Calls(Vec<InnerCall>),
    /// ERC-7579 `execute` with a call type this decoder does not attribute
    /// (delegatecall, static, or future types).
    UnsupportedCallType {
        call_type: u8,
    },
    /// The operation's `callData` does not start with the ERC-7579 `execute`
    /// selector (non-7579 account or unsupported wrapper).
    UnrecognizedSelector {
        selector: Option<String>,
    },
}

pub(crate) fn unwrap_account_execution(call_data: &[u8]) -> Result<AccountExecution> {
    let Some(selector) = call_data.get(..4) else {
        return Ok(AccountExecution::UnrecognizedSelector { selector: None });
    };
    if selector != executeCall::SELECTOR {
        return Ok(AccountExecution::UnrecognizedSelector {
            selector: Some(hex_string(selector)),
        });
    }

    let call =
        executeCall::abi_decode_validate(call_data).context("execute calldata is malformed")?;
    let call_type = call.mode.as_slice()[0];
    match call_type {
        CALL_TYPE_SINGLE => decode_single_execution(call.executionCalldata.as_ref())
            .map(|inner_call| AccountExecution::Calls(vec![inner_call])),
        CALL_TYPE_BATCH => {
            decode_batch_execution(call.executionCalldata.as_ref()).map(AccountExecution::Calls)
        }
        call_type => Ok(AccountExecution::UnsupportedCallType { call_type }),
    }
}

fn decode_single_execution(execution_calldata: &[u8]) -> Result<InnerCall> {
    if execution_calldata.len() < SINGLE_EXECUTION_HEADER_BYTES {
        anyhow::bail!(
            "single-mode execution calldata must carry a packed target and value, got {} bytes",
            execution_calldata.len()
        );
    }
    Ok(InnerCall {
        target: hex_string(&execution_calldata[..EVM_ADDRESS_BYTES]),
        data: execution_calldata[SINGLE_EXECUTION_HEADER_BYTES..].to_vec(),
    })
}

fn decode_batch_execution(execution_calldata: &[u8]) -> Result<Vec<InnerCall>> {
    let mut shim_input = decodeBatchShimCall::SELECTOR.to_vec();
    shim_input.extend_from_slice(execution_calldata);
    let call = decodeBatchShimCall::abi_decode_validate(&shim_input)
        .context("batch execution calldata is malformed")?;
    Ok(call
        .executions
        .into_iter()
        .map(|execution| InnerCall {
            target: address_hex(execution.target),
            data: execution.callData.to_vec(),
        })
        .collect())
}
