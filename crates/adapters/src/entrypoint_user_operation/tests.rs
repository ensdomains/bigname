use alloy_primitives::{Address, B256, I256, U256, hex};
use alloy_sol_types::{SolCall, SolEvent};

use crate::evm_abi::hex_string;

use super::account_execution::{AccountExecution, InnerCall, unwrap_account_execution};
use super::calldata::{EntryPointCalldata, decode_entry_point_calldata, find_user_operation};
use super::decoding::{decode_answer_updated_event, decode_user_operation_event};
use super::write_classifier::{WriteKind, classify_inner_calls};

// Golden vectors produced with `cast` (foundry) against the pinned upstream
// shapes, so the decoder is checked against an independent encoder.
const ALICE_ETH_NAMEHASH: &str =
    "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";

// cast calldata "setText(bytes32,string,string)" <node> "url" "https://example.com"
const SET_TEXT_CALLDATA: &str = concat!(
    "0x10f13a8c787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
    "0000000000000000000000000000000000000000000000000000000000000060",
    "00000000000000000000000000000000000000000000000000000000000000a0",
    "0000000000000000000000000000000000000000000000000000000000000003",
    "75726c0000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000013",
    "68747470733a2f2f6578616d706c652e636f6d00000000000000000000000000",
);

// cast calldata "multicall(bytes[])" "[<setText>,<setAddr>]"
const MULTICALL_CALLDATA: &str = concat!(
    "0xac9650d8",
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000002",
    "0000000000000000000000000000000000000000000000000000000000000040",
    "0000000000000000000000000000000000000000000000000000000000000160",
    "00000000000000000000000000000000000000000000000000000000000000e4",
    "10f13a8c787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
    "0000000000000000000000000000000000000000000000000000000000000060",
    "00000000000000000000000000000000000000000000000000000000000000a0",
    "0000000000000000000000000000000000000000000000000000000000000003",
    "75726c0000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000013",
    "68747470733a2f2f6578616d706c652e636f6d00000000000000000000000000",
    "00000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000044",
    "d5fa2b00787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
    "00000000000000000000000000000000000000000000000000000000000000a1",
    "00000000000000000000000000000000000000000000000000000000",
);

// cast calldata "setName(string)" "alice.eth"
const REVERSE_SET_NAME_CALLDATA: &str = concat!(
    "0xc47f0027",
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000009",
    "616c6963652e6574680000000000000000000000000000000000000000000000",
);

const RESOLVER_TARGET: &str = "0x00000000000000000000000000000000000000e5";
const REVERSE_REGISTRAR_TARGET: &str = "0x0000000000000000000000000000000000000e5e";
const PAYMASTER: &str = "0x000000000000000000000000000000000000aaaa";

fn decode_hex(value: &str) -> Vec<u8> {
    hex::decode(value).expect("fixture hex decodes")
}

// cast calldata "execute(bytes32,bytes)" <single mode> <packed target++value++multicall>
fn exec_single_calldata() -> Vec<u8> {
    let mut packed = decode_hex(RESOLVER_TARGET);
    packed.extend_from_slice(&[0u8; 32]);
    packed.extend_from_slice(&decode_hex(MULTICALL_CALLDATA));
    super::account_execution::executeCall {
        mode: B256::ZERO,
        executionCalldata: packed.into(),
    }
    .abi_encode()
}

fn exec_batch_calldata() -> Vec<u8> {
    let executions = vec![
        super::account_execution::Execution {
            target: address(RESOLVER_TARGET),
            value: U256::ZERO,
            callData: decode_hex(MULTICALL_CALLDATA).into(),
        },
        super::account_execution::Execution {
            target: address(REVERSE_REGISTRAR_TARGET),
            value: U256::ZERO,
            callData: decode_hex(REVERSE_SET_NAME_CALLDATA).into(),
        },
    ];
    let encoded_batch =
        super::account_execution::decodeBatchShimCall { executions }.abi_encode()[4..].to_vec();
    let mut mode = [0u8; 32];
    mode[0] = 0x01;
    super::account_execution::executeCall {
        mode: B256::from(mode),
        executionCalldata: encoded_batch.into(),
    }
    .abi_encode()
}

fn address(value: &str) -> Address {
    value.parse().expect("fixture address parses")
}

fn paymaster_and_data() -> Vec<u8> {
    let mut bytes = decode_hex(PAYMASTER);
    bytes.extend_from_slice(&[0u8; 32]);
    bytes
}

fn packed_operation(
    sender: &str,
    nonce: u64,
    call_data: Vec<u8>,
) -> super::calldata::PackedUserOperation {
    super::calldata::PackedUserOperation {
        sender: address(sender),
        nonce: U256::from(nonce),
        initCode: Vec::new().into(),
        callData: call_data.into(),
        accountGasLimits: B256::ZERO,
        preVerificationGas: U256::from(100_000u64),
        gasFees: B256::ZERO,
        paymasterAndData: paymaster_and_data().into(),
        signature: Vec::new().into(),
    }
}

fn handle_ops_calldata() -> Vec<u8> {
    super::calldata::handleOpsCall {
        ops: vec![
            packed_operation(
                "0x0000000000000000000000000000000000005e4d",
                7,
                exec_single_calldata(),
            ),
            packed_operation(
                "0x0000000000000000000000000000000000006e4d",
                3,
                exec_batch_calldata(),
            ),
        ],
        beneficiary: address("0x000000000000000000000000000000000000bbbb"),
    }
    .abi_encode()
}

fn log_parts(log_data: alloy_primitives::LogData) -> (Vec<String>, Vec<u8>) {
    let topics = log_data
        .topics()
        .iter()
        .map(|topic| hex_string(topic.as_slice()))
        .collect();
    (topics, log_data.data.to_vec())
}

#[test]
fn decodes_user_operation_event_log() {
    let event = super::decoding::UserOperationEvent {
        userOpHash: B256::repeat_byte(0x11),
        sender: address("0x0000000000000000000000000000000000005e4d"),
        paymaster: address(PAYMASTER),
        nonce: U256::from(7u64),
        success: false,
        actualGasCost: U256::from(1_234_567u64),
        actualGasUsed: U256::from(90_000u64),
    };
    let (topics, data) = log_parts(event.encode_log_data());

    let observation = decode_user_operation_event(&topics, &data).expect("event decodes");

    assert_eq!(observation.user_op_hash, hex_string([0x11u8; 32]));
    assert_eq!(
        observation.sender,
        "0x0000000000000000000000000000000000005e4d"
    );
    assert_eq!(observation.paymaster, PAYMASTER);
    assert_eq!(observation.nonce, U256::from(7u64));
    assert!(!observation.success);
    assert_eq!(observation.actual_gas_cost, U256::from(1_234_567u64));
    assert_eq!(observation.actual_gas_used, U256::from(90_000u64));
}

#[test]
fn decodes_answer_updated_log_including_negative_answers() {
    let event = super::decoding::AnswerUpdated {
        current: I256::unchecked_from(-42i64),
        roundId: U256::from(555u64),
        updatedAt: U256::from(1_750_000_000u64),
    };
    let (topics, data) = log_parts(event.encode_log_data());

    let observation = decode_answer_updated_event(&topics, &data).expect("event decodes");

    assert_eq!(observation.answer, I256::unchecked_from(-42i64));
    assert_eq!(observation.round_id, U256::from(555u64));
    assert_eq!(observation.updated_at, U256::from(1_750_000_000u64));
}

#[test]
fn decodes_handle_ops_operations_with_paymaster_prefix() {
    let decoded = decode_entry_point_calldata(&handle_ops_calldata()).expect("decodes");

    let EntryPointCalldata::HandleOps(operations) = decoded else {
        panic!("expected handleOps decode, got {decoded:?}");
    };
    assert_eq!(operations.len(), 2);
    assert_eq!(
        operations[0].sender,
        "0x0000000000000000000000000000000000005e4d"
    );
    assert_eq!(operations[0].nonce, U256::from(7u64));
    assert_eq!(operations[0].paymaster.as_deref(), Some(PAYMASTER));
    assert_eq!(operations[0].call_data, exec_single_calldata());
    assert_eq!(operations[1].nonce, U256::from(3u64));
    assert_eq!(operations[1].call_data, exec_batch_calldata());
}

#[test]
fn foreign_entry_point_selector_is_unsupported_not_an_error() {
    let decoded =
        decode_entry_point_calldata(&decode_hex("0xdeadbeef0000")).expect("decode succeeds");
    assert_eq!(
        decoded,
        EntryPointCalldata::UnsupportedSelector {
            selector: Some("0xdeadbeef".to_owned()),
        }
    );

    let empty = decode_entry_point_calldata(&[]).expect("decode succeeds");
    assert_eq!(
        empty,
        EntryPointCalldata::UnsupportedSelector { selector: None }
    );
}

#[test]
fn operation_without_paymaster_bytes_has_no_paymaster() {
    let mut operation =
        packed_operation("0x0000000000000000000000000000000000005e4d", 1, Vec::new());
    operation.paymasterAndData = Vec::new().into();
    let calldata = super::calldata::handleOpsCall {
        ops: vec![operation],
        beneficiary: address("0x000000000000000000000000000000000000bbbb"),
    }
    .abi_encode();

    let EntryPointCalldata::HandleOps(operations) =
        decode_entry_point_calldata(&calldata).expect("decodes")
    else {
        panic!("expected handleOps decode");
    };
    assert_eq!(operations[0].paymaster, None);
}

#[test]
fn correlates_event_to_operation_by_sender_nonce_and_paymaster() {
    let EntryPointCalldata::HandleOps(operations) =
        decode_entry_point_calldata(&handle_ops_calldata()).expect("decodes")
    else {
        panic!("expected handleOps decode");
    };

    let matched = find_user_operation(
        &operations,
        "0x0000000000000000000000000000000000006E4D",
        U256::from(3u64),
        "0x000000000000000000000000000000000000AAAA",
    );
    assert_eq!(
        matched.map(|operation| operation.sender.as_str()),
        Some("0x0000000000000000000000000000000000006e4d")
    );

    let wrong_nonce = find_user_operation(
        &operations,
        "0x0000000000000000000000000000000000006e4d",
        U256::from(4u64),
        PAYMASTER,
    );
    assert!(wrong_nonce.is_none());

    let wrong_paymaster = find_user_operation(
        &operations,
        "0x0000000000000000000000000000000000006e4d",
        U256::from(3u64),
        "0x000000000000000000000000000000000000cccc",
    );
    assert!(wrong_paymaster.is_none());
}

#[test]
fn unwraps_single_mode_execution_to_target_and_calldata() {
    let unwrapped = unwrap_account_execution(&exec_single_calldata()).expect("unwraps");

    assert_eq!(
        unwrapped,
        AccountExecution::Calls(vec![InnerCall {
            target: RESOLVER_TARGET.to_owned(),
            data: decode_hex(MULTICALL_CALLDATA),
        }])
    );
}

#[test]
fn unwraps_batch_mode_execution_to_all_calls() {
    let unwrapped = unwrap_account_execution(&exec_batch_calldata()).expect("unwraps");

    assert_eq!(
        unwrapped,
        AccountExecution::Calls(vec![
            InnerCall {
                target: RESOLVER_TARGET.to_owned(),
                data: decode_hex(MULTICALL_CALLDATA),
            },
            InnerCall {
                target: REVERSE_REGISTRAR_TARGET.to_owned(),
                data: decode_hex(REVERSE_SET_NAME_CALLDATA),
            },
        ])
    );
}

#[test]
fn delegatecall_mode_is_unsupported_call_type() {
    let mut mode = [0u8; 32];
    mode[0] = 0xff;
    let calldata = super::account_execution::executeCall {
        mode: B256::from(mode),
        executionCalldata: Vec::new().into(),
    }
    .abi_encode();

    let unwrapped = unwrap_account_execution(&calldata).expect("unwraps");
    assert_eq!(
        unwrapped,
        AccountExecution::UnsupportedCallType { call_type: 0xff }
    );
}

#[test]
fn non_execute_selector_is_unrecognized() {
    let unwrapped = unwrap_account_execution(&decode_hex("0x12345678")).expect("unwraps");
    assert_eq!(
        unwrapped,
        AccountExecution::UnrecognizedSelector {
            selector: Some("0x12345678".to_owned()),
        }
    );
}

#[test]
fn classifies_resolver_multicall_as_one_records_write_per_node() {
    let classified = classify_inner_calls(&[InnerCall {
        target: RESOLVER_TARGET.to_owned(),
        data: decode_hex(MULTICALL_CALLDATA),
    }]);

    assert_eq!(classified.writes.len(), 1);
    let write = &classified.writes[0];
    assert_eq!(write.write_kind, WriteKind::Records);
    assert_eq!(write.node.as_deref(), Some(ALICE_ETH_NAMEHASH));
    assert_eq!(write.name, None);
    assert_eq!(write.target, RESOLVER_TARGET);
    assert_eq!(classified.unrecognized_call_count, 0);
}

#[test]
fn classifies_reverse_set_name_with_namehash_matching_cast() {
    let classified = classify_inner_calls(&[InnerCall {
        target: REVERSE_REGISTRAR_TARGET.to_owned(),
        data: decode_hex(REVERSE_SET_NAME_CALLDATA),
    }]);

    assert_eq!(classified.writes.len(), 1);
    let write = &classified.writes[0];
    assert_eq!(write.write_kind, WriteKind::Primary);
    assert_eq!(write.node.as_deref(), Some(ALICE_ETH_NAMEHASH));
    assert_eq!(write.name.as_deref(), Some("alice.eth"));
    assert_eq!(write.source_call, "setName");
}

#[test]
fn primary_claim_normalizes_before_namehash() {
    let calldata = super::write_classifier::setName_1Call {
        name: "Alice.ETH".to_owned(),
    }
    .abi_encode();

    let classified = classify_inner_calls(&[InnerCall {
        target: REVERSE_REGISTRAR_TARGET.to_owned(),
        data: calldata,
    }]);

    assert_eq!(
        classified.writes[0].node.as_deref(),
        Some(ALICE_ETH_NAMEHASH)
    );
    assert_eq!(classified.writes[0].name.as_deref(), Some("Alice.ETH"));
}

#[test]
fn unnormalizable_primary_claim_keeps_name_without_node() {
    let calldata = super::write_classifier::setName_1Call {
        name: "al\u{202e}ice.eth".to_owned(),
    }
    .abi_encode();

    let classified = classify_inner_calls(&[InnerCall {
        target: REVERSE_REGISTRAR_TARGET.to_owned(),
        data: calldata,
    }]);

    assert_eq!(classified.writes.len(), 1);
    assert_eq!(classified.writes[0].node, None);
    assert_eq!(
        classified.writes[0].name.as_deref(),
        Some("al\u{202e}ice.eth")
    );
}

#[test]
fn empty_primary_claim_and_unknown_selectors_stay_unrecognized() {
    let clear_primary = super::write_classifier::setName_1Call {
        name: String::new(),
    }
    .abi_encode();

    let classified = classify_inner_calls(&[
        InnerCall {
            target: REVERSE_REGISTRAR_TARGET.to_owned(),
            data: clear_primary,
        },
        InnerCall {
            target: RESOLVER_TARGET.to_owned(),
            data: decode_hex("0x12345678aa"),
        },
        InnerCall {
            target: RESOLVER_TARGET.to_owned(),
            data: Vec::new(),
        },
    ]);

    assert!(classified.writes.is_empty());
    assert_eq!(classified.unrecognized_call_count, 3);
}

#[test]
fn multicall_recursion_depth_is_capped() {
    let mut nested = decode_hex(SET_TEXT_CALLDATA);
    for _ in 0..5 {
        nested = super::write_classifier::multicallCall {
            data: vec![nested.into()],
        }
        .abi_encode();
    }

    let classified = classify_inner_calls(&[InnerCall {
        target: RESOLVER_TARGET.to_owned(),
        data: nested,
    }]);

    assert!(classified.writes.is_empty());
    assert_eq!(classified.unrecognized_call_count, 1);
}

#[test]
fn end_to_end_bundle_attributes_both_operations() {
    let EntryPointCalldata::HandleOps(operations) =
        decode_entry_point_calldata(&handle_ops_calldata()).expect("decodes")
    else {
        panic!("expected handleOps decode");
    };

    let single_op = find_user_operation(
        &operations,
        "0x0000000000000000000000000000000000005e4d",
        U256::from(7u64),
        PAYMASTER,
    )
    .expect("single-mode operation correlates");
    let AccountExecution::Calls(single_calls) =
        unwrap_account_execution(&single_op.call_data).expect("unwraps")
    else {
        panic!("expected calls");
    };
    let single_writes = classify_inner_calls(&single_calls);
    assert_eq!(single_writes.writes.len(), 1);
    assert_eq!(single_writes.writes[0].write_kind, WriteKind::Records);

    let batch_op = find_user_operation(
        &operations,
        "0x0000000000000000000000000000000000006e4d",
        U256::from(3u64),
        PAYMASTER,
    )
    .expect("batch-mode operation correlates");
    let AccountExecution::Calls(batch_calls) =
        unwrap_account_execution(&batch_op.call_data).expect("unwraps")
    else {
        panic!("expected calls");
    };
    let batch_writes = classify_inner_calls(&batch_calls);
    assert_eq!(batch_writes.writes.len(), 2);
    assert_eq!(
        batch_writes
            .writes
            .iter()
            .map(|write| (write.write_kind, write.node.as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (WriteKind::Records, Some(ALICE_ETH_NAMEHASH)),
            (WriteKind::Primary, Some(ALICE_ETH_NAMEHASH)),
        ]
    );
}
