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

// --- database-backed integration: manifest fixture -> raw facts -> sync ---

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bigname_manifests::{load_repository, sync_repository};
use bigname_storage::{
    CanonicalityState, RawBlock, RawLog, RawTransactionInput, default_database_url,
    load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
    upsert_raw_transaction_inputs,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

use super::{EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED, sync_entrypoint_user_operation};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const TEST_CHAIN: &str = "ethereum-sepolia";
const ENTRYPOINT_ADDRESS: &str = "0x0000000071727de22e5e9d8baf0edac6f37da032";
const FEED_ADDRESS: &str = "0x719e22e3d4b690e5d96ccb40619180b5427f14ae";
const BLOCK_HASH: &str = "0x00000000000000000000000000000000000000000000000000000000000b10c1";

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for entrypoint_user_operation tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bg_ep_uo_{}_{unique:x}_{sequence:x}", std::process::id());

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for entrypoint_user_operation tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect entrypoint_user_operation test pool")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for entrypoint_user_operation tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

/// Write an ACTIVE gas-sponsorship manifest fixture in the real profile-root
/// layout so the test exercises the declarative loader end to end. The
/// checked-in manifest stays shadow until the family's runtime wiring lands.
fn write_fixture_manifest_root() -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "bigname-ep-uo-manifests-{}-{unique:x}",
        std::process::id()
    ));
    let family_dir = root.join("ethereum/ens/ens_gas_sponsorship_l1");
    std::fs::create_dir_all(&family_dir).context("failed to create fixture manifest dir")?;
    let manifest = format!(
        r#"manifest_version = 1
namespace = "ens"
source_family = "ens_gas_sponsorship_l1"
chain = "{TEST_CHAIN}"
deployment_epoch = "ens_v2_sepolia_dev"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]

[[contracts]]
role = "entrypoint"
address = "{ENTRYPOINT_ADDRESS}"
proxy_kind = "none"

[[contracts]]
role = "sponsoring_paymaster"
address = "{PAYMASTER}"
proxy_kind = "none"

[[contracts]]
role = "eth_usd_feed"
address = "{FEED_ADDRESS}"
proxy_kind = "none"

[[abi.events]]
name = "UserOperationEvent"
fragment = "event UserOperationEvent(bytes32 indexed userOpHash, address indexed sender, address indexed paymaster, uint256 nonce, bool success, uint256 actualGasCost, uint256 actualGasUsed)"
emitter_roles = ["entrypoint"]
normalized_events = ["SponsoredUserOperationObserved", "SponsoredNameWriteObserved"]

[[abi.events]]
name = "BeforeExecution"
fragment = "event BeforeExecution()"
emitter_roles = ["entrypoint"]

[[abi.events]]
name = "AnswerUpdated"
fragment = "event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)"
emitter_roles = ["eth_usd_feed"]
normalized_events = ["PriceFeedAnswerUpdated"]
"#
    );
    std::fs::write(family_dir.join("v1.toml"), manifest)
        .context("failed to write fixture manifest")?;
    Ok(root)
}

fn raw_block(block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: TEST_CHAIN.to_owned(),
        block_hash: BLOCK_HASH.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_752_000_000)
            .expect("test timestamp is valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_log_row(
    emitting_address: &str,
    transaction_hash: &str,
    log_index: i64,
    log_data: alloy_primitives::LogData,
) -> RawLog {
    let (topics, data) = log_parts(log_data);
    RawLog {
        chain_id: TEST_CHAIN.to_owned(),
        block_hash: BLOCK_HASH.to_owned(),
        block_number: 5_400_000,
        transaction_hash: transaction_hash.to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn user_operation_log_data(
    sender: &str,
    paymaster: &str,
    nonce: u64,
    success: bool,
) -> alloy_primitives::LogData {
    super::decoding::UserOperationEvent {
        userOpHash: B256::repeat_byte(nonce as u8),
        sender: address(sender),
        paymaster: address(paymaster),
        nonce: U256::from(nonce),
        success,
        actualGasCost: U256::from(1_000_000_000_000_000u64),
        actualGasUsed: U256::from(150_000u64),
    }
    .encode_log_data()
}

#[tokio::test]
async fn syncs_sponsored_operations_price_updates_and_attribution() -> Result<()> {
    let database = TestDatabase::new().await?;
    let manifest_root = write_fixture_manifest_root()?;
    let repository = load_repository(&manifest_root)?;
    sync_repository(database.pool(), &repository).await?;

    upsert_raw_blocks(database.pool(), &[raw_block(5_400_000)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            // Two sponsored operations in one bundle transaction.
            raw_log_row(
                ENTRYPOINT_ADDRESS,
                "0xbundle1",
                2,
                user_operation_log_data(
                    "0x0000000000000000000000000000000000005e4d",
                    PAYMASTER,
                    7,
                    true,
                ),
            ),
            raw_log_row(
                ENTRYPOINT_ADDRESS,
                "0xbundle1",
                5,
                user_operation_log_data(
                    "0x0000000000000000000000000000000000006e4d",
                    PAYMASTER,
                    3,
                    false,
                ),
            ),
            // Foreign paymaster: filtered out entirely.
            raw_log_row(
                ENTRYPOINT_ADDRESS,
                "0xbundle2",
                7,
                user_operation_log_data(
                    "0x0000000000000000000000000000000000007e4d",
                    "0x000000000000000000000000000000000000cccc",
                    9,
                    true,
                ),
            ),
            // Sponsored operation whose transaction input was never retained.
            raw_log_row(
                ENTRYPOINT_ADDRESS,
                "0xbundle3",
                9,
                user_operation_log_data(
                    "0x0000000000000000000000000000000000008e4d",
                    PAYMASTER,
                    11,
                    true,
                ),
            ),
            // Price feed answer.
            raw_log_row(
                FEED_ADDRESS,
                "0xfeedtx",
                12,
                super::decoding::AnswerUpdated {
                    current: I256::unchecked_from(250_000_000_000i64),
                    roundId: U256::from(42u64),
                    updatedAt: U256::from(1_752_000_000u64),
                }
                .encode_log_data(),
            ),
        ],
    )
    .await?;
    upsert_raw_transaction_inputs(
        database.pool(),
        &[RawTransactionInput {
            chain_id: TEST_CHAIN.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            block_number: 5_400_000,
            transaction_hash: "0xbundle1".to_owned(),
            input: handle_ops_calldata(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let summary = sync_entrypoint_user_operation(database.pool(), TEST_CHAIN).await?;
    // 3 sponsored logs (foreign paymaster excluded) + 1 price log.
    assert_eq!(summary.scanned_log_count, 4);
    // 3 op events + 1 single-mode write + 2 batch-mode writes + 1 price row.
    assert_eq!(summary.total_synced_count, 7);
    assert_eq!(summary.total_inserted_count, 7);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    let op_events = events
        .iter()
        .filter(|event| event.event_kind == super::EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED)
        .collect::<Vec<_>>();
    assert_eq!(op_events.len(), 3);
    let statuses = op_events
        .iter()
        .map(|event| {
            (
                event.after_state["nonce"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                event.after_state["attribution_status"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(statuses.get("7").map(String::as_str), Some("attributed"));
    assert_eq!(statuses.get("3").map(String::as_str), Some("attributed"));
    assert_eq!(
        statuses.get("11").map(String::as_str),
        Some("input_unavailable")
    );

    let write_events = events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED)
        .collect::<Vec<_>>();
    assert_eq!(write_events.len(), 3);
    for event in &write_events {
        assert_eq!(
            event.after_state["node"].as_str(),
            Some(ALICE_ETH_NAMEHASH),
            "every fixture write targets alice.eth"
        );
        assert_eq!(
            event.after_state["attribution_source"].as_str(),
            Some("calldata")
        );
    }
    // The failed batch operation still debits: its write rows exist with
    // success=false.
    let failed_writes = write_events
        .iter()
        .filter(|event| event.after_state["success"] == serde_json::json!(false))
        .count();
    assert_eq!(failed_writes, 2);
    // Primary claims synthesize the surface identity from the claimed name.
    let primary_write = write_events
        .iter()
        .find(|event| event.after_state["write_kind"].as_str() == Some("primary"))
        .expect("primary write present");
    assert_eq!(
        primary_write.logical_name_id.as_deref(),
        Some("ens:alice.eth")
    );

    let price_events = events
        .iter()
        .filter(|event| event.event_kind == super::EVENT_KIND_PRICE_FEED_ANSWER_UPDATED)
        .collect::<Vec<_>>();
    assert_eq!(price_events.len(), 1);
    assert_eq!(
        price_events[0].after_state["answer_e8"].as_str(),
        Some("250000000000")
    );

    // Idempotent resync inserts nothing new.
    let resync = sync_entrypoint_user_operation(database.pool(), TEST_CHAIN).await?;
    assert_eq!(resync.total_synced_count, 7);
    assert_eq!(resync.total_inserted_count, 0);

    std::fs::remove_dir_all(&manifest_root).ok();
    database.cleanup().await
}

#[tokio::test]
async fn stays_inert_without_an_active_manifest() -> Result<()> {
    let database = TestDatabase::new().await?;

    let summary = sync_entrypoint_user_operation(database.pool(), TEST_CHAIN).await?;
    assert_eq!(summary, super::persistence_summary::empty_summary(0));

    database.cleanup().await
}
