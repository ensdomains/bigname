use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    process::Command,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bigname_manifests::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use serde_json::json;
use sqlx::types::Uuid;
use tracing_subscriber::fmt::MakeWriter;

use super::{
    CoinbaseSqlSourceRegistry, coinbase_sql_logs_need_validation_provider_payload,
    error::CoinbaseSqlHttpError,
    evidence::fetch_windowed_stored_log_identity_evidence_with,
    pagination::{CoinbaseSqlLogCursor, append_page_rows, ensure_full_page_advanced_cursor},
    planner::build_filter_packs,
    push_deduped_log,
    query::{CoinbaseSqlFilterPack, build_or_split_filter_pack, build_query},
    rows::CoinbaseSqlLogRow,
    stored_log_identity_bucket_from_value, stored_log_identity_evidence_query,
};
use crate::{
    backfill::{
        BackfillBlockRange, BackfillTopicPlan, COINBASE_SQL_RESULT_SET_CAP,
        CoinbaseSqlBackfillConfig, CoinbaseSqlFetchStats, CoinbaseSqlValidationMode,
        DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT, HistoricalLogPayloadRequest,
        StoredLogIdentityEvidence, StoredLogIdentityEvidenceRequest,
        reservation_execution::{
            backfill_job_source_identity_payload, coinbase_sql_backfill_job_source_identity_payload,
        },
        selection::SelectedTargetIntervalIndex,
    },
    provider::{ProviderLog, ProviderResolvedBlock},
};

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct CapturedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        String::from_utf8(
            self.bytes
                .lock()
                .expect("captured log mutex must not be poisoned")
                .clone(),
        )
        .expect("captured logs must be UTF-8")
    }
}

impl io::Write for CapturedLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("captured log mutex must not be poisoned")
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

#[test]
fn stored_identity_query_returns_bounded_bucket_count_and_digest_evidence() -> Result<()> {
    let request = StoredLogIdentityEvidenceRequest {
        chain: "base-mainnet".to_owned(),
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topic0s: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        range: BackfillBlockRange::new(0, 46_954_147)?,
        bucket_blocks: 131_072,
    };
    let sql = stored_log_identity_evidence_query(&request, request.range)?;

    assert!(sql.contains("GROUP BY bucket"));
    assert!(sql.contains("groupBitXor"));
    assert!(sql.contains("MD5(concat(lower(block_hash), lower(transaction_hash)"));
    assert!(sql.contains("l.address = '0x1111111111111111111111111111111111111111'"));
    assert!(!sql.contains("LIMIT"));

    let bucket = stored_log_identity_bucket_from_value(json!({
        "bucket": "7",
        "selected_log_count": "4341559",
        "digest_left": "18446744073709551615",
        "digest_right": 42
    }))?;
    assert_eq!(bucket.bucket, 7);
    assert_eq!(bucket.selected_log_count, 4_341_559);
    assert_eq!(bucket.digest_left, u64::MAX);
    assert_eq!(bucket.digest_right, 42);
    Ok(())
}

#[test]
fn stored_identity_query_reduces_logs_before_one_narrow_transaction_scan() -> Result<()> {
    let request = StoredLogIdentityEvidenceRequest {
        chain: "base-mainnet".to_owned(),
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topic0s: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        range: BackfillBlockRange::new(10, 20)?,
        bucket_blocks: 131_072,
    };
    let sql = stored_log_identity_evidence_query(&request, request.range)?;

    assert!(
        sql.contains("WITH selected_rows AS"),
        "aggregate evidence must filter the two log tables before reading transactions"
    );
    assert_eq!(
        sql.matches("FROM base.transactions t").count(),
        1,
        "aggregate evidence must scan the transaction changelog once"
    );
    assert!(sql.contains("JOIN active_log_rows l"));
    assert!(sql.contains("t.transaction_index"));
    assert!(sql.contains(
        "HAVING sum(CASE WHEN toString(t.action) IN ('1', 'added') THEN 1 WHEN toString(t.action) IN ('-1', 'removed') THEN -1 ELSE 0 END) > 0"
    ));
    assert!(
        sql.find("active_log_rows AS").is_some_and(|logs| sql
            .find("FROM base.transactions t")
            .is_some_and(|tx| logs < tx)),
        "log action reduction must happen before the transaction scan"
    );
    assert!(
        !sql.contains("lower(l.address)") && !sql.contains("lower(l.topics[1])"),
        "normalized direct predicates must preserve provider pushdown"
    );
    Ok(())
}

#[test]
fn stored_identity_sub_window_keeps_the_whole_range_bucket_origin() -> Result<()> {
    let request = StoredLogIdentityEvidenceRequest {
        chain: "base-mainnet".to_owned(),
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topic0s: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        range: BackfillBlockRange::new(10, 30)?,
        bucket_blocks: 8,
    };

    let sql = stored_log_identity_evidence_query(&request, BackfillBlockRange::new(15, 20)?)?;

    assert!(sql.contains("l.block_number BETWEEN 15 AND 20"));
    assert!(sql.contains("intDiv(toInt64(block_number) - 10, 8) AS bucket"));
    Ok(())
}

#[tokio::test]
async fn evidence_resource_limit_halvings_log_each_retry_class_and_depth() -> Result<()> {
    let request = StoredLogIdentityEvidenceRequest {
        chain: "base-mainnet".to_owned(),
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topic0s: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        range: BackfillBlockRange::new(0, 3)?,
        bucket_blocks: 4,
    };
    let captured_logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(captured_logs.clone())
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let evidence =
        fetch_windowed_stored_log_identity_evidence_with(&request, 4, |query_range| async move {
            let query_blocks = query_range.to_block - query_range.from_block + 1;
            if query_blocks > 1 {
                let body = if query_blocks == 4 {
                    "Query memory limit exceeded"
                } else {
                    "Limit for rows or bytes to read on leaf node exceeded"
                };
                return Err(CoinbaseSqlHttpError {
                    status: reqwest::StatusCode::BAD_REQUEST,
                    body: body.to_owned(),
                    attempt_count: 1,
                }
                .into());
            }
            Ok(StoredLogIdentityEvidence {
                buckets: Vec::new(),
                query_count: 1,
            })
        })
        .await?;

    assert_eq!(evidence.query_count, 6);
    let logs = captured_logs.contents();
    let message = "retrying Coinbase SQL stored-history evidence query with a halved window";
    assert_eq!(logs.matches(message).count(), 2, "{logs}");
    for expected in [
        "window_blocks=2",
        "halving_depth=1",
        "error_class=\"query_memory_limit\"",
        "window_blocks=1",
        "halving_depth=2",
        "error_class=\"query_bytes_read_limit\"",
    ] {
        assert!(logs.contains(expected), "missing {expected:?} in {logs}");
    }
    Ok(())
}

fn pack(
    addresses: Vec<String>,
    topic0s: Vec<String>,
    event_signatures: Vec<String>,
) -> CoinbaseSqlFilterPack {
    CoinbaseSqlFilterPack {
        chain: "base-mainnet".to_owned(),
        from_block: 10,
        to_block: 20,
        addresses,
        topic0s,
        event_signatures,
        scan_all_emitters: false,
        source_families: vec!["basenames_base_registry".to_owned()],
    }
}

fn source_plan_for_family(source_family: &str) -> WatchedSourceSelectorPlan {
    let address = "0x1111111111111111111111111111111111111111";
    WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: Some(source_family.to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: vec![WatchedBackfillTarget {
            source_family: source_family.to_owned(),
            contract_instance_id: Uuid::from_u128(1),
            address: address.to_owned(),
            effective_from_block: 1,
            effective_to_block: 8_192,
        }],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
    }
}

fn coinbase_sql_test_config(
    validation_mode: CoinbaseSqlValidationMode,
) -> CoinbaseSqlBackfillConfig {
    CoinbaseSqlBackfillConfig {
        initial_window_blocks: 8_192,
        max_window_blocks: 8_192,
        evidence_window_blocks: 4_000_000,
        page_limit: 50_000,
        sql_char_limit: 10_000,
        query_timeout_secs: 30,
        rate_limit_qps: 5,
        validation_mode,
    }
}

#[test]
fn clients_from_one_registry_share_rate_limiter_across_chains() -> Result<()> {
    const CHILD_MARKER: &str = "BIGNAME_TEST_COINBASE_SQL_SHARED_LIMITER_CHILD";
    const KEY_ID_ENV: &str = "BIGNAME_TEST_COINBASE_SQL_SHARED_LIMITER_KEY_ID";
    const KEY_SECRET_ENV: &str = "BIGNAME_TEST_COINBASE_SQL_SHARED_LIMITER_KEY_SECRET";

    if std::env::var_os(CHILD_MARKER).is_none() {
        // Supply credentials at process creation instead of mutating the
        // multi-threaded test process environment.
        let output = Command::new(std::env::current_exe()?)
            .arg("clients_from_one_registry_share_rate_limiter_across_chains")
            .arg("--nocapture")
            .env(CHILD_MARKER, "1")
            .env(KEY_ID_ENV, "test-key")
            .env(KEY_SECRET_ENV, STANDARD.encode([7_u8; 64]))
            .output()?;
        if !output.status.success() {
            anyhow::bail!(
                "shared limiter child test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        return Ok(());
    }

    let registry = CoinbaseSqlSourceRegistry::from_entries(
        &["ethereum=default".to_owned(), "base=default".to_owned()],
        KEY_ID_ENV.to_owned(),
        KEY_SECRET_ENV.to_owned(),
        coinbase_sql_test_config(CoinbaseSqlValidationMode::Full),
    )?;
    let ethereum = registry
        .source_for("ethereum")?
        .expect("ethereum source should be configured");
    let base = registry
        .source_for("base")?
        .expect("base source should be configured");

    assert!(Arc::ptr_eq(
        &ethereum.client.rate_limiter,
        &base.client.rate_limiter,
    ));
    Ok(())
}

#[test]
fn non_scan_all_coinbase_sql_source_identity_hash_includes_coinbase_fields() -> Result<()> {
    let source_plan = source_plan_for_family("basenames_base_resolver");
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([(
            "basenames_base_resolver".to_owned(),
            vec!["0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned()],
        )]),
        BTreeMap::new(),
        BTreeSet::new(),
    );
    let changed_topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([(
            "basenames_base_resolver".to_owned(),
            vec!["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned()],
        )]),
        BTreeMap::new(),
        BTreeSet::new(),
    );

    let sample_payload = coinbase_sql_backfill_job_source_identity_payload(
        &source_plan,
        &coinbase_sql_test_config(CoinbaseSqlValidationMode::Sample),
        &topic_plan,
    )?;
    let full_payload = coinbase_sql_backfill_job_source_identity_payload(
        &source_plan,
        &coinbase_sql_test_config(CoinbaseSqlValidationMode::Full),
        &topic_plan,
    )?;
    let changed_topic_payload = coinbase_sql_backfill_job_source_identity_payload(
        &source_plan,
        &coinbase_sql_test_config(CoinbaseSqlValidationMode::Sample),
        &changed_topic_plan,
    )?;
    let base_payload = backfill_job_source_identity_payload(&source_plan)?;

    assert_eq!(sample_payload["coinbase_sql_validation_mode"], "sample");
    assert_eq!(full_payload["coinbase_sql_validation_mode"], "full");
    assert_ne!(
        sample_payload["source_identity_hash"],
        full_payload["source_identity_hash"]
    );
    assert_ne!(
        sample_payload["source_identity_hash"],
        changed_topic_payload["source_identity_hash"]
    );
    assert_ne!(
        sample_payload["source_identity_hash"],
        base_payload["source_identity_hash"]
    );

    Ok(())
}

#[test]
fn sample_validation_requires_provider_payload_for_returned_decoded_logs() {
    assert!(coinbase_sql_logs_need_validation_provider_payload(
        CoinbaseSqlValidationMode::Sample,
        true,
        false,
    ));
    assert!(!coinbase_sql_logs_need_validation_provider_payload(
        CoinbaseSqlValidationMode::Sample,
        false,
        false,
    ));
    assert!(coinbase_sql_logs_need_validation_provider_payload(
        CoinbaseSqlValidationMode::Full,
        false,
        false,
    ));
}

#[test]
fn configured_coinbase_sql_page_limit_is_capped_at_effective_result_limit() {
    let config = coinbase_sql_test_config(CoinbaseSqlValidationMode::Full);

    assert_eq!(config.page_limit, 50_000);
    assert_eq!(config.effective_page_limit(), COINBASE_SQL_RESULT_SET_CAP);
}

#[test]
fn query_builder_batches_addresses_and_topics() -> Result<()> {
    let pack = pack(
        vec![
            "0x1111111111111111111111111111111111111111".to_owned(),
            "0x2222222222222222222222222222222222222222".to_owned(),
        ],
        vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
        vec![
            "NameRegistered(string,bytes32,address,uint256)".to_owned(),
            "Transfer(address,address,uint256)".to_owned(),
        ],
    );

    let sql = build_query(&pack, None, 50_000)?;

    assert!(sql.contains("WITH active_transactions AS"));
    assert!(sql.contains("decoded_log_rows AS"));
    assert!(sql.contains("decoded_log_sums AS"));
    assert!(sql.contains("active_decoded_logs AS"));
    assert!(sql.contains("encoded_log_rows AS"));
    assert!(sql.contains("encoded_log_sums AS"));
    assert!(sql.contains("active_encoded_logs AS"));
    assert!(sql.contains("FROM base.events l"));
    assert!(sql.contains("FROM base.encoded_logs l"));
    assert!(sql.contains("JOIN active_transactions t"));
    assert!(sql.contains("t.transaction_index AS transaction_index"));
    assert!(sql.contains("l.log_index AS log_index"));
    assert!(sql.contains("l.event_signature AS event_signature"));
    assert!(sql.contains("toJSONString(l.parameters) AS parameters"));
    assert!(sql.contains("any(l.parameters) AS parameters"));
    assert!(sql.contains("CAST(NULL AS Nullable(String)) AS parameters"));
    assert!(sql.contains("l.address IN ('0x1111111111111111111111111111111111111111', '0x2222222222222222222222222222222222222222')"));
    assert!(sql.contains("l.event_signature IN ('NameRegistered(string,bytes32,address,uint256)', 'Transfer(address,address,uint256)')"));
    assert!(sql.contains("toString(action) IN ('1', 'added')"));
    assert!(sql.contains("toString(l.action) IN ('-1', 'removed')"));
    assert!(sql.contains("AND t.block_hash = l.block_hash"));
    assert!(sql.contains("WHERE t.action_sum > 0"));
    assert!(sql.contains("WHERE e.action_sum > 0"));
    assert!(!sql.contains("HAVING"));
    assert!(!sql.contains("row_number()"));
    assert!(!sql.contains(" OVER "));
    assert!(sql.contains("l.topics[1] IN ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')"));
    assert!(sql.contains("FROM active_decoded_logs l"));
    assert!(sql.contains("FROM active_encoded_logs l"));
    assert!(sql.contains("UNION ALL"));
    assert!(sql.contains("ORDER BY block_number, transaction_index, log_index"));

    let final_event_select_pos = sql
        .find("FROM active_decoded_logs l")
        .expect("query should read active event logs in final selection");
    let cte_section = &sql[..final_event_select_pos];
    assert!(!cte_section.contains("UNION"));
    assert!(
        sql.find("l.address IN")
            .expect("address filter should be present")
            < final_event_select_pos
    );
    assert!(
        sql.find("l.event_signature IN")
            .expect("event signature filter should be present")
            < final_event_select_pos
    );
    Ok(())
}

#[test]
fn query_builder_allows_scan_all_emitter_topic_queries() -> Result<()> {
    let mut pack = pack(
        Vec::new(),
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        vec!["Transfer(address,address,uint256)".to_owned()],
    );
    pack.scan_all_emitters = true;

    let sql = build_query(&pack, None, 50_000)?;

    assert!(!sql.contains("l.address IN"));
    assert!(sql.contains("l.event_signature IN ('Transfer(address,address,uint256)')"));
    assert!(sql.contains(
        "l.topics[1] IN ('0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')"
    ));
    assert!(sql.contains("FROM base.events l"));
    assert!(sql.contains("FROM base.encoded_logs l"));
    Ok(())
}

#[test]
fn query_splitter_keeps_queries_under_character_budget() -> Result<()> {
    let addresses = (0..512)
        .map(|index| format!("0x{index:040x}"))
        .collect::<Vec<_>>();
    let topic0s = (0..8)
        .map(|index| format!("0x{index:064x}"))
        .collect::<Vec<_>>();
    let event_signatures = (0..8)
        .map(|index| format!("Event{index}(bytes32)"))
        .collect::<Vec<_>>();
    let char_limit = DEFAULT_COINBASE_SQL_QUERY_CHAR_LIMIT;
    let packs = build_or_split_filter_pack(
        pack(addresses, topic0s, event_signatures),
        char_limit,
        50_000,
    )?;

    assert!(packs.len() > 1);
    for pack in packs {
        assert!(build_query(&pack, None, 50_000)?.len() <= char_limit);
    }
    Ok(())
}

#[test]
fn query_splitter_splits_scan_all_event_signature_query_over_character_budget() -> Result<()> {
    let mut pack = pack(
        Vec::new(),
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        (0..256)
            .map(|index| format!("VeryLongBasenamesResolverEventSignature{index}(bytes32,string,string,string,address,uint256)"))
            .collect(),
    );
    pack.scan_all_emitters = true;
    let single_signature_query_len = pack
        .event_signatures
        .iter()
        .map(|signature| {
            build_query(
                &CoinbaseSqlFilterPack {
                    event_signatures: vec![signature.clone()],
                    ..pack.clone()
                },
                None,
                50_000,
            )
            .map(|query| query.len())
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .expect("test pack has event signatures");
    let char_limit = single_signature_query_len + 500;

    let packs = build_or_split_filter_pack(pack.clone(), char_limit, 50_000)?;
    let split_signatures = packs
        .iter()
        .flat_map(|pack| pack.event_signatures.iter().cloned())
        .collect::<BTreeSet<_>>();

    assert!(packs.len() > 1);
    assert_eq!(
        split_signatures,
        pack.event_signatures.into_iter().collect::<BTreeSet<_>>()
    );
    for pack in packs {
        assert!(pack.scan_all_emitters);
        assert!(pack.addresses.is_empty());
        assert!(build_query(&pack, None, 50_000)?.len() <= char_limit);
    }
    Ok(())
}

#[test]
fn planner_scans_all_emitters_for_large_basenames_registry_source_family_sets() -> Result<()> {
    let addresses = (0..513)
        .map(|index| format!("0x{index:040x}"))
        .collect::<Vec<_>>();
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("basenames_base_registry".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: addresses
            .iter()
            .enumerate()
            .map(|(index, address)| WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(index as u128 + 1),
                address: address.clone(),
                effective_from_block: 10,
                effective_to_block: 10,
            })
            .collect(),
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: addresses.clone(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 513,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 10,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];
    let topic0 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([("basenames_base_registry".to_owned(), vec![topic0.clone()])]),
        BTreeMap::from([(
            "basenames_base_registry".to_owned(),
            vec!["Transfer(address,address,uint256)".to_owned()],
        )]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 10)?,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    });

    assert_eq!(packs.len(), 1);
    assert!(packs[0].scan_all_emitters);
    assert!(packs[0].addresses.is_empty());
    assert_eq!(packs[0].topic0s, vec![topic0]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["Transfer(address,address,uint256)".to_owned()]
    );
    assert_eq!(
        packs[0].source_families,
        vec!["basenames_base_registry".to_owned()]
    );
    Ok(())
}

#[test]
fn planner_keeps_large_whole_chain_basenames_registry_address_filtered() -> Result<()> {
    let addresses = (0..513)
        .map(|index| format!("0x{index:040x}"))
        .collect::<Vec<_>>();
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: addresses
            .iter()
            .enumerate()
            .map(|(index, address)| WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(index as u128 + 1),
                address: address.clone(),
                effective_from_block: 10,
                effective_to_block: 10,
            })
            .collect(),
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: addresses.clone(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 513,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 10,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];
    let topic0 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([("basenames_base_registry".to_owned(), vec![topic0.clone()])]),
        BTreeMap::from([(
            "basenames_base_registry".to_owned(),
            vec!["Transfer(address,address,uint256)".to_owned()],
        )]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 10)?,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    });

    assert_eq!(packs.len(), 1);
    assert!(!packs[0].scan_all_emitters);
    assert_eq!(packs[0].addresses.len(), addresses.len());
    assert_eq!(packs[0].addresses, addresses);
    assert_eq!(packs[0].topic0s, vec![topic0]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["Transfer(address,address,uint256)".to_owned()]
    );
    assert_eq!(
        packs[0].source_families,
        vec!["basenames_base_registry".to_owned()]
    );
    Ok(())
}

#[test]
fn planner_coalesces_basenames_registry_scan_all_windows() -> Result<()> {
    let address_a = "0x1111111111111111111111111111111111111111";
    let address_b = "0x2222222222222222222222222222222222222222";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("basenames_base_registry".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address_a.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address_b.to_owned(),
                effective_from_block: 11,
                effective_to_block: 11,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address_a.to_owned(), address_b.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 10,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 11,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
        },
    ];
    let topic0 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([("basenames_base_registry".to_owned(), vec![topic0.clone()])]),
        BTreeMap::from([(
            "basenames_base_registry".to_owned(),
            vec!["NewResolver(bytes32,address)".to_owned()],
        )]),
        BTreeSet::new(),
    );
    let selected_addresses = vec![address_a.to_owned(), address_b.to_owned()];

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 11)?,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    });

    assert_eq!(packs.len(), 1);
    assert!(packs[0].scan_all_emitters);
    assert!(packs[0].addresses.is_empty());
    assert_eq!(packs[0].from_block, 10);
    assert_eq!(packs[0].to_block, 11);
    assert_eq!(packs[0].topic0s, vec![topic0]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["NewResolver(bytes32,address)".to_owned()]
    );
    Ok(())
}

#[test]
fn planner_keeps_targeted_basenames_registry_address_filtered_under_threshold() -> Result<()> {
    let address_a = "0x1111111111111111111111111111111111111111";
    let address_b = "0x2222222222222222222222222222222222222222";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address_a.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "basenames_base_registry".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address_b.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address_a.to_owned(), address_b.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 10,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];
    let topic0 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([("basenames_base_registry".to_owned(), vec![topic0.clone()])]),
        BTreeMap::from([(
            "basenames_base_registry".to_owned(),
            vec!["NewResolver(bytes32,address)".to_owned()],
        )]),
        BTreeSet::new(),
    );
    let selected_addresses = vec![address_a.to_owned(), address_b.to_owned()];

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 10)?,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    });

    assert_eq!(packs.len(), 1);
    assert!(!packs[0].scan_all_emitters);
    assert_eq!(
        packs[0].addresses,
        vec![address_a.to_owned(), address_b.to_owned()]
    );
    assert_eq!(packs[0].topic0s, vec![topic0]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["NewResolver(bytes32,address)".to_owned()]
    );
    Ok(())
}

#[test]
fn planner_keeps_basenames_resolver_address_filtered_until_scan_all_is_supported() -> Result<()> {
    let address_a = "0x1111111111111111111111111111111111111111";
    let address_b = "0x2222222222222222222222222222222222222222";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::SourceFamily,
        source_family: Some("basenames_base_resolver".to_owned()),
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "basenames_base_resolver".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address_a.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "basenames_base_resolver".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address_b.to_owned(),
                effective_from_block: 11,
                effective_to_block: 11,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address_a.to_owned(), address_b.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 10,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 11,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
        },
    ];
    let topic0 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([("basenames_base_resolver".to_owned(), vec![topic0.clone()])]),
        BTreeMap::from([(
            "basenames_base_resolver".to_owned(),
            vec!["TextChanged(bytes32,string,string,string)".to_owned()],
        )]),
        BTreeSet::new(),
    );
    let selected_addresses = vec![address_a.to_owned(), address_b.to_owned()];

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 11)?,
        validation_mode: CoinbaseSqlValidationMode::Sample,
    });

    assert_eq!(packs.len(), 2);
    assert!(packs.iter().all(|pack| !pack.scan_all_emitters));
    assert_eq!(packs[0].addresses, vec![address_a.to_owned()]);
    assert_eq!(packs[0].from_block, 10);
    assert_eq!(packs[0].to_block, 10);
    assert_eq!(packs[0].topic0s, vec![topic0.clone()]);
    assert_eq!(packs[1].addresses, vec![address_b.to_owned()]);
    assert_eq!(packs[1].from_block, 11);
    assert_eq!(packs[1].to_block, 11);
    assert_eq!(packs[1].topic0s, vec![topic0]);
    assert!(packs.iter().all(|pack| {
        pack.event_signatures == vec!["TextChanged(bytes32,string,string,string)".to_owned()]
            && pack.source_families == vec!["basenames_base_resolver".to_owned()]
    }));
    Ok(())
}

#[test]
fn planner_splits_same_address_when_source_family_topics_change() -> Result<()> {
    let address = "0x1111111111111111111111111111111111111111";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "family_a".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "family_b".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address.to_owned(),
                effective_from_block: 11,
                effective_to_block: 11,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![
        ProviderResolvedBlock {
            block_number: 10,
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        },
        ProviderResolvedBlock {
            block_number: 11,
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
        },
    ];
    let selected_addresses = vec![address.to_owned()];
    let topic_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let topic_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([
            ("family_a".to_owned(), vec![topic_a.clone()]),
            ("family_b".to_owned(), vec![topic_b.clone()]),
        ]),
        BTreeMap::from([
            ("family_a".to_owned(), vec!["EventA(bytes32)".to_owned()]),
            ("family_b".to_owned(), vec!["EventB(bytes32)".to_owned()]),
        ]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 11)?,
        validation_mode: CoinbaseSqlValidationMode::Full,
    });

    assert_eq!(packs.len(), 2);
    assert_eq!(packs[0].from_block, 10);
    assert_eq!(packs[0].to_block, 10);
    assert_eq!(packs[0].topic0s, vec![topic_a]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["EventA(bytes32)".to_owned()]
    );
    assert_eq!(packs[1].from_block, 11);
    assert_eq!(packs[1].to_block, 11);
    assert_eq!(packs[1].topic0s, vec![topic_b]);
    assert_eq!(
        packs[1].event_signatures,
        vec!["EventB(bytes32)".to_owned()]
    );
    Ok(())
}

#[test]
fn planner_does_not_cartesian_product_addresses_and_topics() -> Result<()> {
    let address_a = "0x1111111111111111111111111111111111111111";
    let address_b = "0x2222222222222222222222222222222222222222";
    let source_plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets: vec![
            WatchedBackfillTarget {
                source_family: "family_a".to_owned(),
                contract_instance_id: Uuid::from_u128(1),
                address: address_a.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
            WatchedBackfillTarget {
                source_family: "family_b".to_owned(),
                contract_instance_id: Uuid::from_u128(2),
                address: address_b.to_owned(),
                effective_from_block: 10,
                effective_to_block: 10,
            },
        ],
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![address_a.to_owned(), address_b.to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        },
    };
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
    let resolved_blocks = vec![ProviderResolvedBlock {
        block_number: 10,
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    }];
    let selected_addresses = vec![address_a.to_owned(), address_b.to_owned()];
    let topic_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let topic_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
    let topic_plan = BackfillTopicPlan::new(
        BTreeMap::from([
            ("family_a".to_owned(), vec![topic_a.clone()]),
            ("family_b".to_owned(), vec![topic_b.clone()]),
        ]),
        BTreeMap::from([
            ("family_a".to_owned(), vec!["EventA(bytes32)".to_owned()]),
            ("family_b".to_owned(), vec!["EventB(bytes32)".to_owned()]),
        ]),
        BTreeSet::new(),
    );

    let packs = build_filter_packs(&HistoricalLogPayloadRequest {
        chain: "base-mainnet",
        source_plan: &source_plan,
        selected_target_index: &selected_target_index,
        resolved_blocks: &resolved_blocks,
        selected_target_addresses_for_chunk: &selected_addresses,
        topic_plan: &topic_plan,
        range: BackfillBlockRange::new(10, 10)?,
        validation_mode: CoinbaseSqlValidationMode::Full,
    });

    assert_eq!(packs.len(), 2);
    assert_eq!(packs[0].addresses, vec![address_a.to_owned()]);
    assert_eq!(packs[0].topic0s, vec![topic_a]);
    assert_eq!(
        packs[0].event_signatures,
        vec!["EventA(bytes32)".to_owned()]
    );
    assert_eq!(packs[1].addresses, vec![address_b.to_owned()]);
    assert_eq!(packs[1].topic0s, vec![topic_b]);
    assert_eq!(
        packs[1].event_signatures,
        vec!["EventB(bytes32)".to_owned()]
    );
    Ok(())
}

#[test]
fn duplicate_sql_pack_log_identities_are_deduped() {
    let log = ProviderLog {
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        block_number: 10,
        transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_owned(),
        transaction_index: 1,
        log_index: 2,
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        topics: vec![
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ],
        data: "0x".to_owned(),
    };
    let mut logs_by_block = BTreeMap::new();
    let mut seen = BTreeSet::new();

    push_deduped_log(&mut logs_by_block, &mut seen, log.clone());
    push_deduped_log(&mut logs_by_block, &mut seen, log);

    assert_eq!(logs_by_block[&10].len(), 1);
}

#[test]
fn basenames_decoded_parameters_synthesize_raw_log_data() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "NameRegistered(string,bytes32,address,uint256)",
        "parameters": {
            "name": "alice",
            "expires": "123"
        },
        "topics": [
            "0x0667086d08417333ce63f40d5bc2ef6fd330e25aaaf317b7c489541f8fe600fa",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "0x0000000000000000000000002222222222222222222222222222222222222222"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "000000000000000000000000000000000000000000000000000000000000007b",
            "0000000000000000000000000000000000000000000000000000000000000005",
            "616c696365000000000000000000000000000000000000000000000000000000"
        )
    );
    Ok(())
}

#[test]
fn decoded_parameters_json_string_synthesize_raw_log_data() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "NameRegistered(string,bytes32,address,uint256)",
        "parameters": "{\"name\":\"alice\",\"expires\":\"123\"}",
        "topics": [
            "0x0667086d08417333ce63f40d5bc2ef6fd330e25aaaf317b7c489541f8fe600fa",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "0x0000000000000000000000002222222222222222222222222222222222222222"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "000000000000000000000000000000000000000000000000000000000000007b",
            "0000000000000000000000000000000000000000000000000000000000000005",
            "616c696365000000000000000000000000000000000000000000000000000000"
        )
    );
    Ok(())
}

#[test]
fn all_indexed_coinbase_sql_events_do_not_need_payload_validation() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "Transfer(address,address,uint256)",
        "parameters": {},
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000002222222222222222222222222222222222222222",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert_eq!(row.data, "0x");
    assert!(!row.requires_validation_provider_data);
    Ok(())
}

#[test]
fn encoded_log_rows_require_validation_provider_payload() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "address": "0x1111111111111111111111111111111111111111",
        "event_signature": null,
        "parameters": null,
        "topics": [
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert_eq!(row.data, "0x");
    assert!(row.requires_validation_provider_data);
    Ok(())
}

#[test]
fn l2_reverse_name_for_addr_changed_parameters_synthesize_raw_log_data() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x0000000000d8e504002cc26e3ec46d81971c1664",
        "event_signature": "NameForAddrChanged(address,string)",
        "parameters": {
            "name": "alice.base.eth"
        },
        "topics": [
            "0x8af7a4c7007a33f680904f3b64733396b730fef22d79555dee29801ca2e479a9",
            "0x0000000000000000000000002222222222222222222222222222222222222222"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000020",
            "000000000000000000000000000000000000000000000000000000000000000e",
            "616c6963652e626173652e657468000000000000000000000000000000000000"
        )
    );
    Ok(())
}

#[test]
fn basenames_registry_decoded_parameters_synthesize_raw_log_data() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "NewResolver(bytes32,address)",
        "parameters": {
            "resolver": "0x00000000000000000000000000000000000000aa"
        },
        "topics": [
            "0x3357218ab03f9f161c8e6f9d4e5418595ab2cf9f21aa08002ea6f9e03a0a39a5",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        "0x00000000000000000000000000000000000000000000000000000000000000aa"
    );
    Ok(())
}

#[test]
fn basenames_resolver_decoded_parameters_synthesize_dynamic_raw_log_data() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "AddressChanged(bytes32,uint256,bytes)",
        "parameters": {
            "coinType": "60",
            "newAddress": "0x00000000000000000000000000000000000000aa"
        },
        "topics": [
            "0x65412581168e88a1e966121d184eda1e72e1ed3a39ca8123b140e87d9a36e945",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        concat!(
            "0x",
            "000000000000000000000000000000000000000000000000000000000000003c",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000014",
            "00000000000000000000000000000000000000aa000000000000000000000000"
        )
    );
    Ok(())
}

#[test]
fn basenames_resolver_binary_address_change_falls_back_to_provider_payload() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "AddressChanged(bytes32,uint256,bytes)",
        "parameters": {
            "coinType": "60",
            "newAddress": "\u{15}\u{13}-B\u{fffd}"
        },
        "topics": [
            "0x65412581168e88a1e966121d184eda1e72e1ed3a39ca8123b140e87d9a36e945",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert_eq!(row.data, "0x");
    assert!(row.requires_validation_provider_data);
    Ok(())
}

#[test]
fn basenames_resolver_text_decoded_parameters_synthesize_two_strings() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "TextChanged(bytes32,string,string,string)",
        "parameters": {
            "key": "url",
            "value": "ipfs://x"
        },
        "topics": [
            "0xd8c9334b912a0a410ef97b2bbd1e8f361d8b5e33bca8338ce35d9b27e5fbd33f",
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        ]
    }))?;

    assert!(!row.requires_validation_provider_data);
    assert_eq!(
        row.data,
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000080",
            "0000000000000000000000000000000000000000000000000000000000000003",
            "75726c0000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000008",
            "697066733a2f2f78000000000000000000000000000000000000000000000000"
        )
    );
    Ok(())
}

#[test]
fn unhandled_decoded_event_falls_back_to_provider_payload_validation() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": "Unhandled(bytes32,string,string)",
        "parameters": {
            "key": "url",
            "value": "https://example.test"
        },
        "topics": [
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        ]
    }))?;

    assert_eq!(row.data, "0x");
    assert!(row.requires_validation_provider_data);
    Ok(())
}

#[test]
fn row_validation_rejects_block_hash_mismatch() -> Result<()> {
    let row = CoinbaseSqlLogRow::from_value(json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_index": 1,
        "log_index": 2,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "topics": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"],
        "data": "0x",
        "tx_from": "0x2222222222222222222222222222222222222222",
        "tx_to": "0x3333333333333333333333333333333333333333"
    }))?;
    let pack = pack(
        vec!["0x1111111111111111111111111111111111111111".to_owned()],
        vec!["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned()],
        vec!["TestEvent(bytes32)".to_owned()],
    );
    let resolved = BTreeMap::from([(
        10,
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
    )]);

    let error = row
        .validate_against_filter_pack(&pack, Some(&resolved))
        .expect_err("mismatched validation-provider block hash must fail");
    assert!(format!("{error:?}").contains("validation provider resolved"));
    Ok(())
}

const DEDUP_TRANSACTION_HASH: &str =
    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DEDUP_TOPIC1: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

fn union_arm_row(
    log_index: i64,
    decoded: bool,
    transaction_hash: &str,
    topic1: &str,
) -> Result<CoinbaseSqlLogRow> {
    let mut object = json!({
        "block_number": 10,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "transaction_hash": transaction_hash,
        "transaction_index": 1,
        "log_index": log_index,
        "emitting_address": "0x1111111111111111111111111111111111111111",
        "event_signature": null,
        "parameters": null,
        "topics": [
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            topic1
        ]
    });
    if decoded {
        object["event_signature"] = json!("NewOwner(bytes32,bytes32,address)");
        object["parameters"] = json!({
            "owner": "0x2222222222222222222222222222222222222222"
        });
    }
    CoinbaseSqlLogRow::from_value(object)
}

fn decoded_union_row(log_index: i64) -> Result<CoinbaseSqlLogRow> {
    union_arm_row(log_index, true, DEDUP_TRANSACTION_HASH, DEDUP_TOPIC1)
}

fn encoded_union_row(log_index: i64) -> Result<CoinbaseSqlLogRow> {
    union_arm_row(log_index, false, DEDUP_TRANSACTION_HASH, DEDUP_TOPIC1)
}

#[test]
fn pagination_drops_byte_identical_union_duplicate_rows() -> Result<()> {
    let mut rows = Vec::new();
    let mut previous_cursor = None;
    let mut stats = CoinbaseSqlFetchStats::default();

    append_page_rows(
        &mut rows,
        &mut previous_cursor,
        vec![
            decoded_union_row(2)?,
            decoded_union_row(2)?,
            decoded_union_row(3)?,
        ],
        &mut stats,
    )?;

    assert_eq!(rows, vec![decoded_union_row(2)?, decoded_union_row(3)?]);
    assert_eq!(stats.union_duplicate_count, 1);
    Ok(())
}

#[test]
fn pagination_prefers_decoded_union_rows_in_both_arrival_orders() -> Result<()> {
    for page in [
        vec![decoded_union_row(2)?, encoded_union_row(2)?],
        vec![encoded_union_row(2)?, decoded_union_row(2)?],
    ] {
        let mut rows = Vec::new();
        let mut previous_cursor = None;
        let mut stats = CoinbaseSqlFetchStats::default();

        append_page_rows(&mut rows, &mut previous_cursor, page, &mut stats)?;

        assert_eq!(rows, vec![decoded_union_row(2)?]);
        assert!(
            !rows[0].requires_validation_provider_data,
            "the decoded shape must win regardless of arrival order"
        );
        assert_eq!(stats.union_duplicate_count, 1);
    }
    Ok(())
}

#[test]
fn pagination_deduplicates_union_rows_across_page_boundaries() -> Result<()> {
    let mut rows = Vec::new();
    let mut previous_cursor = None;
    let mut stats = CoinbaseSqlFetchStats::default();

    // Page cursors are exclusive lower bounds, so a page normally never
    // re-serves its predecessor's tail tuple; if the warehouse does repeat
    // it, the fold must reconcile rather than fail the fetch.
    append_page_rows(
        &mut rows,
        &mut previous_cursor,
        vec![decoded_union_row(1)?, encoded_union_row(2)?],
        &mut stats,
    )?;
    append_page_rows(
        &mut rows,
        &mut previous_cursor,
        vec![decoded_union_row(2)?, decoded_union_row(3)?],
        &mut stats,
    )?;

    assert_eq!(
        rows,
        vec![
            decoded_union_row(1)?,
            decoded_union_row(2)?,
            decoded_union_row(3)?
        ]
    );
    assert_eq!(stats.union_duplicate_count, 1);
    Ok(())
}

#[test]
fn pagination_rejects_conflicting_duplicate_rows() -> Result<()> {
    let conflicting_pages = [
        vec![
            decoded_union_row(2)?,
            union_arm_row(
                2,
                true,
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                DEDUP_TOPIC1,
            )?,
        ],
        vec![
            decoded_union_row(2)?,
            union_arm_row(
                2,
                false,
                DEDUP_TRANSACTION_HASH,
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            )?,
        ],
    ];
    for page in conflicting_pages {
        let mut rows = Vec::new();
        let mut previous_cursor = None;
        let mut stats = CoinbaseSqlFetchStats::default();

        let error = append_page_rows(&mut rows, &mut previous_cursor, page, &mut stats)
            .expect_err("conflicting duplicate content must fail the fetch");
        assert!(
            error.to_string().contains(
                "Coinbase SQL rows were not strictly ordered at block 10, transaction index 1, log index 2"
            ),
            "unexpected error: {error:#}"
        );
        assert_eq!(stats.union_duplicate_count, 0);
    }
    Ok(())
}

#[test]
fn pagination_rejects_backward_ordered_rows() -> Result<()> {
    let mut rows = Vec::new();
    let mut previous_cursor = None;
    let mut stats = CoinbaseSqlFetchStats::default();

    let error = append_page_rows(
        &mut rows,
        &mut previous_cursor,
        vec![decoded_union_row(3)?, decoded_union_row(2)?],
        &mut stats,
    )
    .expect_err("backward cursor order must fail the fetch");
    assert!(
        error.to_string().contains(
            "Coinbase SQL rows were not strictly ordered at block 10, transaction index 1, log index 2"
        ),
        "unexpected error: {error:#}"
    );
    assert_eq!(stats.union_duplicate_count, 0);
    Ok(())
}

#[test]
fn pagination_rejects_full_pages_that_do_not_advance_the_cursor() -> Result<()> {
    let stalled = CoinbaseSqlLogCursor {
        block_number: 10,
        transaction_index: 1,
        log_index: 2,
    };
    let error = ensure_full_page_advanced_cursor(Some(stalled), Some(stalled))
        .expect_err("a full page that leaves the cursor unmoved must fail the fetch");
    assert!(
        error
            .to_string()
            .contains("without advancing the pagination cursor past block 10, transaction index 1, log index 2"),
        "unexpected error: {error:#}"
    );

    let advanced = CoinbaseSqlLogCursor {
        block_number: 10,
        transaction_index: 1,
        log_index: 3,
    };
    ensure_full_page_advanced_cursor(Some(stalled), Some(advanced))?;
    ensure_full_page_advanced_cursor(None, Some(advanced))?;
    ensure_full_page_advanced_cursor(None, None)?;
    Ok(())
}

#[test]
fn pagination_rejects_decoded_twins_with_conflicting_content() -> Result<()> {
    // Two decoded shapes of the same underlying log (identical identity
    // fields) that differ in decoded content must not be reconciled by the
    // encoded/decoded preference: the (decoded, decoded) discriminator arm
    // falls through to the strict-order bail.
    let first = decoded_union_row(2)?;
    let mut second = decoded_union_row(2)?;
    second.data = "0xdeadbeef".to_owned();
    assert!(
        rows_describe_same_identity(&first, &second),
        "fixture rows must share every log identity field"
    );

    let mut rows = Vec::new();
    let mut previous_cursor = None;
    let mut stats = CoinbaseSqlFetchStats::default();
    let error = append_page_rows(
        &mut rows,
        &mut previous_cursor,
        vec![first, second],
        &mut stats,
    )
    .expect_err("conflicting decoded twins must fail the fetch");
    assert!(
        error.to_string().contains(
            "Coinbase SQL rows were not strictly ordered at block 10, transaction index 1, log index 2"
        ),
        "unexpected error: {error:#}"
    );
    assert_eq!(stats.union_duplicate_count, 0);
    Ok(())
}

fn rows_describe_same_identity(a: &CoinbaseSqlLogRow, b: &CoinbaseSqlLogRow) -> bool {
    a.block_number == b.block_number
        && a.block_hash == b.block_hash
        && a.transaction_hash == b.transaction_hash
        && a.transaction_index == b.transaction_index
        && a.log_index == b.log_index
        && a.emitting_address == b.emitting_address
        && a.topics == b.topics
}

#[test]
fn build_query_applies_order_and_limit_to_the_whole_union() -> Result<()> {
    let pack = pack(
        vec!["0x1111111111111111111111111111111111111111".to_owned()],
        Vec::new(),
        Vec::new(),
    );
    for cursor in [
        None,
        Some(super::pagination::CoinbaseSqlLogCursor {
            block_number: 12,
            transaction_index: 3,
            log_index: 4,
        }),
    ] {
        let sql = build_query(&pack, cursor, 50)?;
        assert_eq!(
            sql.matches("ORDER BY").count(),
            1,
            "exactly one ORDER BY must exist, and none inside either union arm: {sql}"
        );
        assert_eq!(
            sql.matches("LIMIT").count(),
            1,
            "exactly one LIMIT must exist, and none inside either union arm: {sql}"
        );
        let union_position = sql
            .find("UNION ALL")
            .expect("query must union the decoded and encoded arms");
        let order_position = sql
            .find("ORDER BY block_number, transaction_index, log_index")
            .expect("query must order by the pagination cursor tuple");
        assert!(
            order_position > union_position,
            "ORDER BY must come after both union arms: {sql}"
        );
        assert!(
            sql.contains("SELECT\n  u.block_number AS block_number,"),
            "the union arms must be wrapped in a subquery: {sql}"
        );
        // ClickHouse binds a trailing ORDER BY/LIMIT to the LAST union arm
        // only; both must sit outside the subquery that closes the union.
        assert!(
            sql.ends_with(") u\nORDER BY block_number, transaction_index, log_index\nLIMIT 50"),
            "ORDER BY/LIMIT must apply to the whole union, outside the subquery close: {sql}"
        );
    }
    Ok(())
}
