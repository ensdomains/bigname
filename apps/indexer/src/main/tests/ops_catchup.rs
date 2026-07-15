use std::{
    collections::BTreeMap,
    sync::{Mutex, atomic::AtomicUsize},
};

use crate::ops_catchup::{CapacityGuardConfig, OpsCatchupConfig, run_ops_finalized_catchup};
use bigname_storage::{
    BackfillLifecycleStatus, ResolverProfileReconciliationTarget,
    enqueue_resolver_profile_reconciliations, load_backfill_job, load_backfill_ranges,
};

include!("support.rs");

#[derive(Clone, Debug, Eq, PartialEq)]
struct OpsRecordedRpcRequest {
    method: String,
    params: Vec<Value>,
}

#[tokio::test]
async fn ops_catchup_bounds_chunks_to_observed_finalized_head_and_preserves_checkpoints()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(41_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        410,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;
    insert_checkpoint_guard(database.pool(), "ethereum-mainnet").await?;
    enqueue_resolver_profile_reconciliations(
        database.pool(),
        &[ResolverProfileReconciliationTarget {
            chain_id: "ethereum-mainnet".to_owned(),
            contract_address: "0x00000000000000000000000000000000000000ff".to_owned(),
        }],
    )
    .await?;

    let blocks = provider_blocks(1, 3);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(blocks, vec![3], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let config = ops_config(10);
    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config.clone(),
    )
    .await?;

    assert_eq!(outcome.planned_chunk_count, 1);
    assert_eq!(outcome.capacity_check_count, 1);
    assert_eq!(outcome.drained_job_count, 1);
    let ranges = all_backfill_ranges(database.pool()).await?;
    assert_eq!(ranges, vec![(1, 3, "completed".to_owned())]);
    assert_checkpoint_guard_unchanged(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resolver_profile_input_changes WHERE processed_generation < generation"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "a successful ops-catchup iteration must drain resolver-profile work"
    );

    let same_generation = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config.clone(),
    )
    .await?;
    assert_eq!(same_generation.reused_completed_chunk_count, 1);
    assert_eq!(same_generation.drained_job_count, 0);
    assert_eq!(ops_table_count(database.pool(), "backfill_jobs").await?, 1);

    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = retention_generation + 1
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .execute(database.pool())
    .await?;
    let after_retention = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config,
    )
    .await?;
    assert_eq!(after_retention.reused_completed_chunk_count, 0);
    assert_eq!(after_retention.drained_job_count, 1);
    let jobs = sqlx::query_as::<_, (i64, String)>(
        r#"
            SELECT raw_log_retention_generation, idempotency_key
            FROM backfill_jobs
            ORDER BY backfill_job_id
            "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].0, 0);
    assert_eq!(jobs[1].0, 1);
    assert!(jobs[0].1.ends_with(":raw_log_retention_generation=0"));
    assert!(jobs[1].1.ends_with(":raw_log_retention_generation=1"));
    assert_eq!(
        jobs[0].1.strip_suffix(":raw_log_retention_generation=0"),
        jobs[1].1.strip_suffix(":raw_log_retention_generation=1"),
        "retention rotation must preserve the logical job identity while changing its generation"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_does_not_leave_precreated_job_pending_when_retention_rotates_before_execution()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(41_101);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        411,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;

    sqlx::query(
        r#"
        CREATE FUNCTION rotate_retention_after_first_ops_job()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE raw_log_staging_input_revisions
            SET retention_generation = 1
            WHERE chain_id = NEW.chain_id
              AND retention_generation = 0;
            RETURN NEW;
        END;
        $$
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER rotate_retention_after_first_ops_job
        AFTER INSERT ON backfill_jobs
        FOR EACH ROW
        EXECUTE FUNCTION rotate_retention_after_first_ops_job()
        "#,
    )
    .execute(database.pool())
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 2), vec![2], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        ops_config(2),
    )
    .await?;

    assert_eq!(outcome.drained_job_count, 2);
    assert_eq!(outcome.reused_completed_chunk_count, 0);
    let jobs = sqlx::query_as::<_, (i64, String, String)>(
        r#"
        SELECT
            job.raw_log_retention_generation,
            job.status::TEXT,
            range.status::TEXT
        FROM backfill_jobs AS job
        JOIN backfill_ranges AS range USING (backfill_job_id)
        ORDER BY job.raw_log_retention_generation
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        jobs,
        vec![
            (0, "completed".to_owned(), "completed".to_owned()),
            (1, "completed".to_owned(), "completed".to_owned()),
        ],
        "the capacity-check job and its generation retry must both be drained"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_retry_reloads_targets_admitted_during_prior_pass() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let manifest_id = 412;
    let registry_contract_instance_id = Uuid::from_u128(41_201);
    let registry_address = "0x0000000000000000000000000000000000001201";
    let child_address = "0x0000000000000000000000000000000000001202";
    insert_ops_ens_v1_registry_manifest_contract(
        database.pool(),
        manifest_id,
        chain,
        registry_contract_instance_id,
        registry_address,
        1,
    )
    .await?;

    // The fixture uses the pinned ENSv1 registry event encodings: assigning a
    // subnode owner emits NewOwner, and a later TTL write emits NewTTL.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L82 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L100 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L104 @ ens_v1@91c966f)
    let blocks = provider_blocks(1, 3);
    let fixtures = vec![
        ProviderBlockFixture {
            block: blocks[0].clone(),
            logs: vec![ops_ens_v1_new_owner_log_payload(
                &blocks[0],
                registry_address,
                child_address,
                0,
            )],
        },
        ProviderBlockFixture {
            block: blocks[1].clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: blocks[2].clone(),
            logs: vec![ops_ens_v1_new_ttl_log_payload(&blocks[2], child_address, 0)],
        },
    ];
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider_with_fixtures(fixtures, vec![3], Arc::clone(&requests)).await?;
    let registry = ProviderRegistry::from_chain_rpc_urls(&[format!("{chain}={provider_url}")])?;

    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task(chain, registry_address)],
        &registry,
        ops_config(3),
    )
    .await?;

    assert!(
        outcome.drained_job_count >= 2,
        "discovery-epoch retry must execute a second target plan"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM raw_logs
                WHERE chain_id = $1
                  AND emitting_address = lower($2)
                  AND block_number = 3
            )
            "#,
        )
        .bind(chain)
        .bind(child_address)
        .fetch_one(database.pool())
        .await?,
        "the retry must reload the newly admitted child and fetch its later log inside the same finalized range"
    );
    assert!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .iter()
            .filter(|request| request.method == "eth_getLogs")
            .filter_map(|request| request.params.first())
            .filter_map(Value::as_object)
            .filter_map(|filter| filter.get("address"))
            .any(|addresses| match addresses {
                Value::String(address) => address.eq_ignore_ascii_case(child_address),
                Value::Array(addresses) => addresses.iter().any(|address| {
                    address
                        .as_str()
                        .is_some_and(|address| address.eq_ignore_ascii_case(child_address))
                }),
                _ => false,
            }),
        "the retry must issue a provider log request that includes the newly admitted child"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_rebuilds_ensv2_retained_history_proof_after_compaction() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let chain = "ethereum-sepolia";
    let root_manifest_id = 4_130;
    let registry_manifest_id = 4_131;
    let root_contract_instance_id = Uuid::from_u128(41_301);
    let registry_contract_instance_id = Uuid::from_u128(41_302);
    let child_contract_instance_id = Uuid::from_u128(41_303);
    let root_address = "0x0000000000000000000000000000000000001301";
    let registry_address = "0x0000000000000000000000000000000000001302";
    let child_address = "0x0000000000000000000000000000000000001303";
    insert_ops_ens_v2_registry_manifests(
        database.pool(),
        chain,
        root_manifest_id,
        registry_manifest_id,
        root_contract_instance_id,
        registry_contract_instance_id,
        root_address,
        registry_address,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        child_contract_instance_id,
        chain,
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        child_contract_instance_id,
        chain,
        child_address,
        Some(registry_manifest_id),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now(),
            active_from_block_number = 2,
            active_from_block_hash = $3,
            active_to_block_number = 4,
            active_to_block_hash = $4
        WHERE chain_id = $1
          AND contract_instance_id = $2
        "#,
    )
    .bind(chain)
    .bind(child_contract_instance_id)
    .bind(hex_string(&[2; 32]))
    .bind(hex_string(&[4; 32]))
    .execute(database.pool())
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        chain,
        "subregistry",
        registry_contract_instance_id,
        child_contract_instance_id,
        Some(registry_manifest_id),
        Some(2),
        Some(4),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now(),
            active_from_block_hash = $3,
            active_to_block_hash = $4
        WHERE chain_id = $1
          AND to_contract_instance_id = $2
        "#,
    )
    .bind(chain)
    .bind(child_contract_instance_id)
    .bind(hex_string(&[2; 32]))
    .bind(hex_string(&[4; 32]))
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, 0)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES ($1, 0, 1, false, now(), NULL, NULL, NULL)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let blocks = provider_blocks(1, 4);
    let fixtures = vec![
        ProviderBlockFixture {
            block: blocks[0].clone(),
            logs: vec![ops_ens_v2_label_registered_log_payload(
                &blocks[0],
                registry_address,
                1,
                "alice",
                0,
            )],
        },
        ProviderBlockFixture {
            block: blocks[1].clone(),
            logs: vec![ops_ens_v2_subregistry_updated_log_payload(
                &blocks[1],
                registry_address,
                child_address,
                1,
                0,
            )],
        },
        ProviderBlockFixture {
            block: blocks[2].clone(),
            logs: vec![ops_ens_v2_label_registered_log_payload(
                &blocks[2],
                child_address,
                2,
                "bob",
                0,
            )],
        },
        ProviderBlockFixture {
            block: blocks[3].clone(),
            logs: vec![ops_ens_v2_subregistry_updated_log_payload(
                &blocks[3],
                registry_address,
                "0x0000000000000000000000000000000000000000",
                1,
                0,
            )],
        },
    ];
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider_with_fixtures(fixtures, vec![4], Arc::clone(&requests)).await?;
    let registry = ProviderRegistry::from_chain_rpc_urls(&[format!("{chain}={provider_url}")])?;

    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task(chain, registry_address)],
        &registry,
        ops_config(4),
    )
    .await?;
    assert!(outcome.drained_job_count >= 1);

    let discovery_epoch =
        bigname_manifests::load_discovery_admission_epoch(database.pool(), chain).await?;
    assert_eq!(
        sqlx::query_as::<_, (bool, Option<i64>, Option<i64>, Option<i64>)>(
            r#"
            SELECT
                retained_history_complete,
                proven_retention_generation,
                proven_discovery_admission_epoch,
                proven_through_block
            FROM raw_log_staging_input_revisions
            WHERE chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (true, Some(1), Some(discovery_epoch), Some(4)),
        "ops catch-up must finish the full-source ENSv2 reconciliation and bind the rebuilt proof to current authority"
    );
    assert!(
        !crate::bootstrap_backfill::load_bootstrap_retention_snapshot(database.pool(), chain, 4,)
            .await?
            .requires_ens_v2_history_recovery,
        "completed ops recovery must not leave the chain fail-closed"
    );
    assert_eq!(
        ops_address_coverage_generations(
            database.pool(),
            chain,
            "ens_v2_registry_l1",
            child_address,
            2,
            4,
        )
        .await?,
        vec![1],
        "the compacted generation must cover the closed child registry's full authoritative interval"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_follow_rereads_finalized_head_for_repeated_finite_jobs() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(42_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        420,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 4), vec![2, 4], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let mut config = ops_config(2);
    config.follow = true;
    config.follow_iterations = Some(2);

    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config,
    )
    .await?;

    assert_eq!(outcome.follow_iteration_count, 2);
    assert_eq!(outcome.drained_job_count, 2);
    assert_eq!(outcome.reused_completed_chunk_count, 1);
    assert_eq!(
        all_backfill_ranges(database.pool()).await?,
        vec![
            (1, 2, "completed".to_owned()),
            (3, 4, "completed".to_owned()),
        ]
    );
    assert!(
        requests
            .lock()
            .expect("request log must not be poisoned")
            .iter()
            .filter(|request| {
                request.method == "eth_getBlockByNumber"
                    && request.params.first().and_then(Value::as_str) == Some("finalized")
            })
            .count()
            >= 2,
        "follow mode must re-read finalized head between iterations"
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_capacity_guard_fails_chunk_with_persisted_metadata_before_range_work()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let contract_instance_id = Uuid::from_u128(43_001);
    let address = "0x0000000000000000000000000000000000000001";
    insert_ops_watched_manifest_contract(
        database.pool(),
        430,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 2), vec![2], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let mut config = ops_config(2);
    config.capacity.postgres_max_bytes = Some(1);

    let error = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task("ethereum-mainnet", address)],
        &registry,
        config,
    )
    .await
    .expect_err("capacity breach must fail the chunk explicitly");
    assert!(
        error.to_string().contains("capacity guard breached"),
        "unexpected error: {error:#}"
    );

    let job_id = sqlx::query_scalar::<_, i64>("SELECT backfill_job_id FROM backfill_jobs")
        .fetch_one(database.pool())
        .await?;
    let job = load_backfill_job(database.pool(), job_id)
        .await?
        .expect("capacity-failed job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        job.failure_reason.as_deref(),
        Some("ops catch-up capacity guard breached")
    );
    assert_eq!(
        job.failure_metadata.get("phase").and_then(Value::as_str),
        Some("capacity_guard")
    );
    assert_eq!(
        job.failure_metadata
            .get("capacity_status")
            .and_then(Value::as_str),
        Some("breached")
    );
    assert_eq!(
        job.failure_metadata
            .get("object_cache_budget_checked")
            .and_then(Value::as_bool),
        Some(false)
    );
    assert!(
        job.failure_metadata
            .get("postgres_database_size_bytes")
            .and_then(Value::as_u64)
            .is_some()
    );
    let ranges = load_backfill_ranges(database.pool(), job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Failed);
    assert_eq!(ops_table_count(database.pool(), "chain_lineage").await?, 0);
    assert_eq!(ops_table_count(database.pool(), "raw_logs").await?, 0);

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_missing_base_provider_is_idle_while_ethereum_runs_and_unknown_start_skips()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    let eth_address = "0x0000000000000000000000000000000000000001";
    let base_address = "0x0000000000000000000000000000000000000002";
    insert_ops_watched_manifest_contract(
        database.pool(),
        440,
        "ens",
        "ethereum-mainnet",
        "ens_v1_wrapper_l1",
        Uuid::from_u128(44_001),
        eth_address,
        Some(1),
    )
    .await?;
    insert_ops_watched_manifest_contract(
        database.pool(),
        441,
        "ens",
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        Uuid::from_u128(44_002),
        "0x0000000000000000000000000000000000000003",
        None,
    )
    .await?;
    insert_ops_watched_manifest_contract(
        database.pool(),
        442,
        "basenames",
        "base-mainnet",
        "basenames_base_registry",
        Uuid::from_u128(44_003),
        base_address,
        Some(1),
    )
    .await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (provider_url, server) =
        ops_catchup_provider(provider_blocks(1, 2), vec![2], Arc::clone(&requests)).await?;
    let registry =
        ProviderRegistry::from_chain_rpc_urls(&[format!("ethereum-mainnet={provider_url}")])?;
    let outcome = run_ops_finalized_catchup(
        database.pool(),
        &[
            catchup_task("base-mainnet", base_address),
            catchup_task("ethereum-mainnet", eth_address),
        ],
        &registry,
        ops_config(2),
    )
    .await?;

    assert_eq!(outcome.provider_configured_chain_count, 1);
    assert_eq!(outcome.missing_provider_chain_count, 1);
    assert_eq!(outcome.skipped_unknown_start_target_count, 1);
    assert_eq!(outcome.drained_job_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM backfill_jobs WHERE chain_id = 'base-mainnet'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        all_backfill_ranges(database.pool()).await?,
        vec![(1, 2, "completed".to_owned())]
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn ops_catchup_rejects_configured_provider_outside_intake_tasks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registry = ProviderRegistry::from_chain_rpc_urls(&[
        "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
        "optimism-mainnet=http://127.0.0.1:7545".to_owned(),
    ])?;

    let error = run_ops_finalized_catchup(
        database.pool(),
        &[catchup_task(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000001",
        )],
        &registry,
        ops_config(2),
    )
    .await
    .expect_err("ops catch-up must reject providers outside selected runtime chains");

    assert!(
        error.to_string().contains(
            "configured provider source chains outside selected/admitted runtime chain set: optimism-mainnet"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

async fn ops_catchup_provider(
    blocks: Vec<ProviderBlock>,
    finalized_sequence: Vec<i64>,
    requests: Arc<Mutex<Vec<OpsRecordedRpcRequest>>>,
) -> Result<(String, JoinHandle<()>)> {
    ops_catchup_provider_with_fixtures(
        blocks
            .into_iter()
            .map(|block| ProviderBlockFixture {
                logs: vec![rpc_log_payload(&block)],
                block,
            })
            .collect(),
        finalized_sequence,
        requests,
    )
    .await
}

async fn ops_catchup_provider_with_fixtures(
    fixtures: Vec<ProviderBlockFixture>,
    finalized_sequence: Vec<i64>,
    requests: Arc<Mutex<Vec<OpsRecordedRpcRequest>>>,
) -> Result<(String, JoinHandle<()>)> {
    let fixtures_by_hash = Arc::new(
        fixtures
            .into_iter()
            .map(|fixture| (fixture.block.block_hash.clone(), fixture))
            .collect::<BTreeMap<_, _>>(),
    );
    let hashes_by_number = Arc::new(
        fixtures_by_hash
            .values()
            .map(|fixture| (fixture.block.block_number, fixture.block.block_hash.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    let latest_number = *hashes_by_number
        .keys()
        .next_back()
        .context("ops catch-up provider needs at least one block")?;
    let finalized_reads = Arc::new(AtomicUsize::new(0));
    let finalized_sequence = Arc::new(finalized_sequence);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        requests
            .lock()
            .expect("request log must not be poisoned")
            .push(OpsRecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                let selection = params
                    .first()
                    .and_then(Value::as_str)
                    .expect("block number or tag parameter must be present");
                let block_number = match selection {
                    "latest" => latest_number,
                    "safe" => finalized_sequence.first().copied().unwrap_or(latest_number),
                    "finalized" => {
                        let index = finalized_reads.fetch_add(1, Ordering::Relaxed);
                        finalized_sequence
                            .get(index)
                            .copied()
                            .or_else(|| finalized_sequence.last().copied())
                            .unwrap_or(latest_number)
                    }
                    number => support_parse_rpc_block_number(number),
                };
                let hash = hashes_by_number
                    .get(&block_number)
                    .unwrap_or_else(|| panic!("unexpected block number request: {body}"));
                rpc_block_bundle_payload(&fixtures_by_hash.get(hash).unwrap().block)
            }
            "eth_getBlockByHash" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                rpc_block_bundle_payload(&fixtures_by_hash.get(&block_hash).unwrap().block)
            }
            "eth_getLogs" => support_logs_for_filter(
                params.first().and_then(Value::as_object).unwrap(),
                &fixtures_by_hash,
                &hashes_by_number,
            ),
            "eth_getBlockReceipts" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                Value::Array(vec![rpc_receipt_payload(
                    &fixtures_by_hash.get(&block_hash).unwrap().block,
                )])
            }
            "eth_getCode" => Value::String("0x6001600155".to_owned()),
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    Ok((url, server))
}

fn ops_ens_v1_new_owner_log_payload(
    block: &ProviderBlock,
    address: &str,
    owner: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            keccak256_hex(b"NewOwner(bytes32,bytes32,address)"),
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            keccak256_hex(b"eth"),
        ],
        "data": hex_string(&abi_word_address(owner)),
    })
}

fn ops_ens_v1_new_ttl_log_payload(block: &ProviderBlock, address: &str, log_index: u64) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            keccak256_hex(b"NewTTL(bytes32,uint64)"),
            keccak256_hex(b"descendant.eth"),
        ],
        "data": hex_string(&abi_word_u64(3_600)),
    })
}

fn ops_ens_v2_subregistry_updated_log_payload(
    block: &ProviderBlock,
    address: &str,
    subregistry: &str,
    token_id: u64,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            keccak256_hex(b"SubregistryUpdated(uint256,address,address)"),
            hex_string(&abi_word_u64(token_id)),
            hex_string(&abi_word_address(subregistry)),
            hex_string(&abi_word_address(
                "0x0000000000000000000000000000000000000dad"
            )),
        ],
        "data": "0x"
    })
}

fn ops_ens_v2_label_registered_log_payload(
    block: &ProviderBlock,
    address: &str,
    token_id: u64,
    label: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            keccak256_hex(b"LabelRegistered(uint256,bytes32,string,address,uint64,address)"),
            hex_string(&abi_word_u64(token_id)),
            keccak256_hex(label.as_bytes()),
            hex_string(&abi_word_address(
                "0x0000000000000000000000000000000000000dad"
            )),
        ],
        "data": encode_ens_v2_label_registered_log_data(
            label,
            "0x0000000000000000000000000000000000000a11",
            1_900_000_000,
        ),
    })
}

fn provider_blocks(from: i64, to: i64) -> Vec<ProviderBlock> {
    (from..=to)
        .map(|block_number| {
            let byte = u8::try_from(block_number).expect("test block number must fit in u8");
            let hash = hex_string(&[byte; 32]);
            let parent_hash = if block_number == from {
                None
            } else {
                let parent_byte =
                    u8::try_from(block_number - 1).expect("test parent must fit in u8");
                Some(hex_string(&[parent_byte; 32]))
            };
            provider_block(&hash, parent_hash.as_deref(), block_number)
        })
        .collect()
}

fn ops_config(chunk_blocks: i64) -> OpsCatchupConfig {
    OpsCatchupConfig {
        deployment_profile: "mainnet".to_owned(),
        manifests_root: PathBuf::from("manifests/mainnet"),
        chunk_blocks,
        follow: false,
        follow_iterations: None,
        follow_poll_interval_secs: 1,
        lease_duration_secs: 300,
        header_audit_mode: HeaderAuditMode::Minimal,
        capacity: CapacityGuardConfig {
            postgres_max_bytes: None,
            min_writable_free_disk_bytes: 0,
            writable_free_disk_path: std::env::current_dir().expect("current dir must exist"),
            estimated_bytes_per_block: 0,
        },
    }
}

fn catchup_task(chain: &str, address: &str) -> IntakeChainTask {
    IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: None,
            canonical_block_number: None,
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_ops_watched_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
    start_block: Option<i64>,
) -> Result<()> {
    let contracts = match start_block {
        Some(start_block) => json!([{"role": "WatchedContract", "start_block": start_block}]),
        None => json!([{"role": "WatchedContract"}]),
    };
    let manifest_payload = json!({
        "contracts": contracts,
        "roots": [],
        "abi": {"events": test_manifest_abi_events()},
    });
    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                namespace,
                source_family,
                chain,
                rollout_status,
                manifest_payload
            )
            VALUES ($1, $2, $3, $4, 'active', $5::jsonb)
            "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(serde_json::to_string(&manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        "WatchedContract",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

async fn insert_ops_ens_v1_registry_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
    start_block: i64,
) -> Result<()> {
    let manifest_payload = json!({
        "roots": [{
            "name": "ENSRegistry",
            "address": address,
            "start_block": start_block
        }],
        "contracts": [{
            "role": "registry",
            "address": address,
            "start_block": start_block
        }],
        "discovery_rules": [{
            "edge_kind": "subregistry",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": {"events": test_manifest_abi_events()},
    });
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            namespace,
            source_family,
            chain,
            rollout_status,
            manifest_payload
        )
        VALUES ($1, 'ens', 'ens_v1_registry_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        "registry",
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(pool, manifest_id, contract_instance_id, address)
        .await?;
    insert_manifest_discovery_rule(
        pool,
        manifest_id,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_ops_ens_v2_registry_manifests(
    pool: &PgPool,
    chain: &str,
    root_manifest_id: i64,
    registry_manifest_id: i64,
    root_contract_instance_id: Uuid,
    registry_contract_instance_id: Uuid,
    root_address: &str,
    registry_address: &str,
) -> Result<()> {
    let root_manifest_payload = json!({
        "roots": [{
            "name": "RootRegistry",
            "address": root_address,
            "start_block": 1
        }],
        "contracts": [],
        "abi": {"events": test_manifest_abi_events()},
    });
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            namespace,
            source_family,
            chain,
            rollout_status,
            manifest_payload
        )
        VALUES ($1, 'ens', 'ens_v2_root_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(root_manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&root_manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, root_contract_instance_id, chain, "root").await?;
    insert_active_contract_instance_address(
        pool,
        root_contract_instance_id,
        chain,
        root_address,
        Some(root_manifest_id),
    )
    .await?;
    insert_manifest_root_contract_instance(
        pool,
        root_manifest_id,
        root_contract_instance_id,
        root_address,
    )
    .await?;

    let registry_manifest_payload = json!({
        "roots": [{
            "name": "RootRegistry",
            "address": registry_address,
            "start_block": 1
        }],
        "contracts": [{
            "role": "registry",
            "address": registry_address,
            "start_block": 1
        }],
        "discovery_rules": [{
            "edge_kind": "subregistry",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": {"events": test_manifest_abi_events()},
    });
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_id,
            namespace,
            source_family,
            chain,
            rollout_status,
            manifest_payload
        )
        VALUES ($1, 'ens', 'ens_v2_registry_l1', $2, 'active', $3::jsonb)
        "#,
    )
    .bind(registry_manifest_id)
    .bind(chain)
    .bind(serde_json::to_string(&registry_manifest_payload)?)
    .execute(pool)
    .await?;
    insert_contract_instance(pool, registry_contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        registry_contract_instance_id,
        chain,
        registry_address,
        Some(registry_manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        registry_manifest_id,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        pool,
        registry_manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        pool,
        registry_manifest_id,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await
}

async fn insert_checkpoint_guard(pool: &PgPool, chain: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES (
            $1,
            '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
            70,
            '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
            60,
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
            50
        )
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_checkpoint_guard_unchanged(pool: &PgPool, chain: &str) -> Result<()> {
    assert_eq!(
        sqlx::query_as::<_, (String, i64, String, i64, String, i64)>(
            r#"
            SELECT
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number,
                finalized_block_hash,
                finalized_block_number
            FROM chain_checkpoints
            WHERE chain_id = $1
            "#
        )
        .bind(chain)
        .fetch_one(pool)
        .await?,
        (
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            70,
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            60,
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            50,
        )
    );
    Ok(())
}

async fn all_backfill_ranges(pool: &PgPool) -> Result<Vec<(i64, i64, String)>> {
    sqlx::query_as::<_, (i64, i64, String)>(
        r#"
        SELECT
            range_start_block_number,
            range_end_block_number,
            status::TEXT
        FROM backfill_ranges
        ORDER BY range_start_block_number, range_end_block_number
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load backfill ranges")
}

async fn ops_table_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table_name}"))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count rows in {table_name}"))
}

async fn ops_address_coverage_generations(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    address: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<i64>> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT DISTINCT job.raw_log_retention_generation
        FROM backfill_coverage_facts fact
        JOIN backfill_jobs job ON job.backfill_job_id = fact.backfill_job_id
        WHERE fact.chain_id = $1
          AND fact.source_family = $2
          AND fact.scope = 'address'
          AND fact.address = lower($3)
          AND fact.covered_from_block = $4
          AND fact.covered_to_block = $5
        ORDER BY job.raw_log_retention_generation
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .bind(address)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .context("failed to load ops catch-up address coverage generations")
}
