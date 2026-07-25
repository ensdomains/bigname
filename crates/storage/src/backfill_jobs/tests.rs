use std::{
    borrow::Cow,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::*;
use crate::default_database_url;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
    connect_options: PgConnectOptions,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        Self::new_before_migration(None).await
    }

    async fn new_before_migration(exclusive_version: Option<i64>) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for backfill job tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_backfill_job_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context(
                "failed to connect admin pool for backfill job tests. Run DB-backed tests through ./scripts/test-db -- <cargo test command>, or set BIGNAME_TEST_DATABASE_URL for an already-running PostgreSQL server.",
            )?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let connect_options = base_options.database(&database_name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options.clone())
            .await
            .context("failed to connect backfill job test pool")?;

        if let Some(exclusive_version) = exclusive_version {
            let migrator = sqlx::migrate::Migrator {
                migrations: Cow::Owned(
                    crate::MIGRATOR
                        .iter()
                        .filter(|migration| migration.version < exclusive_version)
                        .cloned()
                        .collect(),
                ),
                ..sqlx::migrate::Migrator::DEFAULT
            };
            migrator.run(&pool).await.with_context(|| {
                format!("failed to apply backfill test migrations before {exclusive_version}")
            })?;
        } else {
            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for backfill job tests")?;
        }

        Ok(Self {
            admin_pool,
            pool,
            database_name,
            connect_options,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A dedicated single-connection pool, pre-warmed so acquiring a
    /// connection at use time is instant. Concurrency tests need one per
    /// competing transaction: a shared pool serializes competitors inside
    /// pool acquisition for the other transaction's whole lifetime, masking
    /// the very races the tests exist to catch.
    async fn dedicated_single_connection_pool(&self) -> Result<PgPool> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.connect_options.clone())
            .await
            .context("failed to connect dedicated single-connection test pool")?;
        drop(
            pool.acquire()
                .await
                .context("failed to warm dedicated single-connection test pool")?,
        );
        Ok(pool)
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

fn backfill_job_create(idempotency_key: &str) -> BackfillJobCreate {
    BackfillJobCreate {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        source_identity: json!({
            "source_family": "ens_v1_registry_l1",
            "watch_targets": ["0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e"]
        }),
        scan_mode: "logs".to_owned(),
        range_start_block_number: 100,
        range_end_block_number: 120,
        idempotency_key: idempotency_key.to_owned(),
        ranges: vec![
            BackfillRangeSpec {
                range_start_block_number: 100,
                range_end_block_number: 109,
            },
            BackfillRangeSpec {
                range_start_block_number: 110,
                range_end_block_number: 120,
            },
        ],
    }
}

async fn set_backfill_job_attempt_count(
    pool: &PgPool,
    backfill_job_id: i64,
    attempt_count: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET attempt_count = $2,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .bind(attempt_count)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_backfill_job_completed_for_coverage_fact_test(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET status = 'completed'::backfill_lifecycle_status,
            completed_at = now(),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(backfill_job_id)
    .execute(pool)
    .await
    .with_context(|| format!("failed to complete test backfill job {backfill_job_id}"))?;
    Ok(())
}

fn lease_deadline() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 300)
        .expect("lease deadline must be valid")
}

fn coverage_recovery_reservation_fence(
    key: &crate::CoverageRecoveryFailureKey,
    expected_write_epoch: i64,
    expected_failure_attempt_count: i64,
    expected_job_attempt_count: i64,
) -> crate::CoverageRecoveryReservationFence {
    crate::CoverageRecoveryReservationFence {
        key: key.clone(),
        expected_write_epoch,
        expected_failure_attempt_count,
        expected_job_attempt_count,
    }
}

#[tokio::test]
async fn backfill_job_create_is_idempotent_and_rejects_range_widening() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = backfill_job_create("job-create-idempotent");

    let created = create_backfill_job(database.pool(), &request).await?;
    assert_eq!(created.job.status, BackfillLifecycleStatus::Pending);
    assert_eq!(created.job.range_start_block_number, 100);
    assert_eq!(created.job.range_end_block_number, 120);
    assert_eq!(created.ranges.len(), 2);
    assert_eq!(created.ranges[0].checkpoint_block_number, 99);
    assert_eq!(created.ranges[1].checkpoint_block_number, 109);

    let repeated = create_backfill_job(database.pool(), &request).await?;
    assert_eq!(repeated.job.backfill_job_id, created.job.backfill_job_id);
    assert_eq!(
        repeated
            .ranges
            .iter()
            .map(|range| range.backfill_range_id)
            .collect::<Vec<_>>(),
        created
            .ranges
            .iter()
            .map(|range| range.backfill_range_id)
            .collect::<Vec<_>>()
    );

    let mut widened = request.clone();
    widened.range_end_block_number = 121;
    widened.ranges[1].range_end_block_number = 121;
    let error = create_backfill_job(database.pool(), &widened)
        .await
        .expect_err("idempotent create must reject range widening");
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_jobs_capture_the_raw_log_retention_generation_at_creation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let original_request = backfill_job_create("job-before-retention-generation-change");

    assert_eq!(
        ensure_and_load_raw_log_retention_generation(database.pool(), &original_request.chain_id,)
            .await?,
        0
    );
    let original = create_backfill_job(database.pool(), &original_request).await?;
    assert_eq!(original.job.raw_log_retention_generation, 0);

    let updated = sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = retention_generation + 1
        WHERE chain_id = $1
        "#,
    )
    .bind(&original_request.chain_id)
    .execute(database.pool())
    .await
    .context("failed to advance test raw-log retention generation")?;
    assert_eq!(updated.rows_affected(), 1);
    assert_eq!(
        ensure_and_load_raw_log_retention_generation(database.pool(), &original_request.chain_id,)
            .await?,
        1
    );

    let repeated = create_backfill_job(database.pool(), &original_request).await?;
    assert_eq!(repeated.job.backfill_job_id, original.job.backfill_job_id);
    assert_eq!(repeated.job.raw_log_retention_generation, 0);

    let next_request = backfill_job_create("job-after-retention-generation-change");
    let next = create_backfill_job(database.pool(), &next_request).await?;
    assert_ne!(next.job.backfill_job_id, original.job.backfill_job_id);
    assert_eq!(next.job.raw_log_retention_generation, 1);

    let reloaded_original = load_backfill_job(database.pool(), original.job.backfill_job_id)
        .await?
        .context("missing original backfill job")?;
    assert_eq!(reloaded_original.raw_log_retention_generation, 0);

    database.cleanup().await
}

#[tokio::test]
async fn generation_scoped_creation_does_not_reuse_a_completed_pre_compaction_job() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = backfill_job_create("automatic-generation-scoped-job");

    let original = create_generation_scoped_backfill_job(database.pool(), &request).await?;
    assert_eq!(original.job.raw_log_retention_generation, 0);
    assert_eq!(
        original.job.idempotency_key,
        "automatic-generation-scoped-job:raw_log_retention_generation=0"
    );
    mark_backfill_job_completed_for_coverage_fact_test(
        database.pool(),
        original.job.backfill_job_id,
    )
    .await?;

    let stale_planned_generation =
        ensure_and_load_raw_log_retention_generation(database.pool(), &request.chain_id).await?;
    assert_eq!(stale_planned_generation, 0);
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = retention_generation + 1
        WHERE chain_id = $1
        "#,
    )
    .bind(&request.chain_id)
    .execute(database.pool())
    .await
    .context("failed to simulate raw-log compaction after automatic job planning")?;

    let after_compaction = create_generation_scoped_backfill_job(database.pool(), &request).await?;
    assert_ne!(
        after_compaction.job.backfill_job_id,
        original.job.backfill_job_id
    );
    assert_eq!(
        after_compaction.job.status,
        BackfillLifecycleStatus::Pending
    );
    assert_eq!(after_compaction.job.raw_log_retention_generation, 1);
    assert_eq!(
        after_compaction.job.idempotency_key,
        "automatic-generation-scoped-job:raw_log_retention_generation=1"
    );

    let repeated = create_generation_scoped_backfill_job(database.pool(), &request).await?;
    assert_eq!(
        repeated.job.backfill_job_id,
        after_compaction.job.backfill_job_id
    );
    assert_eq!(repeated.job.raw_log_retention_generation, 1);

    database.cleanup().await
}

#[tokio::test]
async fn generation_scoped_creation_rejects_a_manual_key_collision_from_an_older_generation()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_key = "automatic-generation-collision";
    let manual_request =
        backfill_job_create(&format!("{logical_key}:raw_log_retention_generation=1"));
    let manual = create_backfill_job(database.pool(), &manual_request).await?;
    assert_eq!(manual.job.raw_log_retention_generation, 0);

    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = 1
        WHERE chain_id = $1
        "#,
    )
    .bind(&manual_request.chain_id)
    .execute(database.pool())
    .await
    .context("failed to advance raw-log retention generation for collision test")?;

    let automatic_request = backfill_job_create(logical_key);
    let error = create_generation_scoped_backfill_job(database.pool(), &automatic_request)
        .await
        .expect_err("an older-generation manual key collision must fail closed");
    assert!(
        error
            .to_string()
            .contains("captured raw-log retention generation 0, expected 1"),
        "unexpected collision error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn obsolete_generation_sweep_closes_pending_recovery_jobs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "eth-mainnet";
    ensure_and_load_raw_log_retention_generation(database.pool(), chain).await?;

    let recovery = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("indexer-full-closure-coverage-recovery:v2:pending"),
    )
    .await?;
    let unrelated = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("operator-managed-pending"),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = retention_generation + 1
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let swept = fail_obsolete_generation_backfill_jobs(
        database.pool(),
        chain,
        "indexer-full-closure-coverage-recovery:",
    )
    .await?;
    assert_eq!(swept, vec![recovery.job.backfill_job_id]);

    let recovery_job = load_backfill_job(database.pool(), recovery.job.backfill_job_id)
        .await?
        .context("missing swept recovery job")?;
    assert_eq!(recovery_job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        recovery_job.failure_metadata["cause"],
        "obsolete_retention_generation"
    );
    assert!(
        load_backfill_ranges(database.pool(), recovery.job.backfill_job_id)
            .await?
            .iter()
            .all(|range| range.status == BackfillLifecycleStatus::Failed)
    );
    assert_eq!(
        load_backfill_job(database.pool(), unrelated.job.backfill_job_id)
            .await?
            .context("missing unrelated pending job")?
            .status,
        BackfillLifecycleStatus::Pending
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_recovery_attempt_budget_survives_job_revisions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_job = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-budget:revision=1"),
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), first_job.job.backfill_job_id, 1).await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };
    let first = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        first_job.job.backfill_job_id,
        1,
        5,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "provider mismatch",
        "coverage recovery attempt budget exhausted",
        json!({"cause": "provider_mismatch"}),
    )
    .await?;
    assert_eq!(first.attempt_count, 1);
    assert_eq!(
        first.state,
        crate::CoverageRecoveryFailureState::RetryBackoff
    );
    assert_eq!(first.failure_metadata["retry_after_seconds"], 5);

    let repeated = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        first_job.job.backfill_job_id,
        1,
        5,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "duplicate observation",
        "coverage recovery attempt budget exhausted",
        json!({}),
    )
    .await?;
    assert_eq!(repeated, first);

    let next_job = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-budget:revision=2"),
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), next_job.job.backfill_job_id, 4).await?;
    let terminal = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        next_job.job.backfill_job_id,
        4,
        5,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "persistent provider mismatch",
        "coverage recovery attempt budget exhausted",
        json!({"cause": "provider_mismatch"}),
    )
    .await?;
    assert_eq!(terminal.attempt_count, 5);
    assert_eq!(
        terminal.state,
        crate::CoverageRecoveryFailureState::Terminal
    );
    assert!(terminal.retry_not_before.is_none());
    assert_eq!(
        terminal.failure_reason,
        "coverage recovery attempt budget exhausted"
    );
    assert_eq!(
        terminal.failure_metadata["cause"],
        "attempt_budget_exhausted"
    );

    assert_eq!(
        crate::load_coverage_recovery_failure(database.pool(), &key)
            .await?
            .context("missing persisted failure budget")?,
        terminal
    );
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET status = 'failed'::backfill_lifecycle_status,
            failure_reason = 'coverage recovery attempt budget exhausted',
            failure_metadata = '{"state":"terminal"}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(next_job.job.backfill_job_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET status = 'failed'::backfill_lifecycle_status,
            attempt_count = 5,
            failure_reason = 'coverage recovery attempt budget exhausted',
            failure_metadata = '{"state":"terminal"}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(next_job.job.backfill_job_id)
    .execute(database.pool())
    .await?;
    assert!(
        crate::rearm_terminal_coverage_recovery_failure(database.pool(), &key).await?,
        "the exact terminal interval must be operator re-armable"
    );
    assert!(
        load_backfill_ranges(database.pool(), next_job.job.backfill_job_id)
            .await?
            .iter()
            .all(|range| range.attempt_count == 0),
        "re-arm must reset the exhausted persisted job so its next reservation is attempt one"
    );
    assert_eq!(
        crate::load_coverage_recovery_epoch(database.pool(), &key).await?,
        1,
        "operator re-arm must leave a durable epoch tombstone"
    );
    let rearmed_range = reserve_backfill_range(
        database.pool(),
        next_job.job.backfill_job_id,
        "rearmed-worker",
        "rearmed-lease",
        OffsetDateTime::now_utc() + std::time::Duration::from_secs(300),
    )
    .await?
    .context("re-armed range was not reservable")?;
    assert_eq!(rearmed_range.attempt_count, 1);
    let stale_terminal = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        next_job.job.backfill_job_id,
        5,
        5,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "stale provider mismatch observation",
        "coverage recovery attempt budget exhausted",
        json!({"cause": "provider_mismatch"}),
    )
    .await;
    assert!(
        stale_terminal.is_err(),
        "an observation cached before re-arm must not recreate terminal state after attempts reset"
    );
    let stale_preparation = crate::record_coverage_recovery_terminal_failure(
        database.pool(),
        &key,
        0,
        Some(next_job.job.backfill_job_id),
        0,
        "stale topic-less preparation",
        json!({"cause": "source_family_without_active_event_topic0"}),
    )
    .await;
    assert!(
        stale_preparation.is_err(),
        "terminal preparation planned before re-arm must fail its epoch compare-and-set"
    );
    let preserved = load_backfill_ranges(database.pool(), next_job.job.backfill_job_id).await?;
    assert_eq!(preserved[0].lease_token.as_deref(), Some("rearmed-lease"));
    assert_eq!(preserved[0].status, BackfillLifecycleStatus::Reserved);
    assert!(
        !crate::rearm_terminal_coverage_recovery_failure(database.pool(), &key).await?,
        "re-arming an already cleared interval must report no match"
    );
    assert!(
        crate::load_coverage_recovery_failure(database.pool(), &key)
            .await?
            .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_recovery_attempt_watermarks_survive_job_revisit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let job_a = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-revisit:plan-a"),
    )
    .await?;
    let job_b = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-revisit:plan-b"),
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), job_a.job.backfill_job_id, 10).await?;
    set_backfill_job_attempt_count(database.pool(), job_b.job.backfill_job_id, 1).await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };
    let record = |job_id, attempt_count, reason| {
        crate::record_coverage_recovery_attempt_failure(
            database.pool(),
            &key,
            0,
            job_id,
            attempt_count,
            32,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(300),
            reason,
            "coverage recovery attempt budget exhausted",
            json!({"cause": "provider_mismatch"}),
        )
    };
    assert_eq!(
        record(job_a.job.backfill_job_id, 10, "plan A failures")
            .await?
            .attempt_count,
        10
    );
    assert_eq!(
        record(job_b.job.backfill_job_id, 1, "plan B failure")
            .await?
            .attempt_count,
        11
    );
    let revisited = record(
        job_a.job.backfill_job_id,
        10,
        "revisited plan A observation",
    )
    .await?;
    assert_eq!(
        revisited.attempt_count, 11,
        "returning to an older immutable job must not count its ten attempts twice"
    );
    assert_eq!(
        revisited.last_backfill_job_id,
        Some(job_a.job.backfill_job_id),
        "the no-delta revisit must move the crash-recovery pointer to the current plan"
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_recovery_rearm_resets_older_exact_window_job_revisions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };
    let mut plan_a = backfill_job_create(
        "indexer-full-closure-coverage-recovery:v2:rearm-plan-a:coverage_recovery_write_epoch=0:plan",
    );
    plan_a.source_identity = json!({
        "selector_kind": "watched_target_set",
        "selected_targets": [{
            "source_family": key.source_family,
            "address": key.emitting_address,
            "effective_from_block": key.required_from_block,
            "effective_to_block": key.required_to_block,
        }],
        "topic0s_by_source_family": {
            "ens_v1_registry_l1": ["0xplan-a"]
        },
    });
    let first_job = create_generation_scoped_backfill_job(database.pool(), &plan_a).await?;
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        0,
        first_job.job.backfill_job_id,
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), first_job.job.backfill_job_id, 31).await?;
    let retry = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        first_job.job.backfill_job_id,
        31,
        32,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "provider mismatch on plan A",
        "coverage recovery attempt budget exhausted",
        json!({"cause": "provider_mismatch"}),
    )
    .await?;
    assert_eq!(retry.attempt_count, 31);

    let mut plan_b = backfill_job_create(
        "indexer-full-closure-coverage-recovery:v2:rearm-plan-b:coverage_recovery_write_epoch=0:plan",
    );
    plan_b.source_identity = json!({
        "selector_kind": "watched_target_set",
        "selected_targets": [{
            "source_family": key.source_family,
            "address": key.emitting_address,
            "effective_from_block": key.required_from_block,
            "effective_to_block": key.required_to_block,
        }],
        "topic0s_by_source_family": {
            "ens_v1_registry_l1": []
        },
    });
    let terminal_job = create_generation_scoped_backfill_job(database.pool(), &plan_b).await?;
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        0,
        terminal_job.job.backfill_job_id,
    )
    .await?;
    crate::record_coverage_recovery_terminal_failure(
        database.pool(),
        &key,
        0,
        Some(terminal_job.job.backfill_job_id),
        0,
        "topic authority temporarily has no active event topic0 values",
        json!({"cause": "source_family_without_active_event_topic0"}),
    )
    .await?;

    assert!(
        crate::rearm_terminal_coverage_recovery_failure(database.pool(), &key).await?,
        "the repaired exact window must be re-armed"
    );
    for job_id in [
        first_job.job.backfill_job_id,
        terminal_job.job.backfill_job_id,
    ] {
        assert!(
            load_backfill_ranges(database.pool(), job_id)
                .await?
                .iter()
                .all(|range| range.attempt_count == 0),
            "re-arm must discard pre-epoch attempts from every incomplete exact-window job revision; job {job_id} retained attempts"
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT coverage_recovery_write_epoch FROM backfill_jobs WHERE backfill_job_id = $1",
            )
            .bind(job_id)
            .fetch_one(database.pool())
            .await?,
            Some(1),
            "re-arm must bind every reusable exact-window job revision to the new epoch"
        );
    }
    let stale_fence = coverage_recovery_reservation_fence(&key, 0, 0, 0);
    let stale_reservation = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        first_job.job.backfill_job_id,
        Some(&stale_fence),
        "stale-plan-worker",
        "stale-plan-lease",
        lease_deadline(),
    )
    .await;
    assert!(
        stale_reservation.is_err(),
        "a runner holding the pre-rearm job key must not consume a post-rearm attempt"
    );
    assert!(
        load_backfill_ranges(database.pool(), first_job.job.backfill_job_id)
            .await?
            .iter()
            .all(|range| range.attempt_count == 0),
        "the stale reservation guard must reject before incrementing an attempt"
    );
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        1,
        first_job.job.backfill_job_id,
    )
    .await?;
    let current_fence = coverage_recovery_reservation_fence(&key, 1, 0, 0);
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        1,
        terminal_job.job.backfill_job_id,
    )
    .await?;
    let superseded_reservation = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        first_job.job.backfill_job_id,
        Some(&current_fence),
        "superseded-plan-worker",
        "superseded-plan-lease",
        lease_deadline(),
    )
    .await
    .expect_err("a newly bound plan must make the prior job ineligible");
    assert!(
        superseded_reservation
            .downcast_ref::<CoverageRecoveryReservationConflict>()
            .is_some(),
        "superseded-plan reservation must return the typed deferred conflict: {superseded_reservation:#}"
    );
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        1,
        first_job.job.backfill_job_id,
    )
    .await?;
    let current_reservation = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        first_job.job.backfill_job_id,
        Some(&current_fence),
        "current-plan-worker",
        "current-plan-lease",
        lease_deadline(),
    )
    .await?
    .context("the rebound job must remain reservable in the new epoch")?;
    let stale_binding = crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        0,
        first_job.job.backfill_job_id,
    )
    .await;
    assert!(
        stale_binding.is_err(),
        "a stale planner must not rebind or fail a job after operator re-arm"
    );
    let preserved = load_backfill_ranges(database.pool(), first_job.job.backfill_job_id).await?;
    let preserved_current = preserved
        .iter()
        .find(|range| range.backfill_range_id == current_reservation.backfill_range_id)
        .context("current epoch reservation disappeared")?;
    assert_eq!(
        preserved_current.lease_token.as_deref(),
        Some("current-plan-lease"),
        "stale epoch handling must not erase a new epoch runner's live lease"
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_recovery_reservation_fences_the_cached_final_attempt() -> Result<()> {
    let database = TestDatabase::new().await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };
    let mut request = backfill_job_create(
        "indexer-full-closure-coverage-recovery:v2:final-attempt:coverage_recovery_write_epoch=0:plan",
    );
    request.ranges = Vec::new();
    request.source_identity = json!({
        "selector_kind": "watched_target_set",
        "selected_targets": [{
            "source_family": key.source_family,
            "address": key.emitting_address,
            "effective_from_block": key.required_from_block,
            "effective_to_block": key.required_to_block,
        }],
    });
    let job = create_generation_scoped_backfill_job(database.pool(), &request).await?;
    crate::bind_coverage_recovery_job_write_epoch(
        database.pool(),
        &key,
        0,
        job.job.backfill_job_id,
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), job.job.backfill_job_id, 31).await?;
    let recorded = crate::record_coverage_recovery_attempt_failure(
        database.pool(),
        &key,
        0,
        job.job.backfill_job_id,
        31,
        32,
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(300),
        "provider mismatch before final attempt",
        "coverage recovery attempt budget exhausted",
        json!({"cause": "provider_mismatch"}),
    )
    .await?;
    assert_eq!(recorded.attempt_count, 31);
    let final_attempt_fence = coverage_recovery_reservation_fence(&key, 0, 31, 31);

    let final_attempt = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        job.job.backfill_job_id,
        Some(&final_attempt_fence),
        "final-attempt-worker",
        "final-attempt-lease",
        lease_deadline(),
    )
    .await?
    .context("attempt 32 must be reservable")?;
    assert_eq!(final_attempt.attempt_count, 32);
    let duplicate_final_attempt = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        job.job.backfill_job_id,
        Some(&final_attempt_fence),
        "final-attempt-worker",
        "final-attempt-lease",
        lease_deadline(),
    )
    .await?
    .context("the same active final-attempt lease must remain idempotent")?;
    assert_eq!(
        duplicate_final_attempt.backfill_range_id,
        final_attempt.backfill_range_id
    );
    fail_backfill_range(
        database.pool(),
        final_attempt.backfill_range_id,
        "final-attempt-lease",
        "persistent provider mismatch",
        json!({"attempt": 32}),
    )
    .await?;

    let stale_final_attempt = reserve_backfill_range_with_coverage_recovery_fence(
        database.pool(),
        job.job.backfill_job_id,
        Some(&final_attempt_fence),
        "stale-final-attempt-worker",
        "stale-final-attempt-lease",
        lease_deadline(),
    )
    .await;
    assert!(
        stale_final_attempt.is_err(),
        "a second poll that cached attempt 31 must not reserve provider attempt 33"
    );
    assert_eq!(
        load_backfill_ranges(database.pool(), job.job.backfill_job_id).await?[0].attempt_count,
        32,
        "the stale final-attempt reservation must reject before incrementing"
    );

    database.cleanup().await
}

#[tokio::test]
async fn concurrent_first_coverage_failures_preserve_both_attempts() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_job = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-concurrency:first"),
    )
    .await?;
    let second_job = create_generation_scoped_backfill_job(
        database.pool(),
        &backfill_job_create("coverage-attempt-concurrency:second"),
    )
    .await?;
    set_backfill_job_attempt_count(database.pool(), first_job.job.backfill_job_id, 1).await?;
    set_backfill_job_attempt_count(database.pool(), second_job.job.backfill_job_id, 1).await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };

    // Hold INSERT's ROW EXCLUSIVE table lock out while allowing both callers
    // to observe the initially absent key. Releasing this lock makes their
    // ON CONFLICT paths race deterministically.
    let mut blocker = database.pool().begin().await?;
    sqlx::query("LOCK TABLE normalized_replay_coverage_recovery_failures IN SHARE MODE")
        .execute(&mut *blocker)
        .await?;
    let first_pool = database.dedicated_single_connection_pool().await?;
    let second_pool = database.dedicated_single_connection_pool().await?;
    let first_key = key.clone();
    let second_key = key.clone();
    let first = tokio::spawn(async move {
        crate::record_coverage_recovery_attempt_failure(
            &first_pool,
            &first_key,
            0,
            first_job.job.backfill_job_id,
            1,
            32,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(300),
            "first concurrent provider failure",
            "coverage recovery attempt budget exhausted",
            json!({"cause": "provider_mismatch"}),
        )
        .await
    });
    let second = tokio::spawn(async move {
        crate::record_coverage_recovery_attempt_failure(
            &second_pool,
            &second_key,
            0,
            second_job.job.backfill_job_id,
            1,
            32,
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(300),
            "second concurrent provider failure",
            "coverage recovery attempt budget exhausted",
            json!({"cause": "provider_mismatch"}),
        )
        .await
    });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let blocked_writers = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND (
                  query LIKE '%INSERT INTO normalized_replay_coverage_recovery_failures%'
                  OR query LIKE '%pg_advisory_xact_lock(hashtextextended%'
              )
              AND wait_event_type = 'Lock'
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        if blocked_writers == 2 {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "concurrent failure writers did not both reach the blocked insert"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    blocker.commit().await?;
    first.await.context("first failure task panicked")??;
    second.await.context("second failure task panicked")??;

    let recorded = crate::load_coverage_recovery_failure(database.pool(), &key)
        .await?
        .context("concurrent failure attempts were not recorded")?;
    assert_eq!(
        recorded.attempt_count, 2,
        "the stable per-window budget must count both distinct job attempts"
    );

    database.cleanup().await
}

#[tokio::test]
async fn terminal_failure_writes_share_the_rearm_key_lock() -> Result<()> {
    let database = TestDatabase::new().await?;
    let writer_pool = database.dedicated_single_connection_pool().await?;
    let key = crate::CoverageRecoveryFailureKey {
        deployment_profile: "mainnet".to_owned(),
        chain_id: "eth-mainnet".to_owned(),
        raw_log_retention_generation: 0,
        source_family: "ens_v1_registry_l1".to_owned(),
        emitting_address: "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
        required_from_block: 100,
        required_to_block: 120,
    };
    let lock_identity = serde_json::to_string(&(
        &key.deployment_profile,
        &key.chain_id,
        key.raw_log_retention_generation,
        &key.source_family,
        &key.emitting_address,
        key.required_from_block,
        key.required_to_block,
    ))?;
    let mut blocker = database.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(lock_identity)
        .execute(&mut *blocker)
        .await?;

    let writer_key = key.clone();
    let writer = tokio::spawn(async move {
        crate::record_coverage_recovery_terminal_failure(
            &writer_pool,
            &writer_key,
            0,
            None,
            0,
            "terminal preparation failure",
            json!({"cause": "test"}),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let serialized = !writer.is_finished();
    blocker.rollback().await?;
    writer.await.context("terminal failure writer panicked")??;

    database.cleanup().await?;
    anyhow::ensure!(
        serialized,
        "terminal failure writer bypassed the natural-key lock used by operator re-arm"
    );
    Ok(())
}

#[tokio::test]
async fn recovery_jobs_record_query_counts_and_fenced_stored_verification() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = backfill_job_create("coverage-recovery-accounting");
    request.ranges.clear();
    let created = create_generation_scoped_backfill_job(database.pool(), &request).await?;

    record_backfill_job_projected_minimum_provider_queries(
        database.pool(),
        created.job.backfill_job_id,
        7,
    )
    .await?;
    record_backfill_job_projected_minimum_provider_queries(
        database.pool(),
        created.job.backfill_job_id,
        3,
    )
    .await?;
    add_backfill_job_actual_provider_queries(database.pool(), created.job.backfill_job_id, 2)
        .await?;
    add_backfill_job_actual_provider_queries(database.pool(), created.job.backfill_job_id, 4)
        .await?;

    let raw_log_input_revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = $1",
    )
    .bind(&created.job.chain_id)
    .fetch_one(database.pool())
    .await?;
    let verification = BackfillStoredVerification {
        raw_log_input_revision,
        verified_from_block: 100,
        verified_to_block: 120,
        selected_log_count: 42,
        selected_log_digest: "0123456789abcdef0123456789abcdef".to_owned(),
    };
    let mut connection = database.pool().acquire().await?;
    record_backfill_job_stored_verification(
        &mut connection,
        created.job.backfill_job_id,
        created.job.raw_log_retention_generation,
        &verification,
    )
    .await?;
    drop(connection);

    assert_eq!(
        sqlx::query_as::<
            _,
            (
                i64,
                i64,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                Option<String>
            ),
        >(
            r#"
            SELECT
                projected_minimum_provider_query_count,
                actual_provider_query_count,
                stored_verification_raw_log_input_revision,
                stored_verification_from_block,
                stored_verification_to_block,
                stored_verification_log_count,
                stored_verification_digest
            FROM backfill_jobs
            WHERE backfill_job_id = $1
            "#,
        )
        .bind(created.job.backfill_job_id)
        .fetch_one(database.pool())
        .await?,
        (
            7,
            6,
            Some(verification.raw_log_input_revision),
            Some(verification.verified_from_block),
            Some(verification.verified_to_block),
            Some(verification.selected_log_count),
            Some(verification.selected_log_digest),
        )
    );
    assert!(
        backfill_job_stored_verification_is_current(
            database.pool(),
            created.job.backfill_job_id,
            &created.job.chain_id,
            verification.verified_from_block,
            verification.verified_to_block,
        )
        .await?
    );
    let changed_revision = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET revision = revision + 1
        WHERE chain_id = $1
        RETURNING revision
        "#,
    )
    .bind(&created.job.chain_id)
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id,
            block_hash,
            block_number,
            revision
        )
        VALUES ($1, '0xchanged-after-verification', $2, $3)
        "#,
    )
    .bind(&created.job.chain_id)
    .bind(verification.verified_from_block)
    .bind(changed_revision)
    .execute(database.pool())
    .await?;
    assert!(
        !backfill_job_stored_verification_is_current(
            database.pool(),
            created.job.backfill_job_id,
            &created.job.chain_id,
            verification.verified_from_block,
            verification.verified_to_block,
        )
        .await?
    );

    database.cleanup().await
}

#[tokio::test]
async fn stale_claim_sweep_releases_old_heartbeats_but_preserves_fresh_claims() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut stale_request = backfill_job_create("stale-claim-recovery");
    stale_request.ranges.clear();
    let stale = create_backfill_job(database.pool(), &stale_request).await?;
    let stale_range = reserve_backfill_range(
        database.pool(),
        stale.job.backfill_job_id,
        "dead-worker",
        "dead-lease",
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() + 86_400)?,
    )
    .await?
    .expect("stale fixture range must reserve");
    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET status = 'running', updated_at = now() - INTERVAL '2 hours'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(stale_range.backfill_range_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "UPDATE backfill_jobs SET status = 'running', updated_at = now() - INTERVAL '2 hours' WHERE backfill_job_id = $1",
    )
    .bind(stale.job.backfill_job_id)
    .execute(database.pool())
    .await?;

    let mut fresh_request = backfill_job_create("fresh-claim-preserved");
    fresh_request.ranges.clear();
    let fresh = create_backfill_job(database.pool(), &fresh_request).await?;
    reserve_backfill_range(
        database.pool(),
        fresh.job.backfill_job_id,
        "live-worker",
        "live-lease",
        lease_deadline(),
    )
    .await?
    .expect("fresh fixture range must reserve");

    let swept = sweep_stale_backfill_claims(
        database.pool(),
        &stale_request.chain_id,
        OffsetDateTime::from_unix_timestamp(OffsetDateTime::now_utc().unix_timestamp() - 3_600)?,
    )
    .await?;
    assert_eq!(swept, vec![stale.job.backfill_job_id]);
    let reclaimed = reserve_backfill_range(
        database.pool(),
        stale.job.backfill_job_id,
        "replacement-worker",
        "replacement-lease",
        lease_deadline(),
    )
    .await?
    .expect("swept stale range must be ordinarily reclaimable");
    assert_eq!(reclaimed.backfill_range_id, stale_range.backfill_range_id);
    assert_eq!(reclaimed.attempt_count, 2);

    let fresh_range = load_backfill_ranges(database.pool(), fresh.job.backfill_job_id)
        .await?
        .pop()
        .expect("fresh range must exist");
    assert_eq!(fresh_range.status, BackfillLifecycleStatus::Reserved);
    assert_eq!(fresh_range.lease_token.as_deref(), Some("live-lease"));

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_retention_migration_isolates_a_legacy_job_only_chain() -> Result<()> {
    const RETENTION_MIGRATION: i64 = 20260714120000;
    let database = TestDatabase::new_before_migration(Some(RETENTION_MIGRATION)).await?;
    let chain = "legacy-job-only-chain";
    sqlx::query(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key
        )
        VALUES ('legacy', $1, '{}'::JSONB, 'logs', 0, 10, 'legacy-job-only')
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to insert the pre-migration job-only chain")?;

    let migrator = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|migration| migration.version <= RETENTION_MIGRATION)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    migrator
        .run(database.pool())
        .await
        .context("failed to apply the raw-log retention migration")?;

    let state = sqlx::query_as::<_, (i64, bool, Option<i64>)>(
        r#"
        SELECT retention_generation, retained_history_complete, proven_through_block
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        state,
        (1, false, None),
        "a legacy job-only chain must not share generation zero with pre-migration jobs"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT raw_log_retention_generation FROM backfill_jobs WHERE chain_id = $1"
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_accepts_legacy_full_whole_active_with_compact_hash() -> Result<()> {
    let database = TestDatabase::new().await?;
    let selected_targets = vec![
        json!({
            "source_family": "basenames_base_registry",
            "contract_instance_id": "00000000-0000-0000-0000-000000000001",
            "address": "0x0000000000000000000000000000000000000001",
            "effective_from_block": 100,
            "effective_to_block": 120
        }),
        json!({
            "source_family": "basenames_base_registry",
            "contract_instance_id": "00000000-0000-0000-0000-000000000002",
            "address": "0x0000000000000000000000000000000000000002",
            "effective_from_block": 100,
            "effective_to_block": 120
        }),
    ];
    let legacy_full_source_identity_hash =
        "keccak256:0x1111111111111111111111111111111111111111111111111111111111111111";
    let mut request = backfill_job_create("job-create-compact-source-identity");
    request.source_identity = json!({
        "selector_kind": "whole_active_watched_chain",
        "source_family": null,
        "requested_watched_targets": [],
        "selected_targets": selected_targets.clone(),
        "backfill_provider": "coinbase_cdp_sql",
        "scan_mode": "coinbase_sql_hash_pinned_logs_v1",
        "coinbase_sql_plan_version": "base_logs_v2",
        "validation_provider_required": true,
        "coinbase_sql_validation_mode": "sample",
        "topic_filtering": "manifest_abi_topic0_union_v1",
        "coinbase_sql_topic_plan": {
            "topic0s_by_source_family": {
                "basenames_base_registry": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]
            },
            "event_signatures_by_source_family": {
                "basenames_base_registry": ["NewOwner(bytes32,bytes32,address)"]
            },
            "source_families_without_topics": []
        },
        "source_identity_hash": legacy_full_source_identity_hash,
    });

    let created = create_backfill_job(database.pool(), &request).await?;
    let selected_targets = request
        .source_identity
        .get("selected_targets")
        .and_then(serde_json::Value::as_array)
        .expect("test source identity has selected_targets");
    let compact_source_identity = |selected_targets: &[serde_json::Value], hash: &str| {
        json!({
            "selector_kind": "whole_active_watched_chain",
            "source_family": null,
            "requested_watched_targets": [],
            "selected_target_count": selected_targets.len(),
            "selected_targets_digest_algorithm": "keccak256",
            "selected_targets_digest": validate::selected_targets_digest(selected_targets),
            "selected_targets_sample": {
                "first": selected_targets.first(),
                "last": selected_targets.last(),
            },
            "source_identity_payload_format": "selected_targets_digest_v1",
            "backfill_provider": "coinbase_cdp_sql",
            "scan_mode": "coinbase_sql_hash_pinned_logs_v1",
            "coinbase_sql_plan_version": "base_logs_v2",
            "validation_provider_required": true,
            "coinbase_sql_validation_mode": "sample",
            "topic_filtering": "manifest_abi_topic0_union_v1",
            "coinbase_sql_topic_plan": {
                "topic0s_by_source_family": {
                    "basenames_base_registry": ["0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"]
                },
                "event_signatures_by_source_family": {
                    "basenames_base_registry": ["NewOwner(bytes32,bytes32,address)"]
                },
                "source_families_without_topics": []
            },
            "source_identity_hash": hash,
        })
    };
    let mut compact = request.clone();
    compact.source_identity = compact_source_identity(
        selected_targets,
        "keccak256:0x2222222222222222222222222222222222222222222222222222222222222222",
    );

    let repeated = create_backfill_job(database.pool(), &compact).await?;

    assert_eq!(repeated.job.backfill_job_id, created.job.backfill_job_id);
    assert_eq!(
        repeated.job.source_identity, request.source_identity,
        "existing full source identity must be reused without rewriting"
    );

    let mut different_targets = selected_targets.to_vec();
    *different_targets[1]
        .get_mut("effective_to_block")
        .expect("test target has effective_to_block") = json!(121);
    let mut different_compact = compact.clone();
    different_compact.source_identity = compact_source_identity(
        &different_targets,
        "keccak256:0x3333333333333333333333333333333333333333333333333333333333333333",
    );
    let error = create_backfill_job(database.pool(), &different_compact)
        .await
        .expect_err("different compact target set must not reuse legacy full job");
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    let mut provider_drift = compact.clone();
    provider_drift
        .source_identity
        .as_object_mut()
        .expect("compact source identity is an object")
        .insert("coinbase_sql_validation_mode".to_owned(), json!("full"));
    let error = create_backfill_job(database.pool(), &provider_drift)
        .await
        .expect_err(
            "same target set with changed Coinbase SQL fields must not reuse legacy full job",
        );
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    let mut missing_sample = compact;
    missing_sample
        .source_identity
        .as_object_mut()
        .expect("compact source identity is an object")
        .remove("selected_targets_sample");
    let error = create_backfill_job(database.pool(), &missing_sample)
        .await
        .expect_err(
            "compact identity without selected_targets_sample must not reuse legacy full job",
        );
    assert!(
        error
            .to_string()
            .contains("does not match requested immutable job identity"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_reservation_is_idempotent_and_reclaims_expired_leases() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-reservation-idempotent"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-a",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    assert_eq!(reserved.status, BackfillLifecycleStatus::Reserved);
    assert_eq!(reserved.lease_token.as_deref(), Some("lease-a"));
    assert_eq!(reserved.attempt_count, 1);

    let duplicate = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-a",
        lease_deadline(),
    )
    .await?
    .expect("duplicate lease must return the same reservation");
    assert_eq!(duplicate.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(duplicate.attempt_count, 1);

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET lease_expires_at = now() - interval '1 second'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let reclaimed = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-b",
        "lease-b",
        lease_deadline(),
    )
    .await?
    .expect("expired lease must be reclaimable");
    assert_eq!(reclaimed.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(reclaimed.lease_token.as_deref(), Some("lease-b"));
    assert_eq!(reclaimed.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(reclaimed.attempt_count, 2);
    let reclaimed_deadline = reclaimed
        .lease_expires_at
        .expect("reclaimed range must hold worker-b's lease deadline");

    let stale_advance =
        advance_backfill_range(database.pool(), reserved.backfill_range_id, "lease-a", 105)
            .await
            .expect_err("stale worker-a token must not advance or heartbeat after worker-b steals");
    assert!(
        stale_advance
            .to_string()
            .contains("not held by lease token"),
        "unexpected error: {stale_advance:#}"
    );
    let after_stale_advance = load_backfill_ranges(database.pool(), created.job.backfill_job_id)
        .await?
        .into_iter()
        .find(|range| range.backfill_range_id == reclaimed.backfill_range_id)
        .expect("reclaimed range must still exist after stale advance");
    assert_eq!(after_stale_advance.lease_token.as_deref(), Some("lease-b"));
    assert_eq!(after_stale_advance.lease_owner.as_deref(), Some("worker-b"));
    assert_eq!(
        after_stale_advance.lease_expires_at,
        Some(reclaimed_deadline)
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_range_advance_refreshes_active_lease_deadline() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-refreshes-lease"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-refresh",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            updated_at = now() - interval '295 seconds',
            lease_expires_at = now() + interval '5 seconds'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let advanced = advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-refresh",
        105,
    )
    .await?;
    let refreshed_lease = advanced
        .lease_expires_at
        .expect("running range must retain an active lease deadline");
    let minimum_refresh_deadline = OffsetDateTime::now_utc()
        .unix_timestamp()
        .checked_add(240)
        .context("minimum lease refresh timestamp overflowed")?;
    assert!(
        refreshed_lease.unix_timestamp() >= minimum_refresh_deadline,
        "advance must extend the active lease; got {refreshed_lease}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn reservation_finalizes_running_job_when_all_ranges_already_completed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-reservation-finalizes-drained-running-job"),
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            checkpoint_block_number = range_end_block_number,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            completed_at = now(),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(created.job.backfill_job_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'running'::backfill_lifecycle_status,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    )
    .bind(created.job.backfill_job_id)
    .execute(database.pool())
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-finalizer",
        "lease-finalizer",
        lease_deadline(),
    )
    .await?;
    assert!(reserved.is_none());
    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert!(
        job.completed_at.is_some(),
        "reservation should complete the already-drained running job"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_range_advance_rejects_expired_lease_token() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-rejects-expired-lease"),
    )
    .await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-expired",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET lease_expires_at = now() - interval '1 second'
        WHERE backfill_range_id = $1
        "#,
    )
    .bind(reserved.backfill_range_id)
    .execute(database.pool())
    .await?;

    let error = advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-expired",
        105,
    )
    .await
    .expect_err("expired lease token must not advance or refresh a range");
    assert!(
        error.to_string().contains("lease expired"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_range_advance_and_completion_are_monotonic() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-advance-complete"),
    )
    .await?;

    let first = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-first",
        lease_deadline(),
    )
    .await?
    .expect("first range must be reservable");

    let advanced =
        advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 105)
            .await?;
    assert_eq!(advanced.status, BackfillLifecycleStatus::Running);
    assert_eq!(advanced.checkpoint_block_number, 105);

    let stale =
        advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 104)
            .await?;
    assert_eq!(stale.checkpoint_block_number, 105);

    let error = complete_backfill_range(database.pool(), first.backfill_range_id, "lease-first")
        .await
        .expect_err("range completion must require checkpoint at declared end");
    assert!(
        error
            .to_string()
            .contains("has not reached declared range end"),
        "unexpected error: {error:#}"
    );

    advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 109).await?;
    let completed_first =
        complete_backfill_range(database.pool(), first.backfill_range_id, "lease-first").await?;
    assert_eq!(completed_first.status, BackfillLifecycleStatus::Completed);
    assert_eq!(completed_first.lease_token, None);

    let second = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-second",
        lease_deadline(),
    )
    .await?
    .expect("second range must be reservable");
    assert_ne!(second.backfill_range_id, first.backfill_range_id);
    advance_backfill_range(
        database.pool(),
        second.backfill_range_id,
        "lease-second",
        120,
    )
    .await?;
    complete_backfill_range(database.pool(), second.backfill_range_id, "lease-second").await?;

    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert!(job.completed_at.is_some());

    database.cleanup().await
}

#[tokio::test]
async fn backfill_job_failure_records_metadata_without_rewinding_checkpoint() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = backfill_job_create("job-failure");
    request.ranges = Vec::new();
    let created = create_backfill_job(database.pool(), &request).await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-fail",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-fail",
        111,
    )
    .await?;

    let failed = fail_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-fail",
        "rpc timeout",
        json!({ "block": 112 }),
    )
    .await?;
    assert_eq!(failed.status, BackfillLifecycleStatus::Failed);
    assert_eq!(failed.checkpoint_block_number, 111);
    assert_eq!(failed.failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(failed.failure_metadata, json!({ "block": 112 }));

    let failed_job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(failed_job.status, BackfillLifecycleStatus::Failed);

    let retried = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-b",
        "lease-retry",
        lease_deadline(),
    )
    .await?
    .expect("failed range must be explicitly reservable");
    assert_eq!(retried.backfill_range_id, reserved.backfill_range_id);
    assert_eq!(retried.checkpoint_block_number, 111);
    assert_eq!(retried.status, BackfillLifecycleStatus::Reserved);
    assert_eq!(retried.failure_reason, None);
    assert_eq!(retried.failure_metadata, json!({}));

    database.cleanup().await
}

#[tokio::test]
async fn complete_backfill_job_preserves_failed_range_lifecycle_at_range_end() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = backfill_job_create("job-failed-complete-guard");
    request.ranges = Vec::new();
    let created = create_backfill_job(database.pool(), &request).await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-end-fail",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-end-fail",
        request.range_end_block_number,
    )
    .await?;

    let failure_metadata = json!({ "block": request.range_end_block_number, "attempt": 1 });
    let failed = fail_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-end-fail",
        "rpc timeout",
        failure_metadata.clone(),
    )
    .await?;
    assert_eq!(failed.status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        failed.checkpoint_block_number,
        request.range_end_block_number
    );

    let error = complete_backfill_job(database.pool(), created.job.backfill_job_id)
        .await
        .expect_err("job completion must not overwrite failed ranges at declared end");
    assert!(
        error.to_string().contains("failed ranges"),
        "unexpected error: {error:#}"
    );

    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must still exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Failed);
    assert_eq!(job.failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(job.failure_metadata, failure_metadata);
    assert!(job.completed_at.is_none());

    let ranges = load_backfill_ranges(database.pool(), created.job.backfill_job_id).await?;
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].status, BackfillLifecycleStatus::Failed);
    assert_eq!(
        ranges[0].checkpoint_block_number,
        request.range_end_block_number
    );
    assert_eq!(ranges[0].failure_reason.as_deref(), Some("rpc timeout"));
    assert_eq!(
        ranges[0].failure_metadata,
        json!({ "block": request.range_end_block_number, "attempt": 1 })
    );
    assert!(ranges[0].completed_at.is_none());

    database.cleanup().await
}

fn address_coverage_fact(
    source_family: &str,
    address: &str,
    covered_from_block: i64,
    covered_to_block: i64,
) -> BackfillCoverageFactWrite {
    BackfillCoverageFactWrite {
        source_family: source_family.to_owned(),
        scope: BackfillCoverageFactScope::Address,
        address: Some(address.to_owned()),
        covered_from_block,
        covered_to_block,
    }
}

async fn load_coverage_fact_rows(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<Vec<(String, String, String, Option<String>, i64, i64, String)>> {
    sqlx::query_as(
        r#"
        SELECT chain_id, source_family, scope, address, covered_from_block, covered_to_block, derivation
        FROM backfill_coverage_facts
        WHERE backfill_job_id = $1
        ORDER BY scope, source_family, address, covered_from_block, covered_to_block
        "#,
    )
    .bind(backfill_job_id)
    .fetch_all(pool)
    .await
    .context("failed to load coverage fact rows")
}

#[tokio::test]
async fn coverage_fact_writes_are_idempotent_and_validated() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-coverage-fact-writes"),
    )
    .await?;
    mark_backfill_job_completed_for_coverage_fact_test(
        database.pool(),
        created.job.backfill_job_id,
    )
    .await?;
    let facts = vec![
        address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            100,
            120,
        ),
        BackfillCoverageFactWrite {
            source_family: "ens_v1_resolver_l1".to_owned(),
            scope: BackfillCoverageFactScope::Family,
            address: None,
            covered_from_block: 100,
            covered_to_block: 120,
        },
    ];

    let mut conn = database.pool().acquire().await?;
    let inserted = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        &facts,
    )
    .await?;
    assert_eq!(inserted, 2);
    let repeated = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        &facts,
    )
    .await?;
    assert_eq!(repeated, 0);
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        2
    );
    assert_eq!(
        load_coverage_fact_rows(database.pool(), created.job.backfill_job_id).await?,
        vec![
            (
                "eth-mainnet".to_owned(),
                "ens_v1_registry_l1".to_owned(),
                "address".to_owned(),
                Some("0x0000000000000000000000000000000000000001".to_owned()),
                100,
                120,
                "legacy_full_payload_identity".to_owned(),
            ),
            (
                "eth-mainnet".to_owned(),
                "ens_v1_resolver_l1".to_owned(),
                "family".to_owned(),
                None,
                100,
                120,
                "legacy_full_payload_identity".to_owned(),
            ),
        ]
    );

    let mut distinct_interval = facts[0].clone();
    distinct_interval.covered_to_block = 119;
    let distinct_interval_inserted = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        std::slice::from_ref(&distinct_interval),
    )
    .await?;
    assert_eq!(
        distinct_interval_inserted, 1,
        "a same-start fact with a different end block is a distinct interval and must not be dropped"
    );
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        3
    );

    let mut missing_address = facts[0].clone();
    missing_address.address = None;
    let error = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        std::slice::from_ref(&missing_address),
    )
    .await
    .expect_err("address-scoped fact without an address must be rejected");
    assert!(
        error.to_string().contains("must carry an address"),
        "unexpected error: {error:#}"
    );

    let mut inverted_range = facts[0].clone();
    inverted_range.covered_from_block = 121;
    let error = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        std::slice::from_ref(&inverted_range),
    )
    .await
    .expect_err("inverted coverage interval must be rejected");
    assert!(
        error.to_string().contains("is after covered_to_block"),
        "unexpected error: {error:#}"
    );

    drop(conn);
    database.cleanup().await
}

#[tokio::test]
async fn coverage_fact_writes_require_a_completed_containing_job() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-coverage-authority"),
    )
    .await?;
    let mut conn = database.pool().acquire().await?;
    let contained = address_coverage_fact(
        "ens_v1_registry_l1",
        "0x0000000000000000000000000000000000000001",
        100,
        120,
    );

    let error = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        std::slice::from_ref(&contained),
    )
    .await
    .expect_err("a pending job must not authorize coverage facts");
    assert!(
        error.to_string().contains("is pending, not completed"),
        "unexpected error: {error:#}"
    );

    mark_backfill_job_completed_for_coverage_fact_test(
        database.pool(),
        created.job.backfill_job_id,
    )
    .await?;
    for outside in [
        address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            99,
            120,
        ),
        address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            100,
            121,
        ),
    ] {
        let error = write_backfill_coverage_facts(
            &mut conn,
            created.job.backfill_job_id,
            BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
            std::slice::from_ref(&outside),
        )
        .await
        .expect_err("a fact outside its job range must be rejected");
        assert!(
            error
                .to_string()
                .contains("is not contained by job range 100..=120"),
            "unexpected error: {error:#}"
        );
    }
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        0
    );

    drop(conn);
    database.cleanup().await
}

#[tokio::test]
async fn range_completion_records_coverage_facts_only_when_job_flips() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-coverage-on-completion"),
    )
    .await?;
    let coverage_facts = |job: &BackfillJob| {
        vec![address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            job.range_start_block_number,
            job.range_end_block_number,
        )]
        .into_iter()
    };

    let first = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-first",
        lease_deadline(),
    )
    .await?
    .expect("first range must be reservable");
    advance_backfill_range(database.pool(), first.backfill_range_id, "lease-first", 109).await?;
    complete_backfill_range_recording_coverage(
        database.pool(),
        first.backfill_range_id,
        "lease-first",
        coverage_facts,
    )
    .await?;
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        0,
        "facts must not be recorded before the job completes"
    );

    let second = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-second",
        lease_deadline(),
    )
    .await?
    .expect("second range must be reservable");
    advance_backfill_range(
        database.pool(),
        second.backfill_range_id,
        "lease-second",
        120,
    )
    .await?;
    complete_backfill_range_recording_coverage(
        database.pool(),
        second.backfill_range_id,
        "lease-second",
        coverage_facts,
    )
    .await?;

    let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
        .await?
        .expect("job must exist");
    assert_eq!(job.status, BackfillLifecycleStatus::Completed);
    assert_eq!(
        load_coverage_fact_rows(database.pool(), created.job.backfill_job_id).await?,
        vec![(
            "eth-mainnet".to_owned(),
            "ens_v1_registry_l1".to_owned(),
            "address".to_owned(),
            Some("0x0000000000000000000000000000000000000001".to_owned()),
            100,
            120,
            "job_completion".to_owned(),
        )]
    );

    let recompleted = complete_backfill_range_recording_coverage(
        database.pool(),
        second.backfill_range_id,
        "lease-second",
        coverage_facts,
    )
    .await?;
    assert_eq!(recompleted.status, BackfillLifecycleStatus::Completed);
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        1,
        "re-completion must not duplicate coverage facts"
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_fact_writes_chunk_below_the_bind_limit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fact_count = 40_000_usize;
    assert!(
        fact_count > super::coverage_facts::COVERAGE_FACT_INSERT_CHUNK_ROWS,
        "fixture must span multiple insert chunks"
    );
    let mut request = backfill_job_create("job-coverage-chunking");
    request.ranges = Vec::new();
    let created = create_backfill_job(database.pool(), &request).await?;

    let reserved = reserve_backfill_range(
        database.pool(),
        created.job.backfill_job_id,
        "worker-a",
        "lease-chunk",
        lease_deadline(),
    )
    .await?
    .expect("range must be reservable");
    advance_backfill_range(
        database.pool(),
        reserved.backfill_range_id,
        "lease-chunk",
        120,
    )
    .await?;
    complete_backfill_range_recording_coverage(
        database.pool(),
        reserved.backfill_range_id,
        "lease-chunk",
        |job: &BackfillJob| {
            let (covered_from_block, covered_to_block) =
                (job.range_start_block_number, job.range_end_block_number);
            (0..fact_count).map(move |index| {
                address_coverage_fact(
                    "ens_v1_wrapper_l1",
                    &format!("0x{index:040x}"),
                    covered_from_block,
                    covered_to_block,
                )
            })
        },
    )
    .await?;
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        fact_count as u64
    );

    let slice_facts = (0..fact_count)
        .map(|index| {
            address_coverage_fact(
                "ens_v1_wrapper_l1",
                &format!("0x{index:040x}"),
                request.range_start_block_number,
                request.range_end_block_number,
            )
        })
        .collect::<Vec<_>>();
    let mut conn = database.pool().acquire().await?;
    let reinserted = write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::JobCompletion,
        &slice_facts,
    )
    .await?;
    drop(conn);
    assert_eq!(
        reinserted, 0,
        "re-deriving the same facts must be a chunked no-op"
    );

    database.cleanup().await
}

#[tokio::test]
async fn deleting_a_backfill_job_cascades_its_coverage_facts() -> Result<()> {
    let database = TestDatabase::new().await?;
    let created = create_backfill_job(
        database.pool(),
        &backfill_job_create("job-coverage-cascade"),
    )
    .await?;
    mark_backfill_job_completed_for_coverage_fact_test(
        database.pool(),
        created.job.backfill_job_id,
    )
    .await?;
    let mut conn = database.pool().acquire().await?;
    write_backfill_coverage_facts(
        &mut conn,
        created.job.backfill_job_id,
        BackfillCoverageFactDerivation::JobCompletion,
        &[address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            100,
            120,
        )],
    )
    .await?;
    drop(conn);
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        1
    );

    sqlx::query("DELETE FROM backfill_jobs WHERE backfill_job_id = $1")
        .bind(created.job.backfill_job_id)
        .execute(database.pool())
        .await
        .context("failed to delete backfill job for cascade test")?;
    assert_eq!(
        load_backfill_coverage_fact_counts(database.pool(), created.job.backfill_job_id).await?,
        0,
        "coverage facts must cascade with their job"
    );

    database.cleanup().await
}

#[tokio::test]
async fn concurrent_final_range_completions_flip_the_job_and_record_facts_once() -> Result<()> {
    let database = TestDatabase::new().await?;
    // Each completion gets its own pre-warmed single-connection pool so the
    // two transactions genuinely overlap; on a shared pool the second
    // completion sits in pool acquisition for the first transaction's whole
    // lifetime, which serializes them and lets pre-fix code pass.
    let first_pool = database.dedicated_single_connection_pool().await?;
    let second_pool = database.dedicated_single_connection_pool().await?;
    let coverage_facts = |job: &BackfillJob| {
        vec![address_coverage_fact(
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000001",
            job.range_start_block_number,
            job.range_end_block_number,
        )]
        .into_iter()
    };

    // One interleaving is not a proof; repeat the scenario so a lost flip
    // cannot slip through on a lucky schedule.
    for iteration in 0..4 {
        let created = create_backfill_job(
            database.pool(),
            &backfill_job_create(&format!("job-concurrent-final-ranges-{iteration}")),
        )
        .await?;
        let first_lease = format!("lease-a-{iteration}");
        let second_lease = format!("lease-b-{iteration}");
        let first = reserve_backfill_range(
            database.pool(),
            created.job.backfill_job_id,
            "worker-a",
            &first_lease,
            lease_deadline(),
        )
        .await?
        .expect("first range must be reservable");
        let second = reserve_backfill_range(
            database.pool(),
            created.job.backfill_job_id,
            "worker-b",
            &second_lease,
            lease_deadline(),
        )
        .await?
        .expect("second range must be reservable");
        advance_backfill_range(database.pool(), first.backfill_range_id, &first_lease, 109).await?;
        advance_backfill_range(
            database.pool(),
            second.backfill_range_id,
            &second_lease,
            120,
        )
        .await?;
        drop(first_pool.acquire().await?);
        drop(second_pool.acquire().await?);

        // The final two ranges complete concurrently: the job row lock must
        // serialize them so exactly one transaction observes zero incomplete
        // ranges and flips the job with its coverage facts. Without the
        // lock, neither flips and the job is later completed fact-less by
        // the reservation path.
        let (first_result, second_result) = tokio::join!(
            complete_backfill_range_recording_coverage(
                &first_pool,
                first.backfill_range_id,
                &first_lease,
                coverage_facts,
            ),
            complete_backfill_range_recording_coverage(
                &second_pool,
                second.backfill_range_id,
                &second_lease,
                coverage_facts,
            ),
        );
        assert_eq!(
            first_result?.status,
            BackfillLifecycleStatus::Completed,
            "first range completion must succeed (iteration {iteration})"
        );
        assert_eq!(
            second_result?.status,
            BackfillLifecycleStatus::Completed,
            "second range completion must succeed (iteration {iteration})"
        );

        let job = load_backfill_job(database.pool(), created.job.backfill_job_id)
            .await?
            .expect("job must exist");
        assert_eq!(
            job.status,
            BackfillLifecycleStatus::Completed,
            "the last concurrent range completion must flip the job (iteration {iteration})"
        );
        assert_eq!(
            load_coverage_fact_rows(database.pool(), created.job.backfill_job_id).await?,
            vec![(
                "eth-mainnet".to_owned(),
                "ens_v1_registry_l1".to_owned(),
                "address".to_owned(),
                Some("0x0000000000000000000000000000000000000001".to_owned()),
                100,
                120,
                "job_completion".to_owned(),
            )],
            "coverage facts must be recorded exactly once (iteration {iteration})"
        );
    }

    first_pool.close().await;
    second_pool.close().await;
    database.cleanup().await
}
