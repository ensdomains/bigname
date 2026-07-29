use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::time::Duration;

use super::*;
use crate::MIGRATOR;

const CHAIN: &str = "test-chain";
const FAMILY: &str = "test-family";
const ADDRESS: &str = "0x0000000000000000000000000000000000000001";

fn requirement(from: i64, to: i64) -> BackfillTopicCoverageRequirement {
    BackfillTopicCoverageRequirement {
        source_family: FAMILY.to_owned(),
        address: ADDRESS.to_owned(),
        required_from_block: from,
        required_to_block: to,
    }
}

fn topic(index: u8) -> String {
    format!("0x{index:064x}")
}

async fn database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &MIGRATOR,
        "failed to migrate full-closure coverage test",
    )
    .await
}

async fn seed_authority(
    pool: &PgPool,
    raw_revision: i64,
    retention_generation: i64,
    evidence_floor: i64,
    admission_epoch: i64,
) -> Result<()> {
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
            proven_through_block,
            block_revision_evidence_floor
        )
        VALUES ($1, $2, $3, false, now(), NULL, NULL, NULL, $4)
        ON CONFLICT (chain_id) DO UPDATE
        SET revision = EXCLUDED.revision,
            retention_generation = EXCLUDED.retention_generation,
            retained_history_complete = false,
            incomplete_since = now(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL,
            block_revision_evidence_floor = EXCLUDED.block_revision_evidence_floor
        "#,
    )
    .bind(CHAIN)
    .bind(raw_revision)
    .bind(retention_generation)
    .bind(evidence_floor)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, $2)
        ON CONFLICT (chain_id) DO UPDATE SET epoch = EXCLUDED.epoch
        "#,
    )
    .bind(CHAIN)
    .bind(admission_epoch)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_completed_job(
    pool: &PgPool,
    suffix: &str,
    retention_generation: i64,
    source_identity: Value,
    stored_verification_revision: Option<i64>,
    from: i64,
    to: i64,
) -> Result<i64> {
    let row = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status,
            completed_at,
            raw_log_retention_generation,
            stored_verification_raw_log_input_revision,
            stored_verification_from_block,
            stored_verification_to_block,
            stored_verification_log_count,
            stored_verification_digest
        )
        VALUES (
            'test',
            $1,
            $2,
            'test',
            $3,
            $4,
            $5,
            'completed'::backfill_lifecycle_status,
            now(),
            $6,
            $7,
            CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE $3 END,
            CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE $4 END,
            CASE WHEN $7::BIGINT IS NULL THEN NULL ELSE 0 END,
            CASE WHEN $7::BIGINT IS NULL
                 THEN NULL
                 ELSE '00000000000000000000000000000000'
            END
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(CHAIN)
    .bind(source_identity)
    .bind(from)
    .bind(to)
    .bind(format!("full-closure-rollup-{suffix}"))
    .bind(retention_generation)
    .bind(stored_verification_revision)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

async fn insert_fact(
    pool: &PgPool,
    job_id: i64,
    scope: &str,
    address: Option<&str>,
    from: i64,
    to: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'job_completion')
        "#,
    )
    .bind(job_id)
    .bind(CHAIN)
    .bind(FAMILY)
    .bind(scope)
    .bind(address)
    .bind(from)
    .bind(to)
    .execute(pool)
    .await?;
    Ok(())
}

async fn scan(
    pool: &PgPool,
    topics: &BTreeMap<String, Vec<String>>,
    requirements: &[BackfillTopicCoverageRequirement],
    retention_generation: i64,
    admission_epoch: i64,
) -> Result<FullClosureCoverageScanOutcome> {
    find_uncovered_full_closure_coverage(
        pool,
        CHAIN,
        topics,
        requirements,
        retention_generation,
        admission_epoch,
        20,
    )
    .await
}

async fn wait_for_coverage_advisory_lock(pool: &PgPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks held
                    JOIN pg_stat_activity activity USING (pid)
                    WHERE held.locktype = 'advisory'
                      AND held.granted
                      AND activity.datname = current_database()
                )
                "#,
            )
            .fetch_one(pool)
            .await?;
            if observed {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn wait_for_fact_truncate_lock_request(pool: &PgPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observed = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE relation = 'backfill_coverage_facts'::regclass
                      AND mode = 'AccessExclusiveLock'
                      AND database = (
                          SELECT oid
                          FROM pg_database
                          WHERE datname = current_database()
                      )
                )
                "#,
            )
            .fetch_one(pool)
            .await?;
            if observed {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn cached_rollup_merges_only_new_fact_intervals() -> Result<()> {
    let database = database("full_closure_rollup_append").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let first_job =
        insert_completed_job(database.pool(), "append-first", 1, json!({}), None, 1, 5).await?;
    insert_fact(database.pool(), first_job, "address", Some(ADDRESS), 1, 5).await?;

    let first = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 5)],
        1,
        0,
    )
    .await?;
    assert!(first.violations.is_empty());
    assert!(first.synchronization.full_rebuild);

    let cached = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 5)],
        1,
        0,
    )
    .await?;
    assert!(cached.violations.is_empty());
    assert!(!cached.synchronization.full_rebuild);
    assert_eq!(cached.synchronization.appended_fact_count, 0);
    assert_eq!(cached.synchronization.rebuilt_key_count, 0);

    let second_job =
        insert_completed_job(database.pool(), "append-second", 1, json!({}), None, 6, 10).await?;
    insert_fact(database.pool(), second_job, "address", Some(ADDRESS), 6, 10).await?;
    let extended = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(extended.violations.is_empty());
    assert!(!extended.synchronization.full_rebuild);
    assert_eq!(extended.synchronization.appended_fact_count, 1);
    assert_eq!(extended.synchronization.rebuilt_key_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn facts_before_first_state_do_not_accumulate_journal_rows() -> Result<()> {
    let database = database("full_closure_rollup_prestate_journal").await?;
    seed_authority(database.pool(), 0, 0, 0, 0).await?;
    let job = insert_completed_job(database.pool(), "prestate", 0, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;

    let journal_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM full_closure_coverage_input_changes WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(journal_count, 0);
    assert_eq!(
        load_full_closure_coverage_input_revision(database.pool(), CHAIN).await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn pre_migration_facts_seed_the_first_lazy_aggregate() -> Result<()> {
    let database = database("full_closure_rollup_upgrade").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let job = insert_completed_job(database.pool(), "upgrade", 1, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;

    // This is the state immediately after the additive migration is
    // pre-applied to a database whose coverage facts predate its triggers.
    sqlx::query("DELETE FROM full_closure_coverage_input_changes WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;
    sqlx::query("DELETE FROM full_closure_coverage_input_revisions WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;

    let upgraded = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(upgraded.synchronization.full_rebuild);
    assert_eq!(upgraded.synchronization.coverage_input_revision, 0);
    assert!(upgraded.violations.is_empty());

    let resumed = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(!resumed.synchronization.full_rebuild);
    assert!(resumed.violations.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn fact_retirement_rebuilds_only_its_key_and_removes_authority() -> Result<()> {
    let database = database("full_closure_rollup_retirement").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let job =
        insert_completed_job(database.pool(), "retirement", 1, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;
    assert!(
        scan(
            database.pool(),
            &BTreeMap::new(),
            &[requirement(1, 10)],
            1,
            0,
        )
        .await?
        .violations
        .is_empty()
    );

    sqlx::query("DELETE FROM backfill_jobs WHERE backfill_job_id = $1")
        .bind(job)
        .execute(database.pool())
        .await?;
    let retired = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert_eq!(retired.violations, vec![requirement(1, 10)]);
    assert!(!retired.synchronization.full_rebuild);
    assert_eq!(retired.synchronization.rebuilt_key_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn raw_revision_rebuilds_only_overlapping_stored_verification() -> Result<()> {
    let database = database("full_closure_rollup_raw_revision").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let old_job =
        insert_completed_job(database.pool(), "raw-old", 1, json!({}), Some(0), 1, 10).await?;
    insert_fact(database.pool(), old_job, "address", Some(ADDRESS), 1, 10).await?;
    assert!(
        scan(
            database.pool(),
            &BTreeMap::new(),
            &[requirement(1, 10)],
            1,
            0,
        )
        .await?
        .violations
        .is_empty()
    );

    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id, block_hash, block_number, revision
        )
        VALUES ($1, $2, 5, 1)
        "#,
    )
    .bind(CHAIN)
    .bind(format!("0x{:064x}", 1))
    .execute(database.pool())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 1 WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;

    let invalidated = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert_eq!(invalidated.violations, vec![requirement(1, 10)]);
    assert_eq!(invalidated.synchronization.rebuilt_key_count, 1);

    let fresh_job =
        insert_completed_job(database.pool(), "raw-fresh", 1, json!({}), Some(1), 1, 10).await?;
    insert_fact(database.pool(), fresh_job, "address", Some(ADDRESS), 1, 10).await?;
    let recovered = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(recovered.violations.is_empty());
    assert_eq!(recovered.synchronization.appended_fact_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn job_authority_and_topic_changes_invalidate_stale_identity() -> Result<()> {
    let database = database("full_closure_rollup_topics").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let old_topic = topic(1);
    let new_topic = topic(2);
    let job = insert_completed_job(
        database.pool(),
        "topics",
        1,
        json!({"topic0s_by_source_family": {(FAMILY): [old_topic.clone()]}}),
        None,
        1,
        10,
    )
    .await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;
    let old_topics = BTreeMap::from([(FAMILY.to_owned(), vec![old_topic.clone()])]);
    assert!(
        scan(database.pool(), &old_topics, &[requirement(1, 10)], 1, 0)
            .await?
            .violations
            .is_empty()
    );

    sqlx::query("UPDATE backfill_jobs SET source_identity = $2 WHERE backfill_job_id = $1")
        .bind(job)
        .bind(json!({
            "topic0s_by_source_family": {(FAMILY): [new_topic.clone()]}
        }))
        .execute(database.pool())
        .await?;
    let changed_identity = scan(database.pool(), &old_topics, &[requirement(1, 10)], 1, 0).await?;
    assert!(!changed_identity.synchronization.full_rebuild);
    assert_eq!(changed_identity.synchronization.rebuilt_key_count, 1);
    assert_eq!(changed_identity.violations, vec![requirement(1, 10)]);

    let new_topics = BTreeMap::from([(FAMILY.to_owned(), vec![new_topic])]);
    let changed = scan(database.pool(), &new_topics, &[requirement(1, 10)], 1, 0).await?;
    assert!(changed.synchronization.full_rebuild);
    assert!(changed.violations.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn generation_and_admission_epoch_changes_force_cold_rebuilds() -> Result<()> {
    let database = database("full_closure_rollup_authority").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let job = insert_completed_job(database.pool(), "authority", 1, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;
    assert!(
        scan(
            database.pool(),
            &BTreeMap::new(),
            &[requirement(1, 10)],
            1,
            0,
        )
        .await?
        .violations
        .is_empty()
    );

    seed_authority(database.pool(), 0, 2, 0, 1).await?;
    let changed = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        2,
        1,
    )
    .await?;
    assert!(changed.synchronization.full_rebuild);
    assert_eq!(changed.violations, vec![requirement(1, 10)]);

    database.cleanup().await
}

#[tokio::test]
async fn retention_truncate_rebuilds_without_cross_generation_revision_witness() -> Result<()> {
    let database = database("full_closure_rollup_retention_truncate").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let old_job =
        insert_completed_job(database.pool(), "truncate-old", 1, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), old_job, "address", Some(ADDRESS), 1, 10).await?;
    assert!(
        scan(
            database.pool(),
            &BTreeMap::new(),
            &[requirement(1, 10)],
            1,
            0,
        )
        .await?
        .violations
        .is_empty()
    );

    sqlx::query("TRUNCATE raw_logs")
        .execute(database.pool())
        .await?;
    let current = crate::load_raw_log_staging_input_version(database.pool(), CHAIN).await?;
    assert_eq!(current.retention_generation, 2);
    assert_eq!(current.revision, 1);

    let current_job = insert_completed_job(
        database.pool(),
        "truncate-current",
        2,
        json!({}),
        None,
        1,
        10,
    )
    .await?;
    insert_fact(
        database.pool(),
        current_job,
        "address",
        Some(ADDRESS),
        1,
        10,
    )
    .await?;
    let rebuilt = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        2,
        0,
    )
    .await?;
    assert!(rebuilt.synchronization.full_rebuild);
    assert!(rebuilt.violations.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn proof_and_fact_truncate_share_table_before_advisory_lock_order() -> Result<()> {
    let database = database("full_closure_rollup_truncate_lock_order").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let job =
        insert_completed_job(database.pool(), "truncate-lock", 1, json!({}), None, 1, 10).await?;
    insert_fact(database.pool(), job, "address", Some(ADDRESS), 1, 10).await?;
    assert!(
        scan(
            database.pool(),
            &BTreeMap::new(),
            &[requirement(1, 10)],
            1,
            0,
        )
        .await?
        .violations
        .is_empty()
    );
    sqlx::query("UPDATE discovery_admission_epochs SET epoch = 1 WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;

    let mut blocker = database.pool().begin().await?;
    sqlx::query("LOCK TABLE discovery_admission_epochs IN ACCESS EXCLUSIVE MODE")
        .execute(blocker.as_mut())
        .await?;

    let proof_pool = database.pool().clone();
    let proof = tokio::spawn(async move {
        scan(&proof_pool, &BTreeMap::new(), &[requirement(1, 10)], 1, 1).await
    });
    wait_for_coverage_advisory_lock(database.pool()).await?;

    let truncate_pool = database.pool().clone();
    let truncate = tokio::spawn(async move {
        sqlx::query("TRUNCATE backfill_coverage_facts")
            .execute(&truncate_pool)
            .await
    });
    wait_for_fact_truncate_lock_request(database.pool()).await?;
    blocker.commit().await?;

    tokio::time::timeout(Duration::from_secs(10), proof).await???;
    tokio::time::timeout(Duration::from_secs(10), truncate).await???;

    database.cleanup().await
}

#[tokio::test]
async fn journal_gap_rebuilds_from_facts_instead_of_reusing_state() -> Result<()> {
    let database = database("full_closure_rollup_journal_gap").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let first_job =
        insert_completed_job(database.pool(), "journal-first", 1, json!({}), None, 1, 5).await?;
    insert_fact(database.pool(), first_job, "address", Some(ADDRESS), 1, 5).await?;
    scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 5)],
        1,
        0,
    )
    .await?;

    let second_job =
        insert_completed_job(database.pool(), "journal-second", 1, json!({}), None, 6, 10).await?;
    insert_fact(database.pool(), second_job, "address", Some(ADDRESS), 6, 10).await?;
    sqlx::query(
        r#"
        DELETE FROM full_closure_coverage_input_changes
        WHERE chain_id = $1
        "#,
    )
    .bind(CHAIN)
    .execute(database.pool())
    .await?;

    let rebuilt = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(rebuilt.synchronization.full_rebuild);
    assert!(rebuilt.violations.is_empty());

    database.cleanup().await
}

#[tokio::test]
async fn missing_raw_revision_evidence_fails_closed() -> Result<()> {
    let database = database("full_closure_rollup_raw_gap").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_block_revisions (
            chain_id, block_hash, block_number, revision
        )
        VALUES ($1, $2, 5, 2)
        "#,
    )
    .bind(CHAIN)
    .bind(format!("0x{:064x}", 2))
    .execute(database.pool())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 2 WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(database.pool())
        .await?;

    let error = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await
    .expect_err("a raw revision gap must refuse closure");
    assert!(
        format!("{error:#}").contains("without per-block evidence for every intervening revision")
    );

    database.cleanup().await
}

#[tokio::test]
async fn family_and_address_aggregates_compose_gap_free_coverage() -> Result<()> {
    let database = database("full_closure_rollup_scope_union").await?;
    seed_authority(database.pool(), 0, 1, 0, 0).await?;
    let family_job =
        insert_completed_job(database.pool(), "family", 1, json!({}), None, 1, 5).await?;
    insert_fact(database.pool(), family_job, "family", None, 1, 5).await?;
    let address_job =
        insert_completed_job(database.pool(), "address", 1, json!({}), None, 6, 10).await?;
    insert_fact(
        database.pool(),
        address_job,
        "address",
        Some(ADDRESS),
        6,
        10,
    )
    .await?;

    let outcome = scan(
        database.pool(),
        &BTreeMap::new(),
        &[requirement(1, 10)],
        1,
        0,
    )
    .await?;
    assert!(outcome.violations.is_empty());

    database.cleanup().await
}
