use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use super::*;
use crate::{
    CanonicalityState, RawBlock, RawCodeHash, RawCodeHashCorrectionUpdate,
    apply_raw_code_hash_corrections, default_database_url, mark_raw_block_facts_range_orphaned,
    upsert_raw_blocks, upsert_raw_code_hashes,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

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
            .context("failed to parse database URL for resolver-profile queue tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        // PostgreSQL truncates identifiers to 63 bytes. Keep the distinguishing
        // process/sequence/time suffix comfortably inside that boundary so
        // parallel tests cannot collapse onto the same database name.
        let database_name = format!("bn_rp_queue_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for resolver-profile queue tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect resolver-profile queue test pool")?;
        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for resolver-profile queue tests")?;

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

fn raw_code_hash(
    address: &str,
    block_hash: &str,
    block_number: i64,
    code_hash: &str,
    canonicality_state: CanonicalityState,
) -> RawCodeHash {
    RawCodeHash {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        contract_address: address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 32,
        canonicality_state,
    }
}

fn raw_block(block_hash: &str, parent_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
            .expect("test timestamp must be valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

async fn pending_for(
    pool: &PgPool,
    contract_address: &str,
) -> Result<Option<ResolverProfileInputChange>> {
    Ok(load_pending_resolver_profile_input_changes(pool, 1_000)
        .await?
        .into_iter()
        .find(|change| change.contract_address == normalize_evm_address(contract_address)))
}

async fn wait_for_backend_lock(pool: &PgPool, backend_pid: i32) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT COALESCE(wait_event_type = 'Lock', FALSE)
                FROM pg_stat_activity
                WHERE pid = $1
                "#,
            )
            .bind(backend_pid)
            .fetch_optional(pool)
            .await?
            .unwrap_or(false);
            if waiting {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("concurrent raw-code writer did not reach its queue-row lock")??;
    Ok(())
}

#[tokio::test]
async fn queue_coalesces_effective_changes_and_fences_acknowledgement() -> Result<()> {
    let database = TestDatabase::new().await?;
    let pool = database.pool();
    let address = "0x00000000000000000000000000000000000000a1";

    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xa101",
            101,
            "0xaaaa",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    let first = pending_for(pool, address)
        .await?
        .context("first effective hash must enqueue work")?;
    assert_eq!(first.generation, 1);
    assert_eq!(first.previous_code_hash, None);
    assert_eq!(first.current_code_hash.as_deref(), Some("0xaaaa"));
    assert!(!first.force_reconciliation);
    assert!(
        acknowledge_resolver_profile_input_change(pool, "eth-mainnet", address, first.generation)
            .await?
    );

    // The small conflict path executes INSERT ... DO NOTHING followed by an
    // UPDATE. Promotion and a later same-hash observation do not change the
    // effective resolver-profile input and therefore must remain clean.
    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xa101",
            101,
            "0xaaaa",
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xa102",
            102,
            "0xaaaa",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    assert_eq!(pending_for(pool, address).await?, None);

    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xa103",
            103,
            "0xbbbb",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    let second = pending_for(pool, address)
        .await?
        .context("different effective hash must enqueue work")?;
    assert_eq!(second.generation, 2);
    assert_eq!(second.previous_code_hash.as_deref(), Some("0xaaaa"));
    assert_eq!(second.current_code_hash.as_deref(), Some("0xbbbb"));

    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xa104",
            104,
            "0xcccc",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    let coalesced = pending_for(pool, address)
        .await?
        .context("new generation must remain pending")?;
    assert_eq!(coalesced.generation, 3);
    assert_eq!(coalesced.previous_code_hash.as_deref(), Some("0xaaaa"));
    assert_eq!(coalesced.current_code_hash.as_deref(), Some("0xcccc"));
    assert!(
        !acknowledge_resolver_profile_input_change(pool, "eth-mainnet", address, second.generation)
            .await?
    );
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            address,
            coalesced.generation
        )
        .await?
    );

    let forced_count = enqueue_resolver_profile_reconciliations(
        pool,
        &[ResolverProfileReconciliationTarget {
            chain_id: "eth-mainnet".to_owned(),
            contract_address: address.to_uppercase(),
        }],
    )
    .await?;
    assert_eq!(forced_count, 1);
    let forced = pending_for(pool, address)
        .await?
        .context("explicit same-current kick must enqueue work")?;
    assert_eq!(forced.generation, 4);
    assert_eq!(forced.previous_code_hash.as_deref(), Some("0xcccc"));
    assert_eq!(forced.current_code_hash.as_deref(), Some("0xcccc"));
    assert!(forced.force_reconciliation);
    assert!(
        acknowledge_resolver_profile_input_change(pool, "eth-mainnet", address, forced.generation)
            .await?
    );

    // Simulate the duplicate effective notification PostgreSQL can expose to
    // statement triggers around INSERT ... ON CONFLICT DO UPDATE. The second
    // notification observes the same final hash and must not double-bump.
    let ordinary_transition = json!([{
        "chain_id": "eth-mainnet",
        "contract_address": address,
        "previous_code_hash": "0xcccc",
        "current_code_hash": "0xdddd"
    }]);
    let mut duplicate_transaction = pool.begin().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT record_resolver_profile_input_changes($1)")
            .bind(&ordinary_transition)
            .fetch_one(&mut *duplicate_transaction)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT record_resolver_profile_input_changes($1)")
            .bind(&ordinary_transition)
            .fetch_one(&mut *duplicate_transaction)
            .await?,
        0
    );
    duplicate_transaction.commit().await?;
    let deduplicated = pending_for(pool, address)
        .await?
        .context("first ordinary notification must remain pending")?;
    assert_eq!(deduplicated.generation, 5);

    // Match the profile loader's complete same-height ordering: canonicality
    // rank precedes raw row id, and row id breaks ties within one rank.
    let tie_address = "0x00000000000000000000000000000000000000e5";
    let lower_id = raw_code_hash(
        tie_address,
        "0xe501",
        500,
        "0xaaaa",
        CanonicalityState::Canonical,
    );
    let higher_id = raw_code_hash(
        tie_address,
        "0xe502",
        500,
        "0xbbbb",
        CanonicalityState::Canonical,
    );
    upsert_raw_code_hashes(pool, &[lower_id.clone(), higher_id]).await?;
    let id_tie = pending_for(pool, tie_address)
        .await?
        .context("same-rank higher raw row id must win")?;
    assert_eq!(id_tie.current_code_hash.as_deref(), Some("0xbbbb"));
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            tie_address,
            id_tie.generation
        )
        .await?
    );
    upsert_raw_code_hashes(
        pool,
        &[RawCodeHash {
            canonicality_state: CanonicalityState::Finalized,
            ..lower_id
        }],
    )
    .await?;
    let rank_tie = pending_for(pool, tie_address)
        .await?
        .context("finalized lower row id must outrank canonical higher row id")?;
    assert_eq!(rank_tie.previous_code_hash.as_deref(), Some("0xbbbb"));
    assert_eq!(rank_tie.current_code_hash.as_deref(), Some("0xaaaa"));

    // A losing tip orphan followed by recanonicalization returns to its
    // original hash. The dirty generation remains visible even though the
    // coalesced audit endpoints are equal, so callers perform the seeded/net
    // oscillation repair rather than silently acknowledging it.
    let reorg_address = "0x00000000000000000000000000000000000000b2";
    upsert_raw_blocks(
        pool,
        &[
            raw_block("0xb201", "0xb200", 201),
            raw_block("0xb202", "0xb201", 202),
        ],
    )
    .await?;
    upsert_raw_code_hashes(
        pool,
        &[
            raw_code_hash(
                reorg_address,
                "0xb201",
                201,
                "0x1111",
                CanonicalityState::Canonical,
            ),
            raw_code_hash(
                reorg_address,
                "0xb202",
                202,
                "0x2222",
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    let before_reorg = pending_for(pool, reorg_address)
        .await?
        .context("reorg target setup must enqueue work")?;
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            reorg_address,
            before_reorg.generation
        )
        .await?
    );
    let orphaned =
        mark_raw_block_facts_range_orphaned(pool, "eth-mainnet", "0xb202", Some("0xb201")).await?;
    assert_eq!(orphaned.code_hash_count, 1);
    let orphan_transition = pending_for(pool, reorg_address)
        .await?
        .context("orphaning the latest hash must enqueue work")?;
    assert_eq!(
        orphan_transition.previous_code_hash.as_deref(),
        Some("0x2222")
    );
    assert_eq!(
        orphan_transition.current_code_hash.as_deref(),
        Some("0x1111")
    );

    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            reorg_address,
            "0xb202",
            202,
            "0x2222",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    let recanonicalized = pending_for(pool, reorg_address)
        .await?
        .context("recanonicalization must leave a newer dirty generation")?;
    assert!(recanonicalized.generation > orphan_transition.generation);
    assert_eq!(
        recanonicalized.previous_code_hash.as_deref(),
        Some("0x2222")
    );
    assert_eq!(recanonicalized.current_code_hash.as_deref(), Some("0x2222"));

    let correction_address = "0x00000000000000000000000000000000000000c3";
    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            correction_address,
            "0xc301",
            301,
            "0x3333",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    let correction_setup = pending_for(pool, correction_address)
        .await?
        .context("correction target setup must enqueue work")?;
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            correction_address,
            correction_setup.generation
        )
        .await?
    );
    let raw_code_hash_id = sqlx::query_scalar::<_, i64>(
        "SELECT raw_code_hash_id FROM raw_code_hashes WHERE chain_id = $1 AND contract_address = $2",
    )
    .bind("eth-mainnet")
    .bind(correction_address)
    .fetch_one(pool)
    .await?;
    let correction = RawCodeHashCorrectionUpdate {
        raw_code_hash_id,
        stored_code_hash: "0x3333".to_owned(),
        stored_code_byte_length: 32,
        corrected_code_hash: "0x4444".to_owned(),
        corrected_code_byte_length: 17,
    };
    assert_eq!(
        apply_raw_code_hash_corrections(pool, std::slice::from_ref(&correction))
            .await?
            .corrected_count,
        1
    );
    let corrected = pending_for(pool, correction_address)
        .await?
        .context("effective code-hash correction must enqueue work")?;
    assert_eq!(corrected.previous_code_hash.as_deref(), Some("0x3333"));
    assert_eq!(corrected.current_code_hash.as_deref(), Some("0x4444"));
    assert_eq!(
        apply_raw_code_hash_corrections(pool, &[correction])
            .await?
            .already_correct_count,
        1
    );
    assert_eq!(
        pending_for(pool, correction_address)
            .await?
            .context("correction must stay pending")?
            .generation,
        corrected.generation
    );

    database.cleanup().await
}

#[tokio::test]
async fn mixed_bulk_conflicts_do_not_fabricate_or_double_queue_transitions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let pool = database.pool();
    let existing_address = "0x0000000000000000000000000000000000000000";
    let existing = raw_code_hash(
        existing_address,
        "0xd000",
        400,
        "0x5555",
        CanonicalityState::Canonical,
    );
    upsert_raw_code_hashes(pool, std::slice::from_ref(&existing)).await?;
    let initial = pending_for(pool, existing_address)
        .await?
        .context("bulk conflict setup must enqueue work")?;
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            existing_address,
            initial.generation
        )
        .await?
    );

    let mut mixed = Vec::with_capacity(128);
    mixed.push(RawCodeHash {
        canonicality_state: CanonicalityState::Finalized,
        ..existing
    });
    mixed.extend((1_i64..128).map(|index| {
        raw_code_hash(
            &format!("0x{index:040x}"),
            &format!("0xd{index:063x}"),
            400 + index,
            &format!("0x{index:064x}"),
            CanonicalityState::Canonical,
        )
    }));
    upsert_raw_code_hashes(pool, &mixed).await?;

    let pending = load_pending_resolver_profile_input_changes(pool, 1_000).await?;
    assert_eq!(pending.len(), 127);
    assert!(
        pending
            .iter()
            .all(|change| change.contract_address != existing_address)
    );
    let existing_generation = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT generation, processed_generation
        FROM resolver_profile_input_changes
        WHERE chain_id = 'eth-mainnet' AND contract_address = $1
        "#,
    )
    .bind(existing_address)
    .fetch_one(pool)
    .await?;
    assert_eq!(existing_generation, (1, 1));

    let promoted = mixed
        .iter()
        .cloned()
        .map(|mut code_hash| {
            code_hash.canonicality_state = CanonicalityState::Finalized;
            code_hash
        })
        .collect::<Vec<_>>();
    upsert_raw_code_hashes(pool, &promoted).await?;
    let after_conflicts = load_pending_resolver_profile_input_changes(pool, 1_000).await?;
    assert_eq!(after_conflicts.len(), 127);
    assert!(after_conflicts.iter().all(|change| change.generation == 1));

    database.cleanup().await
}

#[tokio::test]
async fn concurrent_writers_cannot_hide_stale_snapshot_or_reversal_work() -> Result<()> {
    let database = TestDatabase::new().await?;
    let pool = database.pool();
    let converged_address = "0x00000000000000000000000000000000000000f1";
    let reversal_address = "0x00000000000000000000000000000000000000f2";

    // The high-block writer owns the queue rows but has not committed. The
    // lower-block writer therefore takes a statement snapshot that cannot see
    // the winning raw rows, then waits on the queue uniqueness conflict.
    let mut high_transaction = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes (
            chain_id, block_hash, block_number, contract_address,
            code_hash, code_byte_length, canonicality_state
        )
        VALUES
            ('eth-mainnet', '0xf101', 601, $1, '0xaaaa', 32, 'canonical'),
            ('eth-mainnet', '0xf201', 601, $2, '0xaaaa', 32, 'canonical')
        "#,
    )
    .bind(converged_address)
    .bind(reversal_address)
    .execute(&mut *high_transaction)
    .await?;

    let mut low_transaction = pool.begin().await?;
    let low_backend_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *low_transaction)
        .await?;
    let low_writer = tokio::spawn(async move {
        sqlx::query(
            r#"
            INSERT INTO raw_code_hashes (
                chain_id, block_hash, block_number, contract_address,
                code_hash, code_byte_length, canonicality_state
            )
            VALUES
                ('eth-mainnet', '0xf100', 600, $1, '0xbbbb', 32, 'canonical'),
                ('eth-mainnet', '0xf200', 600, $2, '0xbbbb', 32, 'canonical')
            "#,
        )
        .bind(converged_address)
        .bind(reversal_address)
        .execute(&mut *low_transaction)
        .await?;
        low_transaction.commit().await?;
        Ok::<(), anyhow::Error>(())
    });

    wait_for_backend_lock(pool, low_backend_pid).await?;
    high_transaction.commit().await?;
    low_writer
        .await
        .context("lower raw-code writer panicked")??;

    let stale_audit = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT generation, current_code_hash
        FROM resolver_profile_input_changes
        WHERE chain_id = 'eth-mainnet' AND contract_address = $1
        "#,
    )
    .bind(converged_address)
    .fetch_one(pool)
    .await?;
    assert_eq!(stale_audit, (2, "0xbbbb".to_owned()));

    // Reads use authoritative raw storage, and acknowledgement converges the
    // durable audit hash while preserving the generation fence.
    let authoritative = pending_for(pool, converged_address)
        .await?
        .context("concurrent target must remain dirty")?;
    assert_eq!(authoritative.current_code_hash.as_deref(), Some("0xaaaa"));
    assert!(
        acknowledge_resolver_profile_input_change(
            pool,
            "eth-mainnet",
            converged_address,
            authoritative.generation
        )
        .await?
    );
    let converged_audit = sqlx::query_as::<_, (i64, i64, String)>(
        r#"
        SELECT generation, processed_generation, current_code_hash
        FROM resolver_profile_input_changes
        WHERE chain_id = 'eth-mainnet' AND contract_address = $1
        "#,
    )
    .bind(converged_address)
    .fetch_one(pool)
    .await?;
    assert_eq!(converged_audit, (2, 2, "0xaaaa".to_owned()));

    // For the second address, orphaning the high observation returns to the
    // stale audit hash. Distinct-transaction notification must still bump.
    sqlx::query(
        r#"
        UPDATE raw_code_hashes
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = 'eth-mainnet'
          AND contract_address = $1
          AND block_hash = '0xf201'
        "#,
    )
    .bind(reversal_address)
    .execute(pool)
    .await?;
    let reversal = pending_for(pool, reversal_address)
        .await?
        .context("same-hash reversal must not be suppressed across transactions")?;
    assert_eq!(reversal.generation, 3);
    assert_eq!(reversal.current_code_hash.as_deref(), Some("0xbbbb"));

    database.cleanup().await
}

#[tokio::test]
async fn upgrade_seed_skips_resolver_history_without_replay_authority() -> Result<()> {
    let database = TestDatabase::new().await?;
    let pool = database.pool();
    let address = "0x0000000000000000000000000000000000000abc";
    let contract_instance_id = Uuid::from_u128(0xabc);

    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain,
            deployment_epoch, rollout_status, normalizer_version,
            file_path, manifest_payload
        )
        VALUES (
            1, 'ens', 'ens_v1_resolver_l1', 'eth-mainnet',
            'historical-test', 'deprecated', 'test',
            'manifests/historical-test.toml', '{}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, 'eth-mainnet', 'resolver', '{}'::JSONB)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role
        )
        VALUES ($1, 'contract', 'removed_resolver', $2, $3, 'public_resolver')
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, admitted_at,
            deactivated_at, source_manifest_id
        )
        VALUES (
            $1, 'eth-mainnet', $2,
            now() - interval '2 days', now() - interval '1 day', $3
        )
        "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    upsert_raw_code_hashes(
        pool,
        &[raw_code_hash(
            address,
            "0xabc1",
            701,
            "0x7777",
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since
        ) VALUES ('eth-mainnet', 0, 1, false, now())
        ON CONFLICT (chain_id) DO UPDATE SET
            retention_generation = 1,
            retained_history_complete = false,
            incomplete_since = now(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        "#,
    )
    .execute(pool)
    .await?;

    // Recreate this migration after the historical fixture exists, matching
    // an upgrade whose retained resolver history is explicitly unproven.
    sqlx::raw_sql(
        r#"
        DROP TRIGGER raw_code_hashes_resolver_profile_input_insert_trigger
            ON raw_code_hashes;
        DROP TRIGGER raw_code_hashes_resolver_profile_input_update_trigger
            ON raw_code_hashes;
        DROP FUNCTION queue_resolver_profile_input_changes_after_raw_code_insert();
        DROP FUNCTION queue_resolver_profile_input_changes_after_raw_code_update();
        DROP FUNCTION record_resolver_profile_input_changes(JSONB);
        DROP TABLE resolver_profile_input_changes;
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::raw_sql(include_str!(
        "../../../../migrations/20260715121000_resolver_profile_input_changes.sql"
    ))
    .execute(pool)
    .await?;

    assert_eq!(
        pending_for(pool, address).await?,
        None,
        "migration must not create permanently pending absence repair from unknown legacy history"
    );

    database.cleanup().await
}
