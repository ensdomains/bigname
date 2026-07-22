use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::time::{Duration, timeout};

use super::*;
use crate::default_database_url;

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
            .context("failed to parse database URL for primary_names_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_spn_{}_{}_{}", std::process::id(), unique, sequence);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for primary_names_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect primary_names_current test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for primary_names_current tests")?;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advisory_fences_are_scoped_to_the_current_database() -> Result<()> {
    let first_database = TestDatabase::new().await?;
    let second_database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    let mut first_fence = first_database.pool().begin().await?;
    lock_primary_name_tuple_in_transaction(&mut first_fence, address, "ens", "60").await?;

    let mut second_tuple_fence = second_database.pool().begin().await?;
    timeout(
        Duration::from_millis(250),
        lock_primary_name_tuple_in_transaction(&mut second_tuple_fence, address, "ens", "60"),
    )
    .await
    .context("the same tuple in another database must not share an advisory fence")??;
    second_tuple_fence.commit().await?;

    let mut second_replacement_fence = second_database.pool().begin().await?;
    timeout(
        Duration::from_millis(250),
        lock_primary_names_current_replacement_in_transaction(&mut second_replacement_fence),
    )
    .await
    .context("another database's tuple work must not block the replacement fence")??;
    second_replacement_fence.commit().await?;

    first_fence.commit().await?;
    first_database.cleanup().await?;
    second_database.cleanup().await
}

#[tokio::test]
async fn upsert_and_load_round_trip_exact_tuple() -> Result<()> {
    let database = TestDatabase::new().await?;

    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000ABC".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::NotFound,
        raw_claim_name: None,
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
        }),
    };

    let inserted = upsert_primary_name_current_rows(database.pool(), &[row]).await?;
    assert_eq!(
        inserted,
        vec![PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::NotFound,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        }]
    );

    let loaded = load_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000abc",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(loaded, inserted.into_iter().next());

    database.cleanup().await
}

#[tokio::test]
async fn delete_and_clear_remove_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_primary_name_current_rows(
        database.pool(),
        &[
            PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                }),
            },
            PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000def".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Unsupported,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({}),
            },
        ],
    )
    .await?;

    let deleted = delete_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ABC",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(deleted, 1);
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    let cleared = clear_primary_names_current(database.pool()).await?;
    assert_eq!(cleared, 1);
    assert!(
        load_primary_name_current(
            database.pool(),
            "0x0000000000000000000000000000000000000def",
            "ens",
            "60",
        )
        .await?
        .is_none()
    );

    database.cleanup().await
}

#[tokio::test]
async fn round_trips_invalid_name_rows_with_raw_claim_input() -> Result<()> {
    let database = TestDatabase::new().await?;

    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::InvalidName,
        raw_claim_name: Some("alice..eth".to_owned()),
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_resolver_l1",
            "contract_role": "resolver",
            "contract_instance_id": "00000000-0000-0000-0000-000000000123",
            "emitting_address": "0x0000000000000000000000000000000000000fed",
        }),
    };

    let inserted =
        upsert_primary_name_current_rows(database.pool(), std::slice::from_ref(&row)).await?;
    assert_eq!(inserted, vec![row.clone()]);

    let loaded = load_primary_name_current(
        database.pool(),
        "0x0000000000000000000000000000000000000ABC",
        "ens",
        "60",
    )
    .await?;
    assert_eq!(loaded, Some(row));

    database.cleanup().await
}

#[tokio::test]
async fn round_trips_primary_name_snapshots_with_normalized_claim_name() -> Result<()> {
    let database = TestDatabase::new().await?;

    let snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
        },
        normalized_claim_name: Some("alice.eth".to_owned()),
        claim_name_is_normalized: true,
    };

    let inserted =
        upsert_primary_name_current_snapshots(database.pool(), std::slice::from_ref(&snapshot))
            .await?;
    assert_eq!(inserted, vec![snapshot.clone()]);

    assert_eq!(
        load_primary_name_current_snapshot(
            database.pool(),
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
        )
        .await?,
        Some(snapshot)
    );

    database.cleanup().await
}

#[tokio::test]
async fn batch_upsert_preserves_input_order_after_sorted_locking() -> Result<()> {
    let database = TestDatabase::new().await?;

    let second_address_snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000def".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({"source": "primary_name_order_test"}),
        },
        normalized_claim_name: Some("zeta.eth".to_owned()),
        claim_name_is_normalized: true,
    };
    let first_address_snapshot = PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: serde_json::json!({"source": "primary_name_order_test"}),
        },
        normalized_claim_name: Some("alpha.eth".to_owned()),
        claim_name_is_normalized: true,
    };

    let input = vec![
        second_address_snapshot.clone(),
        first_address_snapshot.clone(),
    ];
    let inserted = upsert_primary_name_current_snapshots(database.pool(), &input).await?;
    assert_eq!(inserted, input);

    database.cleanup().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_projection_writes_join_the_tuple_fence_without_serializing_other_tuples()
-> Result<()> {
    async fn insert_legacy_projection_row(
        pool: &PgPool,
        address: &str,
    ) -> std::result::Result<u64, sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                normalized_claim_name,
                claim_name_is_normalized,
                claim_provenance
            )
            VALUES ($1, '60', 'ens', 'success', NULL, 'alice.eth', TRUE, '{}'::jsonb)
            "#,
        )
        .bind(address)
        .execute(pool)
        .await
        .map(|result| result.rows_affected())
    }

    let database = TestDatabase::new().await?;
    let fenced_address = "0x0000000000000000000000000000000000000abc";
    let unrelated_address = "0x0000000000000000000000000000000000000def";
    let request_key = format!("ens:{fenced_address}:60");
    let execution_trace_id = uuid::Uuid::from_u128(0x21700000000000000000000000000001);
    sqlx::query(
        r#"
        INSERT INTO execution_traces (
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            final_payload,
            request_metadata,
            finished_at
        )
        VALUES (
            $1,
            'verified_primary_name',
            $2,
            'ens',
            '{"ethereum": {"block_number": 1}}'::jsonb,
            '{"ens_v1": 1}'::jsonb,
            '{"verified_primary_name": {"status": "success"}}'::jsonb,
            '{}'::jsonb,
            now()
        )
        "#,
    )
    .bind(execution_trace_id)
    .bind(&request_key)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO execution_cache_outcomes (
            execution_cache_key,
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            finished_at
        )
        VALUES (
            'legacy-fence-outcome',
            $1,
            '[{"chain_id": "ethereum-mainnet"}]'::jsonb,
            '[{"source_family": "ens_v1_registry_l1", "manifest_version": 1}]'::jsonb,
            '{"boundary_kind": "selected_checkpoint"}'::jsonb,
            '{"boundary_kind": "selected_checkpoint"}'::jsonb,
            $2,
            'verified_primary_name',
            'ens',
            '{"verified_primary_name": {"status": "success"}}'::jsonb,
            now()
        )
        "#,
    )
    .bind(&request_key)
    .bind(execution_trace_id)
    .execute(database.pool())
    .await?;
    let mut fence = database.pool().begin().await?;
    lock_primary_name_tuple_in_transaction(&mut fence, fenced_address, "ens", "60").await?;

    timeout(
        Duration::from_millis(250),
        insert_legacy_projection_row(database.pool(), unrelated_address),
    )
    .await
    .context("an unrelated legacy projection write must not wait for the tuple fence")??;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_cache_outcomes WHERE execution_cache_key = 'legacy-fence-outcome'",
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "a distinct-tuple write must not invalidate the fenced tuple"
    );

    let error = timeout(
        Duration::from_millis(250),
        insert_legacy_projection_row(database.pool(), fenced_address),
    )
    .await
    .context("a conflicting legacy projection write must fail without waiting")?
    .expect_err("a legacy writer must not cross a new API tuple fence");
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("40001")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_cache_outcomes WHERE execution_cache_key = 'legacy-fence-outcome'",
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "the rejected legacy write must roll back its invalidation"
    );

    fence.commit().await?;
    insert_legacy_projection_row(database.pool(), fenced_address).await?;
    assert!(
        load_primary_name_current(database.pool(), fenced_address, "ens", "60")
            .await?
            .is_some()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM execution_cache_outcomes WHERE execution_cache_key = 'legacy-fence-outcome'",
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "the retried legacy writer must invalidate after acquiring the tuple fence"
    );

    database.cleanup().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_full_replacement_aborts_instead_of_crossing_or_deadlocking_a_tuple_fence()
-> Result<()> {
    async fn run_legacy_replacement(
        pool: &PgPool,
        address: &str,
    ) -> std::result::Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        for trigger in [
            "primary_names_current_identity_feed_after_claim_update",
            "primary_names_current_identity_feed_after_insert_delete",
        ] {
            sqlx::query(&format!(
                "ALTER TABLE primary_names_current DISABLE TRIGGER {trigger}"
            ))
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            r#"
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                normalized_claim_name,
                claim_name_is_normalized,
                claim_provenance
            )
            VALUES ($1, '60', 'ens', 'success', NULL, 'legacy.eth', TRUE, '{}'::jsonb)
            "#,
        )
        .bind(address)
        .execute(&mut *transaction)
        .await?;
        for trigger in [
            "primary_names_current_identity_feed_after_claim_update",
            "primary_names_current_identity_feed_after_insert_delete",
        ] {
            sqlx::query(&format!(
                "ALTER TABLE primary_names_current ENABLE TRIGGER {trigger}"
            ))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await
    }

    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let mut fence = database.pool().begin().await?;
    lock_primary_name_tuple_in_transaction(&mut fence, address, "ens", "60").await?;

    let error = timeout(
        Duration::from_millis(250),
        run_legacy_replacement(database.pool(), address),
    )
    .await
    .context("a legacy full replacement must fail without deadlocking")?
    .expect_err("a legacy full replacement must not cross a tuple fence");
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("40001")
    );

    fence.commit().await?;
    run_legacy_replacement(database.pool(), address).await?;
    assert!(
        load_primary_name_current(database.pool(), address, "ens", "60")
            .await?
            .is_some()
    );

    database.cleanup().await
}

#[test]
fn primary_name_route_retention_indexes_use_concurrent_migrations() {
    for version in [20260722120100, 20260722120200] {
        let migration = crate::MIGRATOR
            .iter()
            .find(|migration| migration.version == version)
            .expect("primary-name route retention index migration is registered");
        assert!(
            migration.no_tx,
            "retention index migration {version} must not hold a DDL transaction"
        );
        assert!(
            migration.sql.contains("CREATE INDEX CONCURRENTLY"),
            "retention index migration {version} must not block execution-table writes"
        );
    }
}

#[tokio::test]
async fn rejects_non_normalized_claim_name_sources() -> Result<()> {
    let database = TestDatabase::new().await?;

    let error = upsert_primary_name_current_snapshots(
        database.pool(),
        &[PrimaryNameCurrentSnapshot {
            row: PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
                claim_status: PrimaryNameClaimStatus::Success,
                raw_claim_name: None,
                claim_provenance: serde_json::json!({}),
            },
            normalized_claim_name: Some("Alice.eth".to_owned()),
            claim_name_is_normalized: false,
        }],
    )
    .await
    .expect_err("non-normalized claimed names must be rejected");

    assert!(
        error
            .to_string()
            .contains("must already be ENSIP-15-normalized")
    );

    database.cleanup().await
}

#[test]
fn verified_primary_name_claim_hooks_reads_persisted_hook_material() -> Result<()> {
    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::InvalidName,
        raw_claim_name: Some("alice..eth".to_owned()),
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
            VERIFIED_PRIMARY_NAME_LOOKUP_KEY: {
                "address": "0x0000000000000000000000000000000000000abc",
                "namespace": "ens",
                "coin_type": "60",
            },
            VERIFIED_PRIMARY_NAME_INVALIDATION_KEY: {
                "claim_status": "invalid_name",
                "primary_claim_source": {
                    "address": "0x0000000000000000000000000000000000000abc",
                    "namespace": "ens",
                    "coin_type": "60",
                    "reverse_name": "0000000000000000000000000000000000000abc.addr.reverse",
                    "reverse_node": "0x000000000000000000000000000000000000000000000000000000000000012e",
                    "claim_provenance": {
                        "source_family": "ens_v1_reverse_l1",
                        "contract_role": "reverse_registrar",
                    },
                },
            },
        }),
    };

    let hooks = verified_primary_name_claim_hooks(&row)?;
    assert_eq!(
        hooks.lookup,
        VerifiedPrimaryNameLookupHook {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
        }
    );
    assert_eq!(
        hooks.lookup.request_key(),
        "ens:0x0000000000000000000000000000000000000abc:60"
    );
    assert_eq!(
        hooks.invalidation,
        VerifiedPrimaryNameInvalidationHook {
            claim_status: PrimaryNameClaimStatus::InvalidName,
            reverse_claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
            primary_claim_source: Some(serde_json::json!({
                "address": "0x0000000000000000000000000000000000000abc",
                "namespace": "ens",
                "coin_type": "60",
                "reverse_name": "0000000000000000000000000000000000000abc.addr.reverse",
                "reverse_node": "0x000000000000000000000000000000000000000000000000000000000000012e",
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                },
            })),
        }
    );

    Ok(())
}

#[test]
fn verified_primary_name_claim_hooks_fall_back_to_row_tuple_without_nested_hooks() -> Result<()> {
    let row = PrimaryNameCurrentRow {
        address: "0x0000000000000000000000000000000000000abc".to_owned(),
        namespace: "ens".to_owned(),
        coin_type: "60".to_owned(),
        claim_status: PrimaryNameClaimStatus::NotFound,
        raw_claim_name: None,
        claim_provenance: serde_json::json!({
            "source_family": "ens_v1_reverse_l1",
            "contract_role": "reverse_registrar",
        }),
    };

    let hooks = verified_primary_name_claim_hooks(&row)?;
    assert_eq!(
        hooks.lookup.request_key(),
        "ens:0x0000000000000000000000000000000000000abc:60"
    );
    assert_eq!(
        hooks.invalidation,
        VerifiedPrimaryNameInvalidationHook {
            claim_status: PrimaryNameClaimStatus::NotFound,
            reverse_claim_provenance: serde_json::json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
            }),
            primary_claim_source: None,
        }
    );

    Ok(())
}

#[tokio::test]
async fn rejects_raw_claim_name_outside_invalid_name_status() -> Result<()> {
    let database = TestDatabase::new().await?;

    let error = upsert_primary_name_current_rows(
        database.pool(),
        &[PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000abc".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: Some("alice.eth".to_owned()),
            claim_provenance: serde_json::json!({}),
        }],
    )
    .await
    .expect_err("success rows must reject raw_claim_name");

    assert!(
        error
            .to_string()
            .contains("raw_claim_name only for claim_status invalid_name")
    );

    database.cleanup().await
}
