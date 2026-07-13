use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, NormalizedEvent, RawBlock, default_database_url,
    load_gas_sponsorship_current, load_gas_sponsorship_global_current, upsert_normalized_events,
    upsert_raw_blocks,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

use super::math::SECONDS_PER_REGISTRATION_YEAR;
use super::{rebuild_gas_sponsorship_current, rebuild_gas_sponsorship_global_current};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const TEST_CHAIN: &str = "ethereum-sepolia";
const BLOCK_HASH: &str = "0x00000000000000000000000000000000000000000000000000000000000ab10c";
const BLOCK_NUMBER: i64 = 5_500_000;
const BLOCK_TIMESTAMP: i64 = 1_752_000_000;
const ALICE: &str = "ens:alice.eth";
const ALICE_NAMEHASH: &str = "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";

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
            .context("failed to parse database URL for gas_sponsorship worker tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bg_gas_wrk_{}_{unique:x}_{sequence:x}", std::process::id());

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for gas_sponsorship worker tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect gas_sponsorship worker test pool")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for gas_sponsorship worker tests")?;

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

fn normalized_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    event_kind: &str,
    derivation_kind: &str,
    log_index: i64,
    before_state: Value,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id: None,
        event_kind: event_kind.to_owned(),
        source_family: "ens_gas_sponsorship_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(TEST_CHAIN.to_owned()),
        block_number: Some(BLOCK_NUMBER),
        block_hash: Some(BLOCK_HASH.to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        log_index: Some(log_index),
        raw_fact_ref: json!({"kind": "raw_log"}),
        derivation_kind: derivation_kind.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state,
        after_state,
    }
}

fn registration(event_identity: &str, log_index: i64, duration_seconds: i64) -> NormalizedEvent {
    normalized_event(
        event_identity,
        Some(ALICE),
        "RegistrarNameRegistered",
        "ens_v2_registrar",
        log_index,
        json!({}),
        json!({"duration": duration_seconds, "label": "alice"}),
    )
}

fn sponsored_write(
    event_identity: &str,
    log_index: i64,
    user_op_hash: &str,
    success: bool,
) -> NormalizedEvent {
    normalized_event(
        event_identity,
        Some(ALICE),
        "SponsoredNameWriteObserved",
        "entrypoint_user_operation",
        log_index,
        json!({}),
        json!({
            "user_op_hash": user_op_hash,
            "success": success,
            "write_kind": "records",
            "node": ALICE_NAMEHASH,
        }),
    )
}

fn sponsored_operation(
    event_identity: &str,
    log_index: i64,
    gas_wei: &str,
    success: bool,
    attribution_status: &str,
) -> NormalizedEvent {
    normalized_event(
        event_identity,
        None,
        "SponsoredUserOperationObserved",
        "entrypoint_user_operation",
        log_index,
        json!({}),
        json!({
            "user_op_hash": event_identity,
            "success": success,
            "actual_gas_cost_wei": gas_wei,
            "attribution_status": attribution_status,
        }),
    )
}

fn price_update(event_identity: &str, log_index: i64, answer_e8: &str) -> NormalizedEvent {
    normalized_event(
        event_identity,
        None,
        "PriceFeedAnswerUpdated",
        "entrypoint_user_operation",
        log_index,
        json!({}),
        json!({"pair": "ETH/USD", "answer_e8": answer_e8, "round_id": "1"}),
    )
}

async fn seed_block(pool: &PgPool) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: TEST_CHAIN.to_owned(),
            block_hash: BLOCK_HASH.to_owned(),
            parent_hash: None,
            block_number: BLOCK_NUMBER,
            block_timestamp: OffsetDateTime::from_unix_timestamp(BLOCK_TIMESTAMP)
                .expect("test timestamp is valid"),
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    Ok(())
}

async fn seed_surface(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id, namespace, input_name, canonical_display_name,
            normalized_name, dns_encoded_name, namehash, labelhashes,
            normalizer_version, chain_id, block_hash, block_number,
            canonicality_state
        )
        VALUES (
            $1, 'ens', 'alice.eth', 'alice.eth',
            'alice.eth', '\x05616c69636503657468'::bytea, $2, '{}'::TEXT[],
            'ensip15@ens-normalize-0.1.1', $3, $4, $5,
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(ALICE)
    .bind(ALICE_NAMEHASH)
    .bind(TEST_CHAIN)
    .bind(BLOCK_HASH)
    .bind(BLOCK_NUMBER)
    .execute(pool)
    .await
    .context("failed to seed test name surface")?;
    Ok(())
}

#[tokio::test]
async fn rebuilds_name_rows_with_lease_reset_and_spent_counts() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_block(database.pool()).await?;
    seed_surface(database.pool()).await?;

    upsert_normalized_events(
        database.pool(),
        &[
            registration("reg-1", 1, 2 * SECONDS_PER_REGISTRATION_YEAR),
            sponsored_write("write-1", 2, "0xop1", true),
            sponsored_write("write-2", 3, "0xop2", false),
            // The same operation writing twice spends once.
            sponsored_write("write-2b", 4, "0xop2", false),
        ],
    )
    .await?;

    let summary = rebuild_gas_sponsorship_current(database.pool(), None).await?;
    assert_eq!(summary.requested_name_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_gas_sponsorship_current(database.pool(), ALICE)
        .await?
        .expect("alice row exists");
    assert_eq!(row.namehash, ALICE_NAMEHASH);
    assert_eq!(
        row.registered_seconds_total,
        2 * SECONDS_PER_REGISTRATION_YEAR
    );
    assert_eq!(row.earned_updates, 10);
    // Failed operations debit too, and one operation debits once.
    assert_eq!(row.spent_updates, 2);
    assert_eq!(
        row.lease_start_at.map(|at| at.unix_timestamp()),
        Some(BLOCK_TIMESTAMP)
    );

    // Point rebuild agrees with the full rebuild.
    let point = rebuild_gas_sponsorship_current(database.pool(), Some(ALICE)).await?;
    assert_eq!(point.upserted_row_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn rebuilds_global_totals_with_pricing() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_block(database.pool()).await?;

    upsert_normalized_events(
        database.pool(),
        &[
            // Unpriced operation before the first answer.
            sponsored_operation("op-1", 1, "1000000000000000000", true, "attributed"),
            price_update("price-1", 2, "250000000000"),
            sponsored_operation("op-2", 3, "500000000000000000", false, "input_unavailable"),
        ],
    )
    .await?;

    let summary = rebuild_gas_sponsorship_global_current(database.pool(), None).await?;
    assert_eq!(summary.requested_namespace_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_gas_sponsorship_global_current(database.pool(), "ens")
        .await?
        .expect("global row exists");
    assert_eq!(row.sponsored_op_count, 2);
    assert_eq!(row.attributed_op_count, 1);
    assert_eq!(row.failed_op_count, 1);
    assert_eq!(row.gas_wei_total, "1500000000000000000");
    assert_eq!(row.failed_gas_wei_total, "500000000000000000");
    // 0.5 ETH at 2500 USD = 1250 USD = 125_000_000_000 e8.
    assert_eq!(row.usd_e8_total, "125000000000");
    assert_eq!(row.unpriced_wei_total, "1000000000000000000");

    database.cleanup().await
}

#[tokio::test]
async fn point_rebuild_deletes_rows_whose_facts_vanished() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_block(database.pool()).await?;
    seed_surface(database.pool()).await?;

    upsert_normalized_events(
        database.pool(),
        &[registration("reg-1", 1, SECONDS_PER_REGISTRATION_YEAR)],
    )
    .await?;
    rebuild_gas_sponsorship_current(database.pool(), Some(ALICE)).await?;
    assert!(
        load_gas_sponsorship_current(database.pool(), ALICE)
            .await?
            .is_some()
    );

    // A reorg orphans the registration; the point rebuild removes the row.
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned'")
        .execute(database.pool())
        .await?;
    let summary = rebuild_gas_sponsorship_current(database.pool(), Some(ALICE)).await?;
    assert_eq!(summary.deleted_row_count, 1);
    assert!(
        load_gas_sponsorship_current(database.pool(), ALICE)
            .await?
            .is_none()
    );

    database.cleanup().await
}
