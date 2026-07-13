use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

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
            .context("failed to parse database URL for gas_sponsorship tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bg_gas_spon_{}_{unique:x}_{sequence:x}", std::process::id());

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for gas_sponsorship tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect gas_sponsorship test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for gas_sponsorship tests")?;

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

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn name_row(logical_name_id: &str, spent_updates: i64) -> GasSponsorshipCurrentRow {
    let (namespace, normalized_name) = logical_name_id
        .split_once(':')
        .expect("test logical_name_id must be namespace:name");
    GasSponsorshipCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: "0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec".to_owned(),
        lease_start_at: Some(timestamp(1_700_000_000)),
        registered_seconds_total: 63_072_000,
        earned_updates: 10,
        spent_updates,
        last_sponsored_write_at: Some(timestamp(1_750_000_000)),
        provenance: json!({"derivation_kind": "gas_sponsorship"}),
        coverage: json!({"status": "partial"}),
        chain_positions: json!({"ethereum": {"block_number": 42}}),
        canonicality_summary: json!({"canonical": true}),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_750_000_100),
    }
}

fn global_row(namespace: &str) -> GasSponsorshipGlobalCurrentRow {
    GasSponsorshipGlobalCurrentRow {
        namespace: namespace.to_owned(),
        sponsored_op_count: 12,
        attributed_op_count: 10,
        failed_op_count: 2,
        // Larger than u64/i64/u128 to prove numeric(78,0) round-trips.
        gas_wei_total: "340282366920938463463374607431768211456".to_owned(),
        failed_gas_wei_total: "1000000000000000".to_owned(),
        usd_e8_total: "123456789012".to_owned(),
        unpriced_wei_total: "0".to_owned(),
        provenance: json!({"derivation_kind": "gas_sponsorship"}),
        coverage: json!({"status": "partial"}),
        chain_positions: json!({"ethereum": {"block_number": 42}}),
        canonicality_summary: json!({"canonical": true}),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_750_000_100),
    }
}

#[tokio::test]
async fn upserts_and_loads_name_rows() -> Result<()> {
    let database = TestDatabase::new().await?;

    let rows = vec![name_row("ens:alice.eth", 3), name_row("ens:bob.eth", 0)];
    let upserted = upsert_gas_sponsorship_current_rows(database.pool(), &rows).await?;
    assert_eq!(upserted, 2);

    let loaded = load_gas_sponsorship_current(database.pool(), "ens:alice.eth")
        .await?
        .expect("row loads");
    assert_eq!(loaded, rows[0]);

    let replacement = name_row("ens:alice.eth", 4);
    upsert_gas_sponsorship_current_rows(database.pool(), std::slice::from_ref(&replacement))
        .await?;
    let reloaded = load_gas_sponsorship_current(database.pool(), "ens:alice.eth")
        .await?
        .expect("row reloads");
    assert_eq!(reloaded.spent_updates, 4);

    assert_eq!(
        load_gas_sponsorship_current(database.pool(), "ens:missing.eth").await?,
        None
    );

    assert_eq!(
        delete_gas_sponsorship_current(database.pool(), "ens:bob.eth").await?,
        1
    );
    assert_eq!(clear_gas_sponsorship_current(database.pool()).await?, 1);

    database.cleanup().await
}

#[tokio::test]
async fn upserts_and_loads_global_row_with_wide_numerics() -> Result<()> {
    let database = TestDatabase::new().await?;

    let row = global_row("ens");
    upsert_gas_sponsorship_global_current_row(database.pool(), &row).await?;
    let loaded = load_gas_sponsorship_global_current(database.pool(), "ens")
        .await?
        .expect("global row loads");
    assert_eq!(loaded, row);

    let mut updated = row.clone();
    updated.sponsored_op_count = 13;
    updated.usd_e8_total = "223456789012".to_owned();
    upsert_gas_sponsorship_global_current_row(database.pool(), &updated).await?;
    let reloaded = load_gas_sponsorship_global_current(database.pool(), "ens")
        .await?
        .expect("global row reloads");
    assert_eq!(reloaded, updated);

    assert_eq!(
        load_gas_sponsorship_global_current(database.pool(), "basenames").await?,
        None
    );
    assert_eq!(
        clear_gas_sponsorship_global_current(database.pool()).await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_inconsistent_rows_before_writing() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut mismatched_identity = name_row("ens:alice.eth", 0);
    mismatched_identity.normalized_name = "other.eth".to_owned();
    let error = upsert_gas_sponsorship_current_rows(
        database.pool(),
        std::slice::from_ref(&mismatched_identity),
    )
    .await
    .expect_err("identity mismatch must fail");
    assert!(format!("{error:#}").contains("logical_name_id"));

    let mut negative_counts = global_row("ens");
    negative_counts.attributed_op_count = 99;
    let error = upsert_gas_sponsorship_global_current_row(database.pool(), &negative_counts)
        .await
        .expect_err("attributed > sponsored must fail");
    assert!(format!("{error:#}").contains("op counts"));

    let mut non_decimal = global_row("ens");
    non_decimal.gas_wei_total = "-5".to_owned();
    let error = upsert_gas_sponsorship_global_current_row(database.pool(), &non_decimal)
        .await
        .expect_err("negative decimal string must fail");
    assert!(format!("{error:#}").contains("decimal"));

    database.cleanup().await
}
