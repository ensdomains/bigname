use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::default_database_url;

/// Persisted resolver-overview projection row keyed by resolver target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverCurrentRow {
    pub chain_id: String,
    pub resolver_address: String,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Load one resolver-overview projection row by chain and resolver address.
pub async fn load_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    let normalized_address = normalize_resolver_address(resolver_address);
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM resolver_current
        WHERE chain_id = $1
          AND resolver_address = $2
        "#,
    )
    .bind(chain_id)
    .bind(&normalized_address)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolver_current row for chain_id {chain_id} resolver_address {normalized_address}"
        )
    })?;

    row.map(decode_resolver_current_row).transpose()
}

/// Insert or replace resolver-overview projection rows.
pub async fn upsert_resolver_current_rows(
    pool: &PgPool,
    rows: &[ResolverCurrentRow],
) -> Result<Vec<ResolverCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for resolver_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_resolver_current_row(row)?;
        snapshots.push(upsert_resolver_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit resolver_current upsert")?;

    Ok(snapshots)
}

/// Delete one resolver-overview row so a worker can rebuild the key.
pub async fn delete_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<u64> {
    let normalized_address = normalize_resolver_address(resolver_address);
    sqlx::query(
        r#"
        DELETE FROM resolver_current
        WHERE chain_id = $1
          AND resolver_address = $2
        "#,
    )
    .bind(chain_id)
    .bind(&normalized_address)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete resolver_current row for chain_id {chain_id} resolver_address {normalized_address}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the resolver-overview projection so a worker can perform a one-shot rebuild.
pub async fn clear_resolver_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM resolver_current")
        .execute(pool)
        .await
        .context("failed to clear resolver_current rows")
        .map(|result| result.rows_affected())
}

async fn upsert_resolver_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ResolverCurrentRow,
) -> Result<ResolverCurrentRow> {
    let declared_summary = serde_json::to_string(&row.declared_summary)
        .context("failed to serialize resolver_current declared_summary")?;
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize resolver_current provenance")?;
    let coverage = serde_json::to_string(&row.coverage)
        .context("failed to serialize resolver_current coverage")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize resolver_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize resolver_current canonicality_summary")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO resolver_current (
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1,
            $2,
            $3::jsonb,
            $4::jsonb,
            $5::jsonb,
            $6::jsonb,
            $7::jsonb,
            $8,
            $9
        )
        ON CONFLICT (chain_id, resolver_address) DO UPDATE
        SET
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.chain_id)
    .bind(normalize_resolver_address(&row.resolver_address))
    .bind(declared_summary)
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert resolver_current row for chain_id {} resolver_address {}",
            row.chain_id, row.resolver_address
        )
    })?;

    decode_resolver_current_row(snapshot)
}

fn validate_resolver_current_row(row: &ResolverCurrentRow) -> Result<()> {
    if row.chain_id.trim().is_empty() {
        bail!("resolver_current row must include chain_id");
    }
    if row.resolver_address.trim().is_empty() {
        bail!(
            "resolver_current row for chain_id {} must include resolver_address",
            row.chain_id
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "resolver_current row for chain_id {} resolver_address {} has non-positive manifest_version {}",
            row.chain_id,
            row.resolver_address,
            row.manifest_version
        );
    }

    ensure_json_object(&row.declared_summary, "declared_summary", row)?;
    ensure_json_object(&row.provenance, "provenance", row)?;
    ensure_json_object(&row.coverage, "coverage", row)?;
    ensure_json_object(&row.chain_positions, "chain_positions", row)?;
    ensure_json_object(&row.canonicality_summary, "canonicality_summary", row)?;

    Ok(())
}

fn ensure_json_object(value: &Value, field_name: &str, row: &ResolverCurrentRow) -> Result<()> {
    if !value.is_object() {
        bail!(
            "resolver_current row for chain_id {} resolver_address {} field {} must be a JSON object",
            row.chain_id,
            row.resolver_address,
            field_name
        );
    }

    Ok(())
}

fn decode_resolver_current_row(row: PgRow) -> Result<ResolverCurrentRow> {
    Ok(ResolverCurrentRow {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        resolver_address: row
            .try_get::<String, _>("resolver_address")
            .context("missing resolver_address")?
            .to_ascii_lowercase(),
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn normalize_resolver_address(resolver_address: &str) -> String {
    resolver_address.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use super::*;

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
                .context("failed to parse database URL for resolver_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_resolver_current_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for resolver_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect resolver_current test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for resolver_current tests")?;

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

    fn resolver_current_row(
        chain_id: &str,
        resolver_address: &str,
        manifest_version: i64,
    ) -> ResolverCurrentRow {
        ResolverCurrentRow {
            chain_id: chain_id.to_owned(),
            resolver_address: resolver_address.to_owned(),
            declared_summary: json!({
                "bindings": {
                    "count": 2,
                    "status": "supported"
                },
                "aliases": {
                    "count": 1,
                    "status": "supported"
                },
                "permissions": {
                    "count": 3,
                    "status": "supported"
                },
                "role_holders": {
                    "count": 1,
                    "status": "supported"
                },
                "event_summary": {
                    "count": 5,
                    "status": "supported"
                }
            }),
            provenance: json!({
                "normalized_event_ids": [801, 802, 803],
                "derivation_kind": "resolver_current_rebuild"
            }),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "enumeration_basis": "resolver_overview"
            }),
            chain_positions: json!({
                chain_id: {
                    "chain_id": chain_id,
                    "block_number": 21_100_001,
                    "block_hash": "0xresolver",
                    "timestamp": "2026-04-17T00:15:00Z"
                }
            }),
            canonicality_summary: json!({
                "status": "finalized",
                "chains": {
                    chain_id: "finalized"
                }
            }),
            manifest_version,
            last_recomputed_at: timestamp(1_776_000_900),
        }
    }

    #[tokio::test]
    async fn resolver_current_upserts_and_loads_by_key() -> Result<()> {
        let database = TestDatabase::new().await?;
        let expected = resolver_current_row(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000ABC",
            5,
        );

        let inserted =
            upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
        let mut normalized_expected = expected.clone();
        normalized_expected.resolver_address = expected.resolver_address.to_ascii_lowercase();
        assert_eq!(inserted, vec![normalized_expected.clone()]);

        let loaded = load_resolver_current(
            database.pool(),
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000abc",
        )
        .await?;
        assert_eq!(loaded, Some(normalized_expected));

        database.cleanup().await
    }

    #[tokio::test]
    async fn resolver_current_upsert_replaces_existing_projection_row() -> Result<()> {
        let database = TestDatabase::new().await?;
        let first = resolver_current_row(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000def",
            5,
        );
        upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

        let mut replacement = first.clone();
        replacement.declared_summary = json!({
            "bindings": {
                "count": 4,
                "status": "supported"
            },
            "aliases": {
                "count": 2,
                "status": "supported"
            }
        });
        replacement.coverage = json!({
            "status": "partial",
            "unsupported_reason": "role_holders_pending"
        });
        replacement.manifest_version = 6;

        let updated =
            upsert_resolver_current_rows(database.pool(), std::slice::from_ref(&replacement))
                .await?;
        let mut normalized_replacement = replacement.clone();
        normalized_replacement.resolver_address = replacement.resolver_address.to_ascii_lowercase();
        assert_eq!(updated, vec![normalized_replacement.clone()]);
        assert_eq!(
            load_resolver_current(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000DEF",
            )
            .await?,
            Some(normalized_replacement)
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn resolver_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
        let database = TestDatabase::new().await?;
        let first = resolver_current_row(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000101",
            5,
        );
        let second = resolver_current_row(
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000102",
            5,
        );

        upsert_resolver_current_rows(database.pool(), &[first.clone(), second.clone()]).await?;

        assert_eq!(
            delete_resolver_current(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000101",
            )
            .await?,
            1
        );
        assert_eq!(
            load_resolver_current(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000101",
            )
            .await?,
            None
        );

        let mut normalized_second = second.clone();
        normalized_second.resolver_address = second.resolver_address.to_ascii_lowercase();
        assert_eq!(
            load_resolver_current(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000102",
            )
            .await?,
            Some(normalized_second)
        );

        assert_eq!(clear_resolver_current(database.pool()).await?, 1);
        assert_eq!(
            load_resolver_current(
                database.pool(),
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000102",
            )
            .await?,
            None
        );

        database.cleanup().await
    }
}
