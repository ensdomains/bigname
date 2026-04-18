use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Row, postgres::PgRow};

/// Persisted bootstrap primary-name lookup tuple keyed by address, coin_type, and namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryNameCurrentRow {
    pub address: String,
    pub namespace: String,
    pub coin_type: String,
}

/// Load one primary-name bootstrap tuple by exact address, namespace, and coin_type.
pub async fn load_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentRow>> {
    let normalized_address = normalize_address(address);
    let row = sqlx::query(
        r#"
        SELECT
            address,
            namespace,
            coin_type
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        "#,
    )
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load primary_names_current row for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })?;

    row.map(decode_primary_name_current_row).transpose()
}

/// Insert or replace bootstrap primary-name lookup tuples.
pub async fn upsert_primary_name_current_rows(
    pool: &PgPool,
    rows: &[PrimaryNameCurrentRow],
) -> Result<Vec<PrimaryNameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for primary_names_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_primary_name_current_row(row)?;
        snapshots.push(upsert_primary_name_current_row(&mut transaction, row).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current upsert")?;

    Ok(snapshots)
}

/// Delete one bootstrap primary-name tuple so a worker can rebuild that exact key.
pub async fn delete_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<u64> {
    let normalized_address = normalize_address(address);
    sqlx::query(
        r#"
        DELETE FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        "#,
    )
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete primary_names_current row for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the primary-name bootstrap projection so a worker can perform a one-shot rebuild.
pub async fn clear_primary_names_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM primary_names_current")
        .execute(pool)
        .await
        .context("failed to clear primary_names_current rows")
        .map(|result| result.rows_affected())
}

async fn upsert_primary_name_current_row(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    row: &PrimaryNameCurrentRow,
) -> Result<PrimaryNameCurrentRow> {
    let snapshot = sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace
        )
        VALUES ($1, $2, $3)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE
        SET address = EXCLUDED.address
        RETURNING
            address,
            namespace,
            coin_type
        "#,
    )
    .bind(normalize_address(&row.address))
    .bind(&row.coin_type)
    .bind(&row.namespace)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert primary_names_current row for address {} namespace {} coin_type {}",
            row.address, row.namespace, row.coin_type
        )
    })?;

    decode_primary_name_current_row(snapshot)
}

fn validate_primary_name_current_row(row: &PrimaryNameCurrentRow) -> Result<()> {
    if row.address.trim().is_empty() {
        bail!("primary_names_current row must include address");
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "primary_names_current row for address {} must include namespace",
            row.address
        );
    }
    if row.coin_type.trim().is_empty() {
        bail!(
            "primary_names_current row for address {} namespace {} must include coin_type",
            row.address,
            row.namespace
        );
    }

    Ok(())
}

fn decode_primary_name_current_row(row: PgRow) -> Result<PrimaryNameCurrentRow> {
    Ok(PrimaryNameCurrentRow {
        address: row
            .try_get::<String, _>("address")
            .context("missing address")?
            .to_ascii_lowercase(),
        namespace: row.try_get("namespace").context("missing namespace")?,
        coin_type: row.try_get("coin_type").context("missing coin_type")?,
    })
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
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

    #[tokio::test]
    async fn upsert_and_load_round_trip_exact_tuple() -> Result<()> {
        let database = TestDatabase::new().await?;

        let row = PrimaryNameCurrentRow {
            address: "0x0000000000000000000000000000000000000ABC".to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
        };

        let inserted = upsert_primary_name_current_rows(database.pool(), &[row]).await?;
        assert_eq!(
            inserted,
            vec![PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
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
                },
                PrimaryNameCurrentRow {
                    address: "0x0000000000000000000000000000000000000def".to_owned(),
                    namespace: "ens".to_owned(),
                    coin_type: "60".to_owned(),
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
}
