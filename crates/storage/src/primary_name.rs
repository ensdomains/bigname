use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, postgres::PgRow};

/// Persisted declared claim-state for one address, coin_type, and namespace tuple.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryNameCurrentRow {
    pub address: String,
    pub namespace: String,
    pub coin_type: String,
    pub claim_status: PrimaryNameClaimStatus,
    pub raw_claim_name: Option<String>,
    pub claim_provenance: Value,
}

/// Stable storage representation for projection-owned declared primary-name status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrimaryNameClaimStatus {
    Success,
    NotFound,
    Unsupported,
    InvalidName,
}

impl PrimaryNameClaimStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::InvalidName => "invalid_name",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "success" => Ok(Self::Success),
            "not_found" => Ok(Self::NotFound),
            "unsupported" => Ok(Self::Unsupported),
            "invalid_name" => Ok(Self::InvalidName),
            _ => bail!("unknown primary_names_current claim_status {value}"),
        }
    }
}

/// Load one declared primary-name claim-state row by exact address, namespace, and coin_type.
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
            coin_type,
            claim_status,
            raw_claim_name,
            claim_provenance
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

/// Insert or replace declared primary-name claim-state rows.
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

/// Delete one declared primary-name claim-state row so a worker can rebuild that exact key.
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

/// Clear the primary-name claim-state projection so a worker can perform a one-shot rebuild.
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
    let claim_provenance = serde_json::to_string(&row.claim_provenance)
        .context("failed to serialize primary_names_current claim_provenance")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            claim_provenance
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE
        SET
            claim_status = EXCLUDED.claim_status,
            raw_claim_name = EXCLUDED.raw_claim_name,
            claim_provenance = EXCLUDED.claim_provenance
        RETURNING
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_provenance
        "#,
    )
    .bind(normalize_address(&row.address))
    .bind(&row.coin_type)
    .bind(&row.namespace)
    .bind(row.claim_status.as_str())
    .bind(&row.raw_claim_name)
    .bind(claim_provenance)
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
    match row.claim_status {
        PrimaryNameClaimStatus::InvalidName => {
            let raw_claim_name = row
                .raw_claim_name
                .as_deref()
                .context("primary_names_current invalid_name rows must include raw_claim_name")?;
            if raw_claim_name.trim().is_empty() {
                bail!("primary_names_current invalid_name raw_claim_name must not be blank");
            }
        }
        _ if row.raw_claim_name.is_some() => {
            bail!(
                "primary_names_current rows may include raw_claim_name only for claim_status invalid_name"
            );
        }
        _ => {}
    }
    if !row.claim_provenance.is_object() {
        bail!(
            "primary_names_current row for address {} namespace {} coin_type {} must store claim_provenance as a JSON object",
            row.address,
            row.namespace,
            row.coin_type
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
        claim_status: PrimaryNameClaimStatus::parse(
            &row.try_get::<String, _>("claim_status")
                .context("missing claim_status")?,
        )?,
        raw_claim_name: row.try_get("raw_claim_name").context("missing raw_claim_name")?,
        claim_provenance: row
            .try_get("claim_provenance")
            .context("missing claim_provenance")?,
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

        let inserted = upsert_primary_name_current_rows(database.pool(), &[row.clone()]).await?;
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
}
