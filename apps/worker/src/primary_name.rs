use anyhow::{Context, Result, bail};
use bigname_storage::{
    PrimaryNameCurrentRow, clear_primary_names_current, delete_primary_name_current,
    upsert_primary_name_current_rows,
};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
};

const ENS_NAMESPACE: &str = "ens";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryNamesCurrentRebuildSummary {
    pub requested_tuple_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PrimaryNameTuple {
    address: String,
    namespace: String,
    coin_type: String,
}

pub async fn rebuild_primary_names_current(
    pool: &PgPool,
    address: Option<&str>,
    namespace: Option<&str>,
    coin_type: Option<&str>,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    match (address, namespace, coin_type) {
        (Some(address), Some(namespace), Some(coin_type)) => {
            rebuild_one_primary_name(pool, address, namespace, coin_type).await
        }
        (None, None, None) => rebuild_all_primary_names(pool).await,
        _ => bail!(
            "primary_names_current rebuild requires address, namespace, and coin_type together when targeting one tuple"
        ),
    }
}

async fn rebuild_all_primary_names(pool: &PgPool) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let tuples = load_primary_name_tuples(pool).await?;
    let deleted_row_count = clear_primary_names_current(pool).await?;
    let rows = tuples
        .iter()
        .map(primary_name_row)
        .collect::<Vec<PrimaryNameCurrentRow>>();
    let upserted_row_count = upsert_primary_name_current_rows(pool, &rows).await?.len();

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: tuples.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_primary_name(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<PrimaryNamesCurrentRebuildSummary> {
    let target = PrimaryNameTuple {
        address: normalize_address(address),
        namespace: namespace.to_owned(),
        coin_type: coin_type.to_owned(),
    };
    let deleted_row_count =
        delete_primary_name_current(pool, &target.address, &target.namespace, &target.coin_type)
            .await?;

    let upserted_row_count = match load_primary_name_tuple(pool, &target).await? {
        Some(tuple) => upsert_primary_name_current_rows(pool, &[primary_name_row(&tuple)])
            .await?
            .len(),
        None => 0,
    };

    Ok(PrimaryNamesCurrentRebuildSummary {
        requested_tuple_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn load_primary_name_tuples(pool: &PgPool) -> Result<Vec<PrimaryNameTuple>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT DISTINCT
            LOWER(ne.after_state->>'address') AS address,
            ne.namespace AS namespace,
            ne.after_state->>'coin_type' AS coin_type
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
        ORDER BY address ASC, coin_type ASC, namespace ASC
        "#,
    ))
    .bind(ENS_NAMESPACE)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .fetch_all(pool)
    .await
    .context("failed to load primary-name tuples from canonical ReverseChanged events")?;

    rows.into_iter().map(decode_primary_name_tuple).collect()
}

async fn load_primary_name_tuple(
    pool: &PgPool,
    target: &PrimaryNameTuple,
) -> Result<Option<PrimaryNameTuple>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            LOWER(ne.after_state->>'address') AS address,
            ne.namespace AS namespace,
            ne.after_state->>'coin_type' AS coin_type
        FROM normalized_events ne
        WHERE ne.namespace = $1
          AND ne.event_kind = $2
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
          AND LOWER(ne.after_state->>'address') = $3
          AND ne.after_state->>'coin_type' = $4
          AND ne.after_state->>'address' IS NOT NULL
          AND ne.after_state->>'address' <> ''
          AND ne.after_state->>'coin_type' IS NOT NULL
          AND ne.after_state->>'coin_type' <> ''
        ORDER BY
            ne.block_number DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.normalized_event_id DESC
        LIMIT 1
        "#,
    ))
    .bind(&target.namespace)
    .bind(EVENT_KIND_REVERSE_CHANGED)
    .bind(&target.address)
    .bind(&target.coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load primary-name tuple for address {} namespace {} coin_type {}",
            target.address, target.namespace, target.coin_type
        )
    })?;

    row.map(decode_primary_name_tuple).transpose()
}

fn decode_primary_name_tuple(row: PgRow) -> Result<PrimaryNameTuple> {
    let address = row
        .try_get::<String, _>("address")
        .context("missing primary-name address")?
        .to_ascii_lowercase();
    let namespace = row
        .try_get::<String, _>("namespace")
        .context("missing primary-name namespace")?;
    let coin_type = row
        .try_get::<String, _>("coin_type")
        .context("missing primary-name coin_type")?;

    if address.trim().is_empty() {
        bail!("primary-name tuple is missing address");
    }
    if namespace.trim().is_empty() {
        bail!("primary-name tuple is missing namespace");
    }
    if coin_type.trim().is_empty() {
        bail!("primary-name tuple is missing coin_type");
    }

    Ok(PrimaryNameTuple {
        address,
        namespace,
        coin_type,
    })
}

fn primary_name_row(tuple: &PrimaryNameTuple) -> PrimaryNameCurrentRow {
    PrimaryNameCurrentRow {
        address: tuple.address.clone(),
        namespace: tuple.namespace.clone(),
        coin_type: tuple.coin_type.clone(),
    }
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
    use bigname_storage::{
        CanonicalityState, NormalizedEvent, default_database_url, load_primary_name_current,
        upsert_normalized_events, upsert_primary_name_current_rows,
    };
    use serde_json::json;

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
                .context("failed to parse database URL for worker primary_names_current tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!("bn_wpn_{}_{}_{}", std::process::id(), unique, sequence);

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for worker primary_names_current tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect worker primary_names_current test pool")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for worker primary_names_current tests")?;

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

    fn reverse_changed_event(
        event_identity: &str,
        address: &str,
        coin_type: &str,
        block_number: i64,
        log_index: i64,
        canonicality_state: CanonicalityState,
    ) -> NormalizedEvent {
        let normalized_address = address.to_ascii_lowercase();
        let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

        NormalizedEvent {
            event_identity: event_identity.to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
            source_family: "ens_v1_reverse_l1".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(block_number),
            block_hash: Some(format!("0xblock{block_number:064x}")),
            transaction_hash: Some(format!("0xtx{block_number:064x}")),
            log_index: Some(log_index),
            raw_fact_ref: json!({
                "kind": "raw_log",
                "chain_id": "ethereum-mainnet",
                "block_number": block_number,
                "log_index": log_index,
            }),
            derivation_kind: "ens_v1_reverse_claim".to_owned(),
            canonicality_state,
            before_state: json!({}),
            after_state: json!({
                "source_event": "ReverseClaimed",
                "address": normalized_address,
                "coin_type": coin_type,
                "reverse_namespace": ENS_NAMESPACE,
                "reverse_label": reverse_label,
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
            }),
        }
    }

    #[tokio::test]
    async fn full_rebuild_projects_distinct_canonical_reverse_tuples() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_normalized_events(
            database.pool(),
            &[
                reverse_changed_event(
                    "reverse-a-60-canonical",
                    "0x0000000000000000000000000000000000000aAa",
                    "60",
                    100,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_changed_event(
                    "reverse-a-60-finalized",
                    "0x0000000000000000000000000000000000000aaa",
                    "60",
                    101,
                    0,
                    CanonicalityState::Finalized,
                ),
                reverse_changed_event(
                    "reverse-a-61-safe",
                    "0x0000000000000000000000000000000000000aaa",
                    "61",
                    102,
                    0,
                    CanonicalityState::Safe,
                ),
                reverse_changed_event(
                    "reverse-b-60-canonical",
                    "0x0000000000000000000000000000000000000bbb",
                    "60",
                    103,
                    0,
                    CanonicalityState::Canonical,
                ),
                reverse_changed_event(
                    "reverse-orphaned",
                    "0x0000000000000000000000000000000000000ccc",
                    "60",
                    104,
                    0,
                    CanonicalityState::Orphaned,
                ),
                NormalizedEvent {
                    event_identity: "not-reverse".to_owned(),
                    event_kind: "ResolverChanged".to_owned(),
                    ..reverse_changed_event(
                        "not-reverse-base",
                        "0x0000000000000000000000000000000000000ddd",
                        "60",
                        105,
                        0,
                        CanonicalityState::Canonical,
                    )
                },
            ],
        )
        .await?;

        let summary = rebuild_primary_names_current(database.pool(), None, None, None).await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 3,
                upserted_row_count: 3,
                deleted_row_count: 0,
            }
        );

        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "60",
            )
            .await?
            .is_some()
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000aaa",
                "ens",
                "61",
            )
            .await?
            .is_some()
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000bbb",
                "ens",
                "60",
            )
            .await?
            .is_some()
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000ccc",
                "ens",
                "60",
            )
            .await?
            .is_none()
        );
        assert!(
            load_primary_name_current(
                database.pool(),
                "0x0000000000000000000000000000000000000ddd",
                "ens",
                "60",
            )
            .await?
            .is_none()
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn targeted_rebuild_deletes_stale_tuple_when_no_reverse_event_exists() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_primary_name_current_rows(
            database.pool(),
            &[PrimaryNameCurrentRow {
                address: "0x0000000000000000000000000000000000000abc".to_owned(),
                namespace: "ens".to_owned(),
                coin_type: "60".to_owned(),
            }],
        )
        .await?;

        let summary = rebuild_primary_names_current(
            database.pool(),
            Some("0x0000000000000000000000000000000000000abc"),
            Some("ens"),
            Some("60"),
        )
        .await?;
        assert_eq!(
            summary,
            PrimaryNamesCurrentRebuildSummary {
                requested_tuple_count: 1,
                upserted_row_count: 0,
                deleted_row_count: 1,
            }
        );
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

        database.cleanup().await
    }
}
