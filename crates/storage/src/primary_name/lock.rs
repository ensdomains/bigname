use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

use super::types::{VERIFIED_PRIMARY_NAME_REQUEST_TYPE, normalize_address};

// Tuple operations take this lock in shared mode. A full-table replacement
// takes it exclusively so it does not need millions of tuple locks.
const PRIMARY_NAMES_CURRENT_REPLACEMENT_HASH_SEED: i64 = -0x4249_474e_504e_0001_i64;
const PRIMARY_NAME_TUPLE_HASH_SEED: i64 = 0x504e_5455_504c_4501_i64;

/// Serialize one primary-name projection tuple with route-local fallback work.
pub async fn lock_primary_name_tuple_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<()> {
    lock_primary_names_current_replacement_shared(transaction).await?;

    let normalized_address = normalize_address(address);
    let identity = format!(
        "{}:{normalized_address}:{}:{namespace}:{}:{coin_type}",
        normalized_address.len(),
        namespace.len(),
        coin_type.len(),
    );
    sqlx::query(
        r#"
        SELECT pg_advisory_xact_lock(
            hashtextextended(
                format(
                    '%s:%s:%s',
                    octet_length(current_database()),
                    current_database(),
                    $1
                ),
                $2
            ) & 9223372036854775807::bigint
        )
        "#,
    )
    .bind(identity)
    .bind(PRIMARY_NAME_TUPLE_HASH_SEED)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to lock primary-name tuple for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })?;
    Ok(())
}

/// Exclude tuple operations while replacing the complete primary-name projection.
pub async fn lock_primary_names_current_replacement_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        SELECT
            pg_advisory_xact_lock(
                hashtextextended(
                    format(
                        '%s:%s',
                        octet_length(current_database()),
                        current_database()
                    ),
                    $1
                )
            ),
            set_config(
                'bigname.primary_names_current_replacement_fence',
                'on',
                true
            )
        "#,
    )
    .bind(PRIMARY_NAMES_CURRENT_REPLACEMENT_HASH_SEED)
    .execute(&mut **transaction)
    .await
    .context("failed to lock primary_names_current full replacement")?;
    Ok(())
}

async fn lock_primary_names_current_replacement_shared(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        SELECT pg_advisory_xact_lock_shared(
            hashtextextended(
                format(
                    '%s:%s',
                    octet_length(current_database()),
                    current_database()
                ),
                $1
            )
        )
        "#,
    )
    .bind(PRIMARY_NAMES_CURRENT_REPLACEMENT_HASH_SEED)
    .execute(&mut **transaction)
    .await
    .context("failed to join primary_names_current tuple-write fence")?;
    Ok(())
}

pub(super) async fn invalidate_all_verified_primary_name_outcomes_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM execution_cache_outcomes
        WHERE request_type = $1
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .execute(&mut **transaction)
    .await
    .context("failed to invalidate verified primary-name outcomes for projection replacement")
    .map(|result| result.rows_affected())
}
