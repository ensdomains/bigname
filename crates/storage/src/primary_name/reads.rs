use std::collections::BTreeMap;

use super::types::{PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};
use anyhow::Result;
use sqlx::PgPool;

/// Load one declared primary-name claim-state row by exact address, namespace, and coin_type.
pub async fn load_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentRow>> {
    load_primary_name_current_snapshot(pool, address, namespace, coin_type)
        .await
        .map(|snapshot| snapshot.map(|snapshot| snapshot.row))
}

/// Load one declared primary-name claim snapshot by exact address, namespace, and coin_type.
/// Under the publication switch the claim is read from the families instead
/// (`families::records::load_family_primary_name_snapshot`, TYR-36 step 7b).
pub async fn load_primary_name_current_snapshot(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    crate::families::records::load_family_primary_name_snapshot(pool, address, namespace, coin_type)
        .await
}

/// Load the declared primary-name claim snapshots for one address and several exact
/// `(namespace, coin_type)` keys in one statement, with the same canonicality, hydration-fallback,
/// and decoding rules as [`load_primary_name_current_snapshot`]. Keys without a readable row are
/// absent from the result.
pub async fn load_primary_name_current_snapshots(
    pool: &PgPool,
    address: &str,
    keys: &[(String, String)],
) -> Result<BTreeMap<(String, String), PrimaryNameCurrentSnapshot>> {
    crate::families::records::load_family_primary_name_snapshots(pool, address, keys).await
}
