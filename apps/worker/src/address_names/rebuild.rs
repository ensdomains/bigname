use std::collections::BTreeSet;

use anyhow::Result;
use bigname_storage::upsert_address_names_current_rows;
use sqlx::PgPool;

use super::{
    AddressNamesCurrentRebuildSummary,
    cleanup::{
        delete_stale_address_names_current_rows,
        delete_stale_address_names_current_rows_for_address,
    },
    load::load_current_bindings,
    projection::build_rows,
    util::normalize_address,
};

pub async fn rebuild_address_names_current(
    pool: &PgPool,
    address: Option<&str>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    match address {
        Some(address) => rebuild_one_address(pool, address).await,
        None => rebuild_all_addresses(pool).await,
    }
}

async fn rebuild_all_addresses(pool: &PgPool) -> Result<AddressNamesCurrentRebuildSummary> {
    let bindings = load_current_bindings(pool).await?;
    let rows = build_rows(pool, &bindings, None).await?;
    let requested_address_count = rows
        .iter()
        .map(|row| row.address.clone())
        .collect::<BTreeSet<_>>()
        .len();
    let upserted_row_count = upsert_address_names_current_rows(pool, &rows).await?.len();
    let deleted_row_count = delete_stale_address_names_current_rows(pool, &rows).await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_address(
    pool: &PgPool,
    address: &str,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let normalized_address = normalize_address(address);
    let bindings = load_current_bindings(pool).await?;
    let rows = build_rows(pool, &bindings, Some(normalized_address.as_str())).await?;
    let upserted_row_count = upsert_address_names_current_rows(pool, &rows).await?.len();
    let deleted_row_count =
        delete_stale_address_names_current_rows_for_address(pool, &normalized_address, &rows)
            .await?;

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}
