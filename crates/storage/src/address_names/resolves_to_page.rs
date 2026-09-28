//! The `address_records_current` page entry point for one coin type, and its switch branch.
use anyhow::Result;
use sqlx::{PgConnection, PgPool};

use super::{
    resolves_to::{
        AddressRecordsCoinSelector, AddressRecordsCurrentPage, AddressRecordsFilter,
        load_sorted_entries,
    },
    source::RowSource,
    types::{
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
        AddressNamesCurrentSortedCursor,
    },
};

/// Load a bounded page of current names whose `addr:<coin_type>` record resolves to `address`.
///
/// `coin_type` is the decimal ENSIP-9/SLIP-44 coin type. `namespaces` restricts rows to those
/// public namespaces; `None` reads every namespace. Sort, order, dedupe, and the keyset cursor
/// use the `address_names_current` vocabulary. The rows are composed
/// from the owned key families instead (`families::records::load_family_resolves_to_page`).
#[allow(clippy::too_many_arguments)]
pub async fn load_address_records_current_page(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    crate::families::records::load_family_resolves_to_page(
        pool, address, coin_type, namespaces, dedupe_by, q, authority, sort, order, cursor,
        page_size,
    )
    .await
}

/// The page over `source`, on `conn`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_address_records_page_from(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    let filter = AddressRecordsFilter {
        address,
        coins: AddressRecordsCoinSelector::Single(coin_type),
        namespaces,
        dedupe_by,
        q,
        authority,
        source,
    };
    let (rows, next_cursor) =
        load_sorted_entries(conn, &filter, sort, order, cursor, page_size).await?;
    Ok(AddressRecordsCurrentPage {
        entries: rows.into_iter().map(|row| row.entry).collect(),
        next_cursor,
    })
}
