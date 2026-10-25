//! The `address_records_current` page entry point for one coin type, and the page statements a
//! composed read runs (`families::records::resolves_to_serving`).
use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;

use super::{
    WalkPage,
    resolves_to::{
        AddressRecordCurrentEntry, AddressRecordsCoinSelector, AddressRecordsCurrentPage,
        AddressRecordsFilter, load_address_records_filtered_ids_from, load_sorted_entries,
    },
    resolves_to_evm::{AddressRecordEvmEntry, load_address_records_evm_page_from},
    source::RowSource,
    types::{
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
        AddressNamesCurrentSortedCursor, NameQuery,
    },
};

/// Load a bounded page of current names whose `addr:<coin_type>` record resolves to `address`.
///
/// `coin_type` is the decimal ENSIP-9/SLIP-44 coin type. `namespaces` restricts rows to those
/// public namespaces; `None` reads every namespace. `servable_namespaces` are the namespaces a
/// request that names none spans, where a record written under no name looks for its node's
/// registry pointer. `parent`, a normalized name, keeps only names
/// exactly one label below it. Sort, order, dedupe, and the keyset cursor
/// use the `address_names_current` vocabulary. The rows are composed
/// from the owned key families instead (`families::records::load_family_resolves_to_page`).
#[allow(clippy::too_many_arguments)]
pub async fn load_address_records_current_page(
    db: impl Into<crate::ReadDb<'_>>,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    servable_namespaces: &[String],
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    parent: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    crate::families::records::load_family_resolves_to_page(
        db,
        address,
        coin_type,
        namespaces,
        servable_namespaces,
        dedupe_by,
        q,
        authority,
        parent,
        sort,
        order,
        cursor,
        page_size,
    )
    .await
}

/// The page over `source`, on `conn`.
#[allow(clippy::too_many_arguments)]
async fn load_address_records_page_from(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
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

/// A resolves-to page request; `coin_type` is `None` for `coin_type=evm`.
pub(crate) struct AddressRecordsQuery<'a> {
    pub(crate) address: &'a str,
    pub(crate) coin_type: Option<&'a str>,
    pub(crate) namespaces: Option<&'a [String]>,
    /// The namespaces a request that names none spans.
    pub(crate) servable_namespaces: &'a [String],
    pub(crate) dedupe_by: AddressNamesCurrentDedupe,
    pub(crate) q: Option<NameQuery<'a>>,
    pub(crate) authority: Option<&'a [&'a str]>,
    pub(crate) parent: Option<&'a str>,
    pub(crate) sort: AddressNamesCurrentSort,
    pub(crate) order: AddressNamesCurrentOrder,
    pub(crate) cursor: Option<&'a AddressNamesCurrentSortedCursor>,
    pub(crate) page_size: u64,
}

impl AddressRecordsQuery<'_> {
    fn source<'a>(&'a self, rows: &'a Value, names: &'a Value) -> RowSource<'a> {
        RowSource::Composed {
            rows,
            names,
            parent: self.parent,
        }
    }

    /// The names among `rows` with a row the page's filters keep.
    pub(crate) async fn kept(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<Vec<String>> {
        load_address_records_filtered_ids_from(
            conn,
            self.source(rows, names),
            self.address,
            self.coin_type,
            self.namespaces,
            self.q,
            self.authority,
        )
        .await
    }
}

/// The single-coin page statement of `query`.
pub(crate) struct SingleCoinPage<'a> {
    pub(crate) query: &'a AddressRecordsQuery<'a>,
    pub(crate) coin_type: &'a str,
}

impl WalkPage for SingleCoinPage<'_> {
    type Entry = AddressRecordCurrentEntry;

    async fn page(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<(Vec<Self::Entry>, Option<AddressNamesCurrentSortedCursor>)> {
        let query = self.query;
        let page = load_address_records_page_from(
            conn,
            query.source(rows, names),
            query.address,
            self.coin_type,
            query.namespaces,
            query.dedupe_by,
            query.q,
            query.authority,
            query.sort,
            query.order,
            query.cursor,
            query.page_size,
        )
        .await?;
        Ok((page.entries, page.next_cursor))
    }

    fn entry_name(entry: &Self::Entry) -> &str {
        &entry.logical_name_id
    }
}

/// The `coin_type=evm` page statement of the query.
pub(crate) struct EvmCoinsPage<'a>(pub(crate) &'a AddressRecordsQuery<'a>);

impl WalkPage for EvmCoinsPage<'_> {
    type Entry = AddressRecordEvmEntry;

    async fn page(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<(Vec<Self::Entry>, Option<AddressNamesCurrentSortedCursor>)> {
        let query = self.0;
        let page = load_address_records_evm_page_from(
            conn,
            query.source(rows, names),
            query.address,
            query.namespaces,
            query.dedupe_by,
            query.q,
            query.authority,
            query.sort,
            query.order,
            query.cursor,
            query.page_size,
        )
        .await?;
        Ok((page.entries, page.next_cursor))
    }

    fn entry_name(entry: &Self::Entry) -> &str {
        &entry.entry.logical_name_id
    }
}
