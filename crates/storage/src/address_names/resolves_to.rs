//! Reads over `address_records_current`, the reverse index of current `addr:<coin_type>`
//! resolver records ("names that resolve to this address").
//!
//! The page shares its sort vocabulary, keyset cursor, and dedupe modes with the
//! `address_names_current` collection so one API route can serve both relations, but the read
//! set is keyed by the requested coin type and applies the ENSIP-19 default-address fallback the
//! forward record read applies (`bigname_domain::resolver_read::evaluate_indexed_record`).

use anyhow::{Context, Result, bail};
use bigname_domain::resolver_read::ensip19_default_fallback_target;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    query::{
        escape_like_pattern, push_address_names_current_cursor_after,
        push_address_names_current_cursor_identity_match,
        push_address_names_current_cursor_sort_value_match, push_address_names_current_order,
        push_address_names_current_sortable_entries_cte,
    },
    resolves_to_evm::{
        EVM_OUTER_COLUMNS, EvmFacets, decode_evm_facets, push_evm_address_rows_cte,
        push_evm_entries_ctes,
    },
    resolves_to_filter::ADDRESS_RECORDS_CURRENT_READ_FILTER,
    types::{
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
        AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    },
};
use crate::{
    SurfaceBindingKind,
    projection_helpers::{
        checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
    },
};

/// Record key of the ENSIP-19 default EVM address entry.
pub const ENSIP19_DEFAULT_ADDRESS_RECORD_KEY: &str = "addr:2147483648";

/// One current name whose `addr` record resolves to the requested address for the requested
/// coin type. `record_key` names the inventory entry that answered: the exact
/// `addr:<coin_type>` entry, or `addr:2147483648` when the ENSIP-19 default address answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressRecordCurrentEntry {
    pub address: String,
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    /// Authority identity stays absent when the name only has a record-serving resource.
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Option<Uuid>,
    pub record_resource_id: Uuid,
    pub binding_kind: Option<SurfaceBindingKind>,
    /// The requested coin type, not the stored row's coin type. On a `coin_type=evm` read it is
    /// the representative row's stored coin type; the matches are carried beside the entry.
    pub coin_type: String,
    pub record_key: String,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Bounded sorted page of names resolving to an address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddressRecordsCurrentPage {
    pub entries: Vec<AddressRecordCurrentEntry>,
    pub next_cursor: Option<AddressNamesCurrentSortedCursor>,
}

/// Load a bounded page of current names whose `addr:<coin_type>` record resolves to `address`.
///
/// `coin_type` is the decimal ENSIP-9/SLIP-44 coin type. `namespaces` restricts rows to those
/// public namespaces; `None` reads every namespace. Sort, order, dedupe, and the keyset cursor
/// use the `address_names_current` vocabulary.
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
    let filter = AddressRecordsFilter {
        address,
        coins: AddressRecordsCoinSelector::Single(coin_type),
        namespaces,
        dedupe_by,
        q,
        authority,
    };
    let (rows, next_cursor) =
        load_sorted_entries(pool, &filter, sort, order, cursor, page_size).await?;
    Ok(AddressRecordsCurrentPage {
        entries: rows.into_iter().map(|row| row.entry).collect(),
        next_cursor,
    })
}

/// Run one page statement (after validating a continuation cursor) and split the keyset page.
pub(super) async fn load_sorted_entries(
    pool: &PgPool,
    filter: &AddressRecordsFilter<'_>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<(Vec<SortedEntry>, Option<AddressNamesCurrentSortedCursor>)> {
    let page_size = checked_page_size_usize(
        page_size,
        "address_records_current page_size must be positive",
        "address_records_current page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "address_records_current page_size is too large",
        "address_records_current page_size exceeds SQL limit",
    )?;

    if let Some(cursor) = cursor {
        ensure_cursor_matches_sort(sort, cursor)?;
        ensure_cursor_exists(pool, filter, sort, cursor).await?;
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_page_statement(&mut builder, filter, sort, order, cursor, page_limit);
    let rows = builder.build().fetch_all(pool).await.with_context(|| {
        format!(
            "failed to load address_records_current page for {} sort {} order {}",
            filter.context(),
            sort.as_str(),
            order.as_str()
        )
    })?;
    let rows = rows
        .into_iter()
        .map(|row| decode_sorted_entry(row, filter.coins, sort))
        .collect::<Result<Vec<_>>>()?;
    Ok(split_keyset_page(rows, page_size, |row| {
        AddressNamesCurrentSortedCursor {
            sort_value: match sort {
                AddressNamesCurrentSort::Name => AddressNamesCurrentSortedCursorValue::Name(
                    row.entry.canonical_display_name.clone(),
                ),
                AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt => {
                    AddressNamesCurrentSortedCursorValue::Timestamp(row.sort_timestamp)
                }
            },
            logical_name_id: row.entry.logical_name_id.clone(),
            resource_id: row
                .entry
                .resource_id
                .unwrap_or(row.entry.record_resource_id),
        }
    }))
}

/// The page statement: the filtered and grouped entries, the cursor predicate, order, and limit.
pub(super) fn push_page_statement<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &AddressRecordsFilter<'a>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&'a AddressNamesCurrentSortedCursor>,
    page_limit: i64,
) {
    push_entries_cte(builder, filter);
    push_address_names_current_sortable_entries_cte(builder, sort);
    builder.push(
        r#"
        SELECT
            address,
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            authority_resource_id,
            resource_id,
            record_resource_id,
            binding_kind,
            record_key,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at,
        "#,
    );
    if filter.coins == AddressRecordsCoinSelector::Evm {
        builder.push(EVM_OUTER_COLUMNS);
    }
    if sort.is_timestamp() {
        builder.push("sort_timestamp");
    } else {
        builder.push("NULL::TIMESTAMPTZ AS sort_timestamp");
    }
    builder.push(" FROM ");
    builder.push(if sort.is_timestamp() {
        "sortable_entries"
    } else {
        "entries"
    });
    builder.push(" WHERE TRUE ");
    if let Some(cursor) = cursor {
        push_address_names_current_cursor_after(builder, sort, order, cursor);
    }
    push_address_names_current_order(builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);
}

/// Which stored rows a read matches: one requested coin type with the ENSIP-19 default-address
/// fallback, or every stored row whose coin type is an EVM coin type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AddressRecordsCoinSelector<'a> {
    Single(&'a str),
    Evm,
}

impl AddressRecordsCoinSelector<'_> {
    fn as_str(&self) -> &str {
        match self {
            Self::Single(coin_type) => coin_type,
            Self::Evm => "evm",
        }
    }
}

pub(super) struct AddressRecordsFilter<'a> {
    pub(super) address: &'a str,
    pub(super) coins: AddressRecordsCoinSelector<'a>,
    pub(super) namespaces: Option<&'a [String]>,
    pub(super) dedupe_by: AddressNamesCurrentDedupe,
    pub(super) q: Option<&'a str>,
    pub(super) authority: Option<&'a str>,
}

impl AddressRecordsFilter<'_> {
    fn context(&self) -> String {
        let mut parts = vec![
            format!("address {}", self.address),
            format!("coin_type {}", self.coins.as_str()),
        ];
        if let Some(namespaces) = self.namespaces {
            parts.push(format!("namespaces {}", namespaces.join(",")));
        }
        if let Some(q) = self.q {
            parts.push(format!("q {q}"));
        }
        if let Some(authority) = self.authority {
            parts.push(format!("authority {authority}"));
        }
        parts.push(format!("dedupe_by {}", self.dedupe_by.as_str()));
        parts.join(" ")
    }
}

/// Whether a request for `coin_type` may be answered by the ENSIP-19 default EVM address when
/// no exact entry shadows it. Mirrors `ensip19_default_fallback_target`; a non-numeric or
/// out-of-range value never falls back.
fn ensip19_default_address_may_answer(coin_type: &str) -> bool {
    coin_type
        .parse::<u64>()
        .is_ok_and(ensip19_default_fallback_target)
}

fn push_entries_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &AddressRecordsFilter<'a>,
) {
    builder.push("\n        WITH ");
    if filter.coins == AddressRecordsCoinSelector::Evm {
        push_evm_address_rows_cte(builder, filter.address);
    }
    builder.push(
        r#"filtered AS (
            SELECT
                arc.address,
                arc.logical_name_id,
                arc.namespace,
                arc.raw_name AS canonical_display_name,
                arc.raw_name AS normalized_name,
                arc.namehash,
                arc.surface_binding_id,
                -- Sorting and dedupe need a stable key even when authority is absent; keep
                -- that key separate from the optional authority returned to callers.
                arc.resource_id AS authority_resource_id,
                COALESCE(arc.resource_id, arc.record_resource_id) AS resource_id,
                arc.record_resource_id,
                arc.binding_kind,
                arc.record_key,
                arc.provenance,
                CASE WHEN arc.support_status = 'supported'
                     THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                     ELSE jsonb_build_object(
                         'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                         'unsupported_reason', arc.unsupported_reason
                     ) END AS coverage,
                arc.chain_positions,
                arc.canonicality_summary,
                arc.manifest_version,
                arc.last_recomputed_at,
                "#,
    );
    match filter.coins {
        AddressRecordsCoinSelector::Single(coin_type) => {
            builder.push("CASE WHEN arc.coin_type = ");
            builder.push_bind(coin_type);
            builder.push(" THEN 0 ELSE 1 END AS record_rank");
        }
        AddressRecordsCoinSelector::Evm => {
            builder.push("arc.coin_type AS matched_coin_type");
        }
    }
    builder.push(match filter.coins {
        AddressRecordsCoinSelector::Single(_) => {
            "\n            FROM bigname_phase.address_records_current arc"
        }
        AddressRecordsCoinSelector::Evm => "\n            FROM evm_address_rows arc",
    });
    builder.push(
        r#"
            JOIN bigname_phase.name_surfaces surface
              ON surface.logical_name_id = arc.logical_name_id
            LEFT JOIN bigname_phase.resources resource
              ON resource.resource_id = arc.resource_id
            JOIN bigname_phase.resources record_resource
              ON record_resource.resource_id = arc.record_resource_id
            LEFT JOIN bigname_phase.surface_bindings binding
              ON binding.surface_binding_id = arc.surface_binding_id
            JOIN bigname_phase.chain_lineage surface_lineage
              ON surface_lineage.chain_id = surface.chain_id
             AND surface_lineage.block_hash = surface.block_hash
            LEFT JOIN bigname_phase.chain_lineage resource_lineage
              ON resource_lineage.chain_id = resource.chain_id
             AND resource_lineage.block_hash = resource.block_hash
            JOIN bigname_phase.chain_lineage record_resource_lineage
              ON record_resource_lineage.chain_id = record_resource.chain_id
             AND record_resource_lineage.block_hash = record_resource.block_hash
            LEFT JOIN bigname_phase.chain_lineage binding_lineage
              ON binding_lineage.chain_id = binding.chain_id
             AND binding_lineage.block_hash = binding.block_hash
            WHERE arc.address = "#,
    );
    builder.push_bind(filter.address);
    if let Some(namespaces) = filter.namespaces {
        builder.push(" AND arc.namespace = ANY(");
        builder.push_bind(namespaces);
        builder.push(")");
    }
    match filter.coins {
        // Exact entry for the requested coin type, or the ENSIP-19 default EVM address when the
        // serving resolver declares that read feature and no exact entry shadows this coin type.
        AddressRecordsCoinSelector::Single(coin_type) => {
            builder.push(" AND (arc.coin_type = ");
            builder.push_bind(coin_type);
            builder.push(" OR (");
            builder.push_bind(ensip19_default_address_may_answer(coin_type));
            builder.push(" AND arc.record_key = ");
            builder.push_bind(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY);
            builder.push(
                " AND arc.provenance ->> 'ensip19_default_address' = 'true' \
                  AND NOT COALESCE(arc.provenance -> 'shadowed_coin_types' ? ",
            );
            builder.push_bind(coin_type);
            builder.push(", false)))");
        }
        // Applied while reading `evm_address_rows`.
        AddressRecordsCoinSelector::Evm => {}
    }
    if let Some(prefix) = filter.q {
        builder.push(" AND arc.raw_name LIKE ");
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(authority) = filter.authority {
        crate::name_current::push_public_authority_filter(
            builder,
            "arc.logical_name_id",
            authority,
        );
    }
    builder.push(ADDRESS_RECORDS_CURRENT_READ_FILTER);
    builder.push(
        r#"
        )"#,
    );
    if filter.coins == AddressRecordsCoinSelector::Evm {
        push_evm_entries_ctes(builder, filter.dedupe_by);
        return;
    }
    builder.push(
        r#",
        entries AS (
            SELECT DISTINCT ON ("#,
    );
    let dedupe_key = match filter.dedupe_by {
        AddressNamesCurrentDedupe::Surface => "address, logical_name_id",
        AddressNamesCurrentDedupe::Resource => "address, resource_id",
    };
    builder.push(dedupe_key);
    builder.push(
        r#")
                address,
                logical_name_id,
                namespace,
                canonical_display_name,
                normalized_name,
                namehash,
                surface_binding_id,
                authority_resource_id,
                resource_id,
                record_resource_id,
                binding_kind,
                record_key,
                provenance,
                coverage,
                chain_positions,
                canonicality_summary,
                manifest_version,
                last_recomputed_at
            FROM filtered
            ORDER BY "#,
    );
    builder.push(dedupe_key);
    builder.push(
        r#", record_rank ASC, canonical_display_name ASC, logical_name_id ASC
        )
        "#,
    );
}

async fn ensure_cursor_exists(
    pool: &PgPool,
    filter: &AddressRecordsFilter<'_>,
    sort: AddressNamesCurrentSort,
    cursor: &AddressNamesCurrentSortedCursor,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_cursor_exists_statement(&mut builder, filter, sort, cursor);
    let row = builder.build().fetch_one(pool).await.with_context(|| {
        format!(
            "failed to validate address_records_current page cursor for {} sort {}",
            filter.context(),
            sort.as_str()
        )
    })?;
    if crate::sql_row::get::<bool>(&row, "cursor_exists")? {
        Ok(())
    } else {
        bail!("address_records_current page cursor does not match a grouped entry")
    }
}

/// The continuation check: the cursor row must still be one of the grouped entries.
pub(super) fn push_cursor_exists_statement<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &AddressRecordsFilter<'a>,
    sort: AddressNamesCurrentSort,
    cursor: &'a AddressNamesCurrentSortedCursor,
) {
    push_entries_cte(builder, filter);
    push_address_names_current_sortable_entries_cte(builder, sort);
    builder.push(" SELECT EXISTS (SELECT 1 FROM ");
    builder.push(if sort.is_timestamp() {
        "sortable_entries"
    } else {
        "entries"
    });
    builder.push(" WHERE ");
    push_address_names_current_cursor_identity_match(builder, cursor);
    push_address_names_current_cursor_sort_value_match(builder, sort, cursor);
    builder.push(") AS cursor_exists");
}

fn ensure_cursor_matches_sort(
    sort: AddressNamesCurrentSort,
    cursor: &AddressNamesCurrentSortedCursor,
) -> Result<()> {
    match (sort, &cursor.sort_value) {
        (AddressNamesCurrentSort::Name, AddressNamesCurrentSortedCursorValue::Name(_))
        | (
            AddressNamesCurrentSort::ExpiresAt | AddressNamesCurrentSort::RegisteredAt,
            AddressNamesCurrentSortedCursorValue::Timestamp(_),
        ) => Ok(()),
        _ => bail!(
            "address_records_current page cursor sort value does not match sort {}",
            sort.as_str()
        ),
    }
}

pub(super) struct SortedEntry {
    pub(super) entry: AddressRecordCurrentEntry,
    sort_timestamp: Option<OffsetDateTime>,
    pub(super) evm: Option<EvmFacets>,
}

fn decode_sorted_entry(
    row: PgRow,
    coins: AddressRecordsCoinSelector<'_>,
    sort: AddressNamesCurrentSort,
) -> Result<SortedEntry> {
    // A single-coin row reports the requested coin type; an evm row reports its representative's
    // own stored coin type beside the group's facets.
    let (coin_type, evm) = match coins {
        AddressRecordsCoinSelector::Single(coin_type) => (coin_type.to_owned(), None),
        AddressRecordsCoinSelector::Evm => {
            let (coin_type, facets) = decode_evm_facets(&row)?;
            (coin_type, Some(facets))
        }
    };
    let sort_timestamp = sort
        .is_timestamp()
        .then(|| crate::sql_row::get::<Option<OffsetDateTime>>(&row, "sort_timestamp"))
        .transpose()?
        .flatten();
    let binding_kind = crate::sql_row::get(&row, "binding_kind")?;
    let entry = AddressRecordCurrentEntry {
        address: crate::sql_row::get(&row, "address")?,
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        resource_id: crate::sql_row::get(&row, "authority_resource_id")?,
        record_resource_id: crate::sql_row::get(&row, "record_resource_id")?,
        binding_kind,
        coin_type,
        record_key: crate::sql_row::get(&row, "record_key")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    };
    Ok(SortedEntry {
        entry,
        sort_timestamp,
        evm,
    })
}

#[cfg(test)]
mod tests {
    use super::ensip19_default_address_may_answer;

    #[test]
    fn default_address_answers_only_evm_coin_types() {
        assert!(ensip19_default_address_may_answer("60"));
        assert!(ensip19_default_address_may_answer("2147483658"));
        assert!(ensip19_default_address_may_answer("2147492101"));
        // The default entry itself is matched exactly, never by fallback.
        assert!(!ensip19_default_address_may_answer("2147483648"));
        assert!(!ensip19_default_address_may_answer("0"));
        assert!(!ensip19_default_address_may_answer("61"));
        assert!(!ensip19_default_address_may_answer("4294967296"));
        assert!(!ensip19_default_address_may_answer("abc"));
    }
}
