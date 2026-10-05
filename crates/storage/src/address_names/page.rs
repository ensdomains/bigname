use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_address_names_current_summary,
    query::{
        push_address_names_current_cursor_after, push_address_names_current_grouped_entries_cte,
        push_address_names_current_order, push_address_names_current_sortable_entries_cte,
    },
    source::RowSource,
    types::{
        AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentCursor,
        AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentPage,
        AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedPage,
        AddressNamesCurrentSummary, NameQuery,
    },
};
mod cursor;

use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};
use cursor::{
    address_names_current_legacy_cursor_from_sorted,
    address_names_current_sorted_cursor_from_entry,
    address_names_current_sorted_cursor_from_legacy, decode_address_name_current_sorted_entry,
    ensure_address_names_current_cursor_matches_sort,
};

/// Load a bounded page of grouped current address-name entries from the default canonical read set.
pub async fn load_address_names_current_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relation: Option<AddressNameRelation>,
    dedupe_by: AddressNamesCurrentDedupe,
    cursor: Option<&AddressNamesCurrentCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentPage> {
    let sorted_cursor = cursor.map(address_names_current_sorted_cursor_from_legacy);
    let relations = relation.into_iter().collect::<Vec<_>>();
    let relations = (!relations.is_empty()).then_some(relations.as_slice());
    let page = load_address_names_current_page_sorted_for_relations(
        pool,
        address,
        namespace,
        relations,
        dedupe_by,
        None,
        None,
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        sorted_cursor.as_ref(),
        page_size,
    )
    .await?;

    let next_cursor = page
        .next_cursor
        .map(address_names_current_legacy_cursor_from_sorted)
        .transpose()?;

    Ok(AddressNamesCurrentPage {
        entries: page.entries,
        next_cursor,
        summary: page.summary,
    })
}

/// Load a bounded page of grouped current address-name entries with v2 set-valued relation controls.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_names_current_page_sorted_for_relations(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    load_address_names_current_page_filtered(
        pool, address, namespace, relations, dedupe_by, q, authority, None, None, sort, order,
        cursor, page_size,
    )
    .await
}

/// The page is read from the owned key families. A cursor holds a sort position; its
/// original row need not still exist in the current collection. `parent`, a normalized name,
/// keeps only names exactly one label below it.
#[allow(clippy::too_many_arguments)]
pub async fn load_address_names_current_page_filtered(
    db: impl Into<crate::ReadDb<'_>>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
    parent: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    crate::families::records::load_family_address_names_page(
        db,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
        parent,
        sort,
        order,
        cursor,
        page_size,
    )
    .await
}

/// The page over `source`: the summary and the page, two statements on
/// `conn`, which a composed read holds in one snapshot.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_address_names_page_from(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    let summary = load_address_names_current_summary(
        &mut *conn,
        source,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
    )
    .await?;
    let (entries, next_cursor) = load_address_names_page_entries_from(
        conn,
        source,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
        sort,
        order,
        cursor,
        page_size,
    )
    .await?;
    Ok(AddressNamesCurrentSortedPage {
        entries,
        next_cursor,
        summary,
    })
}

/// The page statement of [`load_address_names_page_from`] alone: the page's entries and the
/// cursor after its last entry when more follow, with no summary.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_address_names_page_entries_from(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<(
    Vec<AddressNameCurrentEntry>,
    Option<AddressNamesCurrentSortedCursor>,
)> {
    let page_size = checked_page_size_usize(
        page_size,
        "address_names_current page_size must be positive",
        "address_names_current page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "address_names_current page_size is too large",
        "address_names_current page_size exceeds SQL limit",
    )?;
    if let Some(cursor) = cursor {
        ensure_address_names_current_cursor_matches_sort(sort, cursor)?;
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        source,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
    );
    push_address_names_current_sortable_entries_cte(&mut builder, source.names(), sort);
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
            resource_id,
            token_lineage_id,
            binding_kind,
            relations,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at,
            served_owner,
            served_authority,
            served_lifecycle_shadow,
        "#,
    );
    if sort.is_timestamp() {
        builder.push("sort_timestamp");
    } else {
        builder.push("NULL::NUMERIC AS sort_timestamp");
    }
    builder.push(" FROM ");
    builder.push(if sort.is_timestamp() {
        "sortable_entries"
    } else {
        "entries"
    });
    builder.push(" WHERE TRUE ");
    if let Some(cursor) = cursor {
        push_address_names_current_cursor_after(&mut builder, sort, order, cursor);
    }
    push_address_names_current_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .with_context(|| {
            let mut parts =
                load_context_parts(address, namespace, relations, dedupe_by, q, authority);
            parts.push(format!("sort {}", sort.as_str()));
            parts.push(format!("order {}", order.as_str()));
            format!(
                "failed to load address_names_current grouped page for {}",
                parts.join(" ")
            )
        })?;

    let rows = rows
        .into_iter()
        .map(|row| decode_address_name_current_sorted_entry(row, sort))
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        address_names_current_sorted_cursor_from_entry(row, sort)
    });
    let mut entries: Vec<AddressNameCurrentEntry> = rows.into_iter().map(|row| row.entry).collect();
    attach_served_managers(&mut entries, source);
    Ok((entries, next_cursor))
}

/// The names among `source`'s rows with a row the page's filters keep. A name with none adds
/// nothing to any page group, so a walk can drop its rows.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_address_names_filtered_ids_from(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
) -> Result<Vec<String>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        source,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
    );
    builder.push(" SELECT DISTINCT logical_name_id FROM filtered");
    builder
        .build_query_scalar()
        .fetch_all(conn)
        .await
        .context("failed to filter the walked address-name rows")
}

/// A surface-less registry child's `served_manager` from its composed relation rows, all of which
/// carry the same one, its registry owner (`families::records::registry_children`). It rides
/// beside the page query rather than through it: query.rs is an input of the interpreter content
/// hash (crates/content-hash/src/compute.rs, `SEMANTIC_SOURCE_FILES`).
fn attach_served_managers(entries: &mut [AddressNameCurrentEntry], source: RowSource<'_>) {
    let RowSource::Composed { rows, .. } = source;
    let mut managers: BTreeMap<&str, &str> = BTreeMap::new();
    for row in rows.as_array().into_iter().flatten() {
        if row["registry_child"] != true {
            continue;
        }
        let (Some(id), Some(manager)) = (
            row["logical_name_id"].as_str(),
            row["served_manager"].as_str(),
        ) else {
            continue;
        };
        let previous = managers.insert(id, manager);
        debug_assert!(
            previous.is_none_or(|previous| previous == manager),
            "registry child {id} carries two served managers"
        );
    }
    for entry in entries.iter_mut().filter(|entry| entry.is_registry_child()) {
        entry.served_manager = managers
            .get(entry.logical_name_id.as_str())
            .map(|manager| (*manager).to_owned());
        debug_assert!(
            entry.served_manager.is_some(),
            "registry child {} has no served manager",
            entry.logical_name_id
        );
    }
}

fn load_context_parts(
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
) -> Vec<String> {
    let mut parts = vec![format!("address {address}")];
    if let Some(namespace) = namespace {
        parts.push(format!("namespace {namespace}"));
    }
    if let Some(relations) = relations.filter(|relations| !relations.is_empty()) {
        parts.push(format!(
            "relations {}",
            relations
                .iter()
                .map(|relation| relation.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(q) = q {
        parts.push(format!("q {q}"));
    }
    if let Some(authority) = authority {
        parts.push(format!("authority {}", authority.join(",")));
    }
    parts.push(format!("dedupe_by {}", dedupe_by.as_str()));
    parts
}

#[allow(clippy::too_many_arguments)]
async fn load_address_names_current_summary(
    conn: &mut PgConnection,
    source: RowSource<'_>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
) -> Result<AddressNamesCurrentSummary> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_address_names_current_grouped_entries_cte(
        &mut builder,
        source,
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
    );
    builder.push(
        r#",
        ordered_entries AS (
            SELECT
                entries.*,
                ROW_NUMBER() OVER (
                    ORDER BY
                        canonical_display_name ASC,
                        logical_name_id ASC,
                        resource_id::TEXT ASC
                ) AS entry_position
            FROM entries
        ),
        normalized_event_id_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'normalized_event_ids') = 'array'
                    THEN provenance -> 'normalized_event_ids'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        raw_fact_ref_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'raw_fact_refs') = 'array'
                    THEN provenance -> 'raw_fact_refs'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        manifest_version_values AS (
            SELECT DISTINCT ON (value)
                value,
                entry_position,
                value_position
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(
                CASE
                    WHEN JSONB_TYPEOF(provenance -> 'manifest_versions') = 'array'
                    THEN provenance -> 'manifest_versions'
                    ELSE '[]'::JSONB
                END
            ) WITH ORDINALITY AS provenance_values(value, value_position)
            ORDER BY value, entry_position ASC, value_position ASC
        ),
        chain_position_values AS (
            SELECT
                slot,
                position_value,
                (position_value ->> 'block_number')::BIGINT AS block_number,
                position_value ->> 'block_hash' AS block_hash
            FROM ordered_entries
            CROSS JOIN LATERAL JSONB_EACH(chain_positions) AS positions(slot, position_value)
            WHERE position_value ? 'chain_id'
              AND position_value ? 'block_number'
              AND position_value ? 'block_hash'
              AND position_value ? 'timestamp'
              AND JSONB_TYPEOF(position_value -> 'block_number') = 'number'
        ),
        chain_position_heads AS (
            SELECT DISTINCT ON (slot)
                slot,
                position_value
            FROM chain_position_values
            ORDER BY slot, block_number DESC, block_hash DESC
        )
        SELECT
            (SELECT COUNT(*)::BIGINT FROM ordered_entries) AS grouped_entry_count,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM normalized_event_id_values
                ),
                '[]'::JSONB
            ) AS provenance_normalized_event_ids,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM raw_fact_ref_values
                ),
                '[]'::JSONB
            ) AS provenance_raw_fact_refs,
            COALESCE(
                (
                    SELECT JSONB_AGG(value ORDER BY entry_position ASC, value_position ASC)
                    FROM manifest_version_values
                ),
                '[]'::JSONB
            ) AS provenance_manifest_versions,
            (
                SELECT provenance ->> 'derivation_kind'
                FROM ordered_entries
                WHERE JSONB_TYPEOF(provenance -> 'derivation_kind') = 'string'
                ORDER BY entry_position ASC
                LIMIT 1
            ) AS provenance_derivation_kind,
            COALESCE(
                (
                    SELECT JSONB_OBJECT_AGG(slot, position_value ORDER BY slot)
                    FROM chain_position_heads
                ),
                '{}'::JSONB
            ) AS chain_positions,
            CASE
                WHEN (SELECT COUNT(*) FROM ordered_entries) = 0 THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE COALESCE(canonicality_summary ->> 'status', '') NOT IN ('safe', 'finalized')
                ) THEN 'head'
                WHEN EXISTS (
                    SELECT 1
                    FROM ordered_entries
                    WHERE canonicality_summary ->> 'status' = 'safe'
                ) THEN 'safe'
                ELSE 'finalized'
            END AS consistency,
            (SELECT MAX(last_recomputed_at) FROM ordered_entries) AS last_recomputed_at
        "#,
    );

    let row = builder.build().fetch_one(conn).await.with_context(|| {
        let parts = load_context_parts(address, namespace, relations, dedupe_by, q, authority);
        format!(
            "failed to load address_names_current grouped summary for {}",
            parts.join(" ")
        )
    })?;

    decode_address_names_current_summary(row)
}

#[cfg(test)]
mod served_manager_tests;
