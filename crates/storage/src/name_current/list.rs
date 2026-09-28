use anyhow::{Context, Result};
use sqlx::{PgExecutor, Postgres, QueryBuilder, postgres::PgRow, types::time::OffsetDateTime};

use super::{NameCurrentRow, decode_name_current_row};
use crate::{
    AddressNameRelation,
    projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentListSort {
    Name,
    ExpiryDate,
    RegistrationDate,
    CreatedAt,
}

impl NameCurrentListSort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiryDate => "expiry_date",
            Self::RegistrationDate => "registration_date",
            Self::CreatedAt => "created_at",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentListOrder {
    Asc,
    Desc,
}

impl NameCurrentListOrder {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentAddressRelationFilter {
    Relation(AddressNameRelation),
    Any,
}

impl NameCurrentAddressRelationFilter {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Relation(relation) => relation.as_str(),
            Self::Any => "any",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentAddressFilter {
    pub address: String,
    pub relation: NameCurrentAddressRelationFilter,
    /// When set (and non-empty), the address-membership CTE matches any address in this list
    /// (`anc.address = ANY($list)`), superseding `address`. Backs the subgraph `owner_in` filter.
    pub addresses: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentListFilter {
    pub namespace: Option<String>,
    /// Namespace set for public search reads. When non-empty, binds `nc.namespace = ANY($list)`
    /// and supersedes `namespace`, keeping a storage-ordered page in one DB query.
    pub namespaces: Option<Vec<String>>,
    pub name: Option<String>,
    pub prefix: Option<String>,
    pub contains: Option<String>,
    /// ASCII-fold this already-valid name fragment before matching stored normalized bytes.
    pub contains_nocase: Option<String>,
    pub resolver: Option<String>,
    pub address: Option<NameCurrentAddressFilter>,
    /// When `Some(true)`, restrict to names whose declared registration authority is the ENS v2
    /// registry (`declared_summary.registration.authority_kind = 'ens_v2_registry'`). Backs the
    /// subgraph `isMigrated` filter. `Some(false)` / `None` apply no migration predicate.
    pub is_migrated: Option<bool>,
    /// When true, omit every row the exact-name projection does not support. A collection with no
    /// row-local status field cannot report why a name is unsupported, so it omits the name rather
    /// than serving registration fields no selected authority backs.
    pub supported_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NameCurrentListCursorValue {
    Name(String),
    Timestamp(Option<OffsetDateTime>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListCursor {
    pub sort_value: NameCurrentListCursorValue,
    pub namespace: String,
    pub normalized_name: String,
    pub namehash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListRow {
    pub row: NameCurrentRow,
    pub labelhash: Option<String>,
    pub token_id: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    pub created_at: Option<OffsetDateTime>,
    pub registration_date: Option<OffsetDateTime>,
    pub expiry_date: Option<OffsetDateTime>,
    pub resolver_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentListPage {
    pub rows: Vec<NameCurrentListRow>,
    pub next_cursor: Option<NameCurrentListCursor>,
    pub total_count: Option<u64>,
}

/// Shared projection of the derived list columns from the `filtered_names` CTE. Every list/count
/// read path appends its own `WHERE` / `ORDER BY` / `LIMIT` after this so the column shape that
/// [`decode_name_current_list_row`] decodes stays identical across cursor, offset, and by-namehash
/// reads.
pub(super) const NAME_CURRENT_LIST_SELECT: &str = r#"
        SELECT
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            serving_resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at,
            labelhash,
            token_id,
            owner,
            registrant,
            created_at,
            registration_date,
            expiry_date,
            resolver_address
        FROM filtered_names
"#;

fn list_page_limits(page_size: u64) -> Result<(usize, i64)> {
    let page_size = checked_page_size_usize(
        page_size,
        "name_current list page_size must be positive",
        "name_current list page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "name_current list page_size is too large",
        "name_current list page_size exceeds SQL limit",
    )?;
    Ok((page_size, page_limit))
}

/// The list page, without a total, over the served rows, or with `composed` (a JSON array of
/// the rows `source_row` in families/name/list.rs builds) over those rows instead, through the
/// same derived columns, predicates, order and cursor. One statement, so the composed readers
/// run it inside their read snapshot.
pub(crate) async fn list_page_from(
    executor: impl PgExecutor<'_>,
    filter: &NameCurrentListFilter,
    (sort, order): (NameCurrentListSort, NameCurrentListOrder),
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    composed: &serde_json::Value,
) -> Result<NameCurrentListPage> {
    let (page_size, page_limit) = list_page_limits(page_size)?;

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_list_cte(&mut builder, filter, composed, |_| {});
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" WHERE TRUE ");
    if let Some(cursor) = cursor {
        push_name_current_list_cursor_after(&mut builder, sort, order, cursor);
    }
    push_name_current_list_order(&mut builder, sort, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(executor)
        .await
        .with_context(|| format!("failed to load name_current compact page for {filter:?}"))?;
    let mut rows = rows
        .into_iter()
        .map(decode_name_current_list_row)
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if rows.len() > page_size {
        rows.truncate(page_size);
        rows.last()
            .map(|row| name_current_list_cursor_from_row(row, sort))
    } else {
        None
    };

    Ok(NameCurrentListPage {
        rows,
        next_cursor,
        total_count: None,
    })
}

/// The column list of a composed row set bound as `jsonb_to_recordset` (the rows
/// `source_row` in families/name/list.rs builds).
pub(crate) const COMPOSED_NC_COLUMNS: &str =
    "nc(logical_name_id text, namespace text, raw_name text,
    namehash text, surface_binding_id uuid, resource_id uuid, serving_resource_id uuid,
    token_lineage_id uuid, binding_kind text, declared_summary jsonb, provenance jsonb,
    support_status text, unsupported_reason text, chain_positions jsonb,
    canonicality_summary jsonb, manifest_version bigint, last_recomputed_at timestamptz)";

/// Push the `filtered_names` CTE over the served rows, or over `composed` rows, which the
/// composed reader has already judged readable, and let the caller append extra predicates
/// against `nc` after the standard filter predicates, inside the CTE's `WHERE`, where they can
/// drive an index scan.
pub(super) fn push_filtered_name_list_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a NameCurrentListFilter,
    composed: &'a serde_json::Value,
    push_extra_predicates: impl FnOnce(&mut QueryBuilder<'a, Postgres>),
) {
    builder.push("WITH ");
    builder.push(
        r#"
        filtered_names AS (
            SELECT
                nc.logical_name_id,
                nc.namespace,
                nc.raw_name AS canonical_display_name,
                nc.raw_name AS normalized_name,
                nc.namehash,
                nc.surface_binding_id,
                nc.resource_id,
                nc.serving_resource_id,
                nc.token_lineage_id,
                nc.binding_kind,
                nc.declared_summary,
                nc.provenance,
                CASE WHEN nc.support_status = 'supported'
                     THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                     ELSE jsonb_build_object(
                         'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                         'unsupported_reason', nc.unsupported_reason
                     ) END AS coverage,
                nc.chain_positions,
                nc.canonicality_summary,
                nc.manifest_version,
                nc.last_recomputed_at,
                surface.labelhashes[1] AS labelhash,
                NULLIF(COALESCE(
                    nc.declared_summary #>> '{authority,token_id}',
                    nc.declared_summary #>> '{registration,token_id}',
                    nc.declared_summary #>> '{registration,upstream_resource}',
                    nc.declared_summary #>> '{control,token_id}'
                ), '') AS token_id,
                NULLIF(LOWER(COALESCE(
                    nc.declared_summary #>> '{control,registry_owner}',
                    nc.declared_summary #>> '{control,owner}'
                )), '') AS owner,
                NULLIF(LOWER(COALESCE(
                    nc.declared_summary #>> '{control,registrant}',
                    nc.declared_summary #>> '{registration,registrant}'
                )), '') AS registrant,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "created_at"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["history", "created_at"]);
    builder.push(
        r#",
                    surface_lineage.block_timestamp
                ) AS created_at,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "registration_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["registration", "registered_at"]);
    builder.push(
        r#"
                ) AS registration_date,
                COALESCE(
                    "#,
    );
    push_json_timestamp_expr(builder, &["registration", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["registration", "expiry"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["control", "expiry_date"]);
    builder.push(", ");
    push_json_timestamp_expr(builder, &["control", "expiry"]);
    builder.push(
        r#"
                ) AS expiry_date,
                NULLIF(LOWER(nc.declared_summary #>> '{resolver,address}'), '') AS resolver_address
        "#,
    );

    builder.push(" FROM JSONB_TO_RECORDSET(");
    builder.push_bind(composed);
    builder.push(") AS ");
    builder.push(COMPOSED_NC_COLUMNS);
    builder.push(
        r#"
            JOIN bigname_phase.name_surfaces surface
              ON surface.logical_name_id = nc.logical_name_id
            JOIN bigname_phase.chain_lineage surface_lineage
              ON surface_lineage.chain_id = surface.chain_id
             AND surface_lineage.block_hash = surface.block_hash
            WHERE TRUE
            "#,
    );
    push_name_current_filter_predicates(builder, filter);
    push_extra_predicates(builder);
    builder.push(")");
}

include!("list_filters.rs");
include!("list_paging.rs");
