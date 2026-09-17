//! Namespace-wide listing of current names by registration expiry.
//!
//! This is the `sort=expires_at` case of the derived list reader with an expiry window. It exists
//! as its own entry point because the window must be applied twice: once as the exact bound on the
//! derived `expiry_date` column the page orders by, and once inside the `filtered_names` CTE as a
//! sargable predicate on the stored `registration.expiry` number, which is what the
//! `name_current_registration_expiry_idx` partial index covers
//! (`schema-v2/baseline/06_projections.sql`). The projection writes `registration.expiry` as a
//! JSON number of unix seconds (`crates/project/src/builders/name_current/build.sql`); the derived
//! column also tolerates RFC 3339 strings and `control.expiry`, but the index does not, so a row
//! whose only expiry is in one of those forms is outside this listing by design.

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, types::time::OffsetDateTime};

use super::list::{
    NAME_CURRENT_LIST_SELECT, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage, NameCurrentListSort,
    decode_name_current_list_row, name_current_list_cursor_from_row,
    push_filtered_name_current_cte_with, push_name_current_list_cursor_after,
    push_name_current_list_order,
};
use crate::projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize};

const REGISTRATION_EXPIRY_JSON_PATH: &str = "'{registration,expiry}'";

/// Window over current names of one namespace by registration expiry. `expires_after` is
/// inclusive and `expires_before` exclusive, so consecutive windows tile without overlap. At least
/// one bound is required: the reader refuses an unbounded namespace scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentExpiringFilter {
    pub namespace: String,
    pub expires_after: Option<OffsetDateTime>,
    pub expires_before: Option<OffsetDateTime>,
}

/// Load a bounded page of supported current names whose registration expiry falls in the window,
/// ordered by expiry then by name identity. Rows with no numeric `registration.expiry` are never
/// listed, so the page carries no null-expiry rows in either order.
/// A released name keeps its lapsed registration's numeric expiry and stays listed: the window
/// selects registrations by expiry, whether they are live, in grace or released.
pub async fn load_name_current_expiring_page(
    pool: &PgPool,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<NameCurrentListPage> {
    if filter.expires_after.is_none() && filter.expires_before.is_none() {
        bail!("name_current expiring page requires an expires_after or expires_before bound");
    }
    if let Some(cursor) = cursor
        && !matches!(
            cursor.sort_value,
            NameCurrentListCursorValue::Timestamp(Some(_))
        )
    {
        bail!("name_current expiring page cursor must carry an expiry timestamp");
    }
    let page_size = checked_page_size_usize(
        page_size,
        "name_current expiring page_size must be positive",
        "name_current expiring page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "name_current expiring page_size is too large",
        "name_current expiring page_size exceeds SQL limit",
    )?;

    // A listing row carries no status or unsupported_reason field, so unsupported names are
    // omitted here exactly as search omits them.
    let list_filter = NameCurrentListFilter {
        namespace: Some(filter.namespace.clone()),
        supported_only: true,
        ..NameCurrentListFilter::default()
    };
    // Keep the index prefilter conservative: rounding a fractional upper bound to f64
    // can discard a matching expiry. The timestamp predicates below enforce exact bounds.
    let after_seconds = filter
        .expires_after
        .map(|time| time.unix_timestamp() as f64);
    let before_seconds = filter
        .expires_before
        .map(|time| (time.unix_timestamp() + i64::from(time.nanosecond() != 0)) as f64);

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_current_cte_with(&mut builder, &list_filter, |builder| {
        builder.push(" AND JSONB_TYPEOF(nc.declared_summary #> ");
        builder.push(REGISTRATION_EXPIRY_JSON_PATH);
        builder.push(") = 'number'");
        if let Some(after_seconds) = after_seconds {
            builder.push(" AND (nc.declared_summary #>> ");
            builder.push(REGISTRATION_EXPIRY_JSON_PATH);
            builder.push(")::DOUBLE PRECISION >= ");
            builder.push_bind(after_seconds);
        }
        if let Some(before_seconds) = before_seconds {
            builder.push(" AND (nc.declared_summary #>> ");
            builder.push(REGISTRATION_EXPIRY_JSON_PATH);
            builder.push(")::DOUBLE PRECISION < ");
            builder.push_bind(before_seconds);
        }
    });
    builder.push(NAME_CURRENT_LIST_SELECT);
    builder.push(" WHERE expiry_date IS NOT NULL");
    if let Some(expires_after) = filter.expires_after {
        builder.push(" AND expiry_date >= ");
        builder.push_bind(expires_after);
    }
    if let Some(expires_before) = filter.expires_before {
        builder.push(" AND expiry_date < ");
        builder.push_bind(expires_before);
    }
    if let Some(cursor) = cursor {
        push_name_current_list_cursor_after(
            &mut builder,
            NameCurrentListSort::ExpiryDate,
            order,
            cursor,
        );
    }
    push_name_current_list_order(&mut builder, NameCurrentListSort::ExpiryDate, order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load name_current expiring page for {filter:?}"))?;
    let mut rows = rows
        .into_iter()
        .map(decode_name_current_list_row)
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if rows.len() > page_size {
        rows.truncate(page_size);
        rows.last()
            .map(|row| name_current_list_cursor_from_row(row, NameCurrentListSort::ExpiryDate))
    } else {
        None
    };

    Ok(NameCurrentListPage {
        rows,
        next_cursor,
        total_count: None,
    })
}
