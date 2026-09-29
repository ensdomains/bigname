//! Namespace-wide listing of current names by exact registration expiry.
//!
//! The family reader walks indexed lifecycle and wrapper expiry candidates before composing a
//! bounded batch. This statement applies the exact half-open window and keyset order to that
//! batch. Registration expiry may be a decimal JSON string or number; neither is narrowed to
//! a calendar timestamp or floating-point value.

use anyhow::{Context, Result, bail};
use sqlx::{PgExecutor, Postgres, QueryBuilder};

use crate::UnixSeconds;

use super::list::{
    NAME_CURRENT_LIST_SELECT, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage, NameCurrentListSort,
    decode_name_current_list_row, name_current_list_cursor_from_row, push_filtered_name_list_cte,
    push_name_current_list_cursor_after, push_name_current_list_order,
};
use crate::projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize};

const REGISTRATION_EXPIRY_JSON_PATH: &str = "'{registration,expiry}'";

/// Window over current names of one namespace by registration expiry. `expires_after` is
/// inclusive and `expires_before` exclusive, so consecutive windows tile without overlap. At least
/// one bound is required: the reader refuses an unbounded namespace scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentExpiringFilter {
    pub namespace: String,
    pub expires_after: Option<UnixSeconds>,
    pub expires_before: Option<UnixSeconds>,
}

/// The expiring page over the served rows, or with `composed` over those rows instead (see
/// `list_page_from`). One statement, so the composed reader runs it inside its read snapshot.
pub(crate) async fn expiring_page_from(
    executor: impl PgExecutor<'_>,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    composed: &serde_json::Value,
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
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_list_cte(&mut builder, &list_filter, composed, |builder| {
        // Only a finite registration expiry participates in /names. The exact derived-column
        // predicates below retain the shared alias priority and classified-null behavior.
        builder.push(" AND (nc.declared_summary #>> ");
        builder.push(REGISTRATION_EXPIRY_JSON_PATH);
        builder.push(") ~ '^-?[0-9]+(\\.[0-9]+)?$'");
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
        .fetch_all(executor)
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
