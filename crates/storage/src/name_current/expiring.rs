//! Namespace-wide listing of current names by exact registration expiry.
//!
//! The family reader selects a page's names from the stored name summary and composes only
//! those. This statement applies the exact half-open window and keyset order to the composed
//! rows. Registration expiry may be a decimal JSON string or number; neither is narrowed to
//! a calendar timestamp or floating-point value.

use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgExecutor, Postgres, QueryBuilder};

use crate::UnixSeconds;

use super::list::{
    NAME_CURRENT_LIST_SELECT, NameCurrentListCursor, NameCurrentListCursorValue,
    NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage, NameCurrentListSort,
    decode_name_current_list_row, name_current_list_cursor_from_row, push_filtered_name_list_cte,
    push_name_current_list_cursor_after, push_name_current_list_order,
};
use super::{escape_like_pattern, push_public_authority_predicate};
use crate::projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize};

/// One half-open expiry interval. Scalar requests may omit one endpoint; repeated windows
/// carry both. Values retain the exact precision of the public expiry filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameCurrentExpiryWindow {
    pub expires_after: Option<UnixSeconds>,
    pub expires_before: Option<UnixSeconds>,
}

/// The stored canonical deadline selected by a names request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameCurrentDeadline {
    Expiry,
    GraceEnds,
}

impl NameCurrentDeadline {
    pub const fn column(self) -> &'static str {
        match self {
            Self::Expiry => "expires_at",
            Self::GraceEnds => "grace_ends_at",
        }
    }

    fn sort(self) -> NameCurrentListSort {
        match self {
            Self::Expiry => NameCurrentListSort::ExpiryDate,
            Self::GraceEnds => NameCurrentListSort::GraceEndsAt,
        }
    }

    pub fn value(self, row: &super::list::NameCurrentListRow) -> Option<UnixSeconds> {
        match self {
            Self::Expiry => row.expiry_date,
            // Keep the canonical numeric value exact. Public timestamp formatting emits whole
            // seconds and must never determine a fractional keyset cursor or membership.
            Self::GraceEnds => row
                .row
                .declared_summary
                .pointer("/registration/grace_ends_at")
                .and_then(UnixSeconds::from_json),
        }
    }
}

/// A disjoint union of deadline intervals over current names in one namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentExpiringFilter {
    pub deadline: NameCurrentDeadline,
    pub namespace: String,
    pub windows: Vec<NameCurrentExpiryWindow>,
    /// Public `authority` values (`ens_v0`, `ens_v1`, `ens_v2`); a row matches when the value it
    /// serves is any listed one.
    pub authorities: Option<Vec<String>>,
    /// A normalized name: only names exactly one label below it.
    pub parent: Option<String>,
}

impl NameCurrentExpiringFilter {
    /// Keep storage callers within the same bounded, disjoint shape as the API. In particular,
    /// invalid windows must never spend the per-arm limit on duplicate names.
    pub(crate) fn validate_windows(&self) -> Result<()> {
        ensure!(
            (1..=32).contains(&self.windows.len()),
            "the expiring page requires 1 to 32 windows"
        );
        for window in &self.windows {
            ensure!(
                window.expires_after.is_some() || window.expires_before.is_some(),
                "the expiring page requires an expires_after or expires_before bound"
            );
            if self.windows.len() > 1 {
                ensure!(
                    window.expires_after.is_some() && window.expires_before.is_some(),
                    "multiple expiry windows require both bounds"
                );
            }
            if let (Some(after), Some(before)) = (window.expires_after, window.expires_before) {
                ensure!(after < before, "expiry window after must precede before");
            }
        }
        let mut sorted = self.windows.clone();
        sorted.sort_unstable_by_key(|window| window.expires_after);
        ensure!(
            sorted
                .windows(2)
                .all(|pair| pair[0].expires_before <= pair[1].expires_after),
            "expiry windows must not overlap or repeat"
        );
        Ok(())
    }
}

/// The `LIKE` patterns of the names exactly one label below `parent`: a name matches the first
/// and not the second. Normalized labels hold no `.`.
pub(crate) fn parent_like_patterns(parent: &str) -> (String, String) {
    let parent = escape_like_pattern(parent);
    (format!("_%.{parent}"), format!("%.%.{parent}"))
}

/// ` AND` the name in `column` is exactly one label below `parent`.
pub(crate) fn push_parent_predicate(
    builder: &mut QueryBuilder<'_, Postgres>,
    column: &str,
    parent: &str,
) {
    let (one_below, deeper) = parent_like_patterns(parent);
    builder.push(format!(" AND {column} LIKE "));
    builder.push_bind(one_below);
    builder.push(format!(" ESCAPE '\\' AND {column} NOT LIKE "));
    builder.push_bind(deeper);
    builder.push(" ESCAPE '\\'");
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
    filter.validate_windows()?;
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

    // Apply the same lifecycle discovery eligibility as search, preserving authority coverage.
    let list_filter = NameCurrentListFilter {
        namespace: Some(filter.namespace.clone()),
        supported_only: true,
        ..NameCurrentListFilter::default()
    };
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_name_list_cte(&mut builder, &list_filter, composed, |builder| {
        // Only a finite registration expiry participates in /names. The exact derived-column
        // predicates below retain the shared alias priority and classified-null behavior.
        builder.push(" AND ");
        builder.push(crate::families::name::FINITE_REGISTRATION_EXPIRY_SQL);
        if let Some(authorities) = &filter.authorities {
            push_public_authority_predicate(builder, "nc", authorities);
        }
        if let Some(parent) = &filter.parent {
            push_parent_predicate(builder, "nc.raw_name", parent);
        }
    });
    builder.push(NAME_CURRENT_LIST_SELECT);
    let date_column = filter.deadline.sort().as_str();
    builder.push(format!(" WHERE {date_column} IS NOT NULL"));
    builder.push(" AND (");
    for (index, window) in filter.windows.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        builder.push("(TRUE");
        if let Some(after) = window.expires_after {
            builder
                .push(format!(" AND {date_column} >= "))
                .push_bind(after);
        }
        if let Some(before) = window.expires_before {
            builder
                .push(format!(" AND {date_column} < "))
                .push_bind(before);
        }
        builder.push(")");
    }
    builder.push(")");
    if let Some(cursor) = cursor {
        push_name_current_list_cursor_after(&mut builder, filter.deadline.sort(), order, cursor);
    }
    push_name_current_list_order(&mut builder, filter.deadline.sort(), order);
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
            .map(|row| name_current_list_cursor_from_row(row, filter.deadline.sort()))
    } else {
        None
    };

    Ok(NameCurrentListPage {
        rows,
        next_cursor,
        total_count: None,
    })
}

#[cfg(test)]
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;
    use crate::families::{control::lifecycle::NamePlace, name::rendered::label_hash};

    async fn one_below(pool: &sqlx::PgPool, names: &[&str], parent: &str) -> Result<Vec<String>> {
        let mut query = QueryBuilder::<Postgres>::new("SELECT name FROM unnest(");
        query.push_bind(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        );
        query.push("::text[]) AS candidate(name) WHERE TRUE");
        push_parent_predicate(&mut query, "candidate.name", parent);
        query.push(" ORDER BY name");
        Ok(query.build_query_scalar().fetch_all(pool).await?)
    }

    #[tokio::test]
    async fn parent_predicate_agrees_with_the_registrar_name_place() -> Result<()> {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("expiring_parent_predicate").pool_max_connections(1),
        )
        .await?;
        let hash = format!("[{}]", "ab".repeat(32));
        let names = [
            "eth".to_owned(),
            "foo.eth".to_owned(),
            "sub.foo.eth".to_owned(),
            format!("{hash}.eth"),
            format!("sub.{hash}.eth"),
            "fooeth".to_owned(),
            "x.fooeth".to_owned(),
            "base.eth".to_owned(),
            "foo.base.eth".to_owned(),
            "sub.foo.base.eth".to_owned(),
        ];
        let names = names.iter().map(String::as_str).collect::<Vec<_>>();
        for (namespace, parent, place) in [
            ("ens", "eth", NamePlace::EthSecondLevel),
            ("basenames", "base.eth", NamePlace::BasenamesSecondLevel),
        ] {
            let mut expected = names
                .iter()
                .filter(|name| {
                    let labelhashes = name
                        .split('.')
                        .map(|label| {
                            format!("0x{}", alloy_primitives::hex::encode(label_hash(label)))
                        })
                        .collect::<Vec<_>>();
                    NamePlace::of(namespace, &labelhashes) == place
                })
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(
                one_below(database.pool(), &names, parent).await?,
                expected,
                "{parent}"
            );
        }
        assert_eq!(
            one_below(
                database.pool(),
                &["x.a_b.eth", "x.aXb.eth", "y.a%b.eth"],
                "a_b.eth"
            )
            .await?,
            ["x.a_b.eth"]
        );
        database.cleanup().await?;
        Ok(())
    }
}
