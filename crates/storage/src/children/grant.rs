//! The API role's `SELECT` on the stored child listing, which every child read joins. A role
//! without it still starts, with one startup WARN (docs/deployment.md, "The child listing family
//! and its counts"). Each child read checks the privilege in its own snapshot and fails with
//! [`ChildListingUnreadable`], and the reads that do not join the listing serve as before.
use anyhow::{Context, Result};
use sqlx::PgExecutor;

/// The relations the subnames page, the labels page, the label count and the child counts join.
pub const CHILD_LISTING_TABLES: [&str; 2] = [
    "bigname_phase.project_child_listing",
    "bigname_phase.project_child_listing_count",
];

/// Each of `$1` the session's role cannot read: absent, in a schema it has no `USAGE` on, or
/// without `SELECT`. The `CASE` keeps a missing schema or table from reaching the privilege
/// checks, which would raise instead of answering.
const UNREADABLE: &str = "/* storage:children.listing_grant */
    SELECT relation FROM unnest($1::text[]) relation
    WHERE CASE
        WHEN to_regnamespace(split_part(relation, '.', 1)) IS NULL THEN TRUE
        WHEN NOT has_schema_privilege(to_regnamespace(split_part(relation, '.', 1)), 'USAGE')
            THEN TRUE
        WHEN to_regclass(relation) IS NULL THEN TRUE
        ELSE NOT has_table_privilege(to_regclass(relation), 'SELECT')
    END
    ORDER BY relation";

/// A child read ran under a role that cannot read the stored child listing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildListingUnreadable {
    pub relations: Vec<String>,
}

impl std::fmt::Display for ChildListingUnreadable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "the role cannot read {}",
            self.relations.join(", ")
        )
    }
}

impl std::error::Error for ChildListingUnreadable {}

/// The [`CHILD_LISTING_TABLES`] the session's role cannot read, in name order.
pub async fn unreadable_child_listing_tables<'e>(
    executor: impl PgExecutor<'e>,
) -> Result<Vec<String>> {
    sqlx::query_scalar(UNREADABLE)
        .bind(&CHILD_LISTING_TABLES[..])
        .fetch_all(executor)
        .await
        .context("failed to check the role's read of the stored child listing")
}

/// Fails with [`ChildListingUnreadable`] when the session's role cannot read the listing. Run it
/// in the read's snapshot, before the read.
pub(super) async fn require_child_listing<'e>(executor: impl PgExecutor<'e>) -> Result<()> {
    let relations = unreadable_child_listing_tables(executor).await?;
    if relations.is_empty() {
        return Ok(());
    }
    Err(ChildListingUnreadable { relations }.into())
}
