//! The listing's page and total statements: the request's filters over the address's listed
//! rows, the sort index's key and the keyset position.
use anyhow::{Context, Result};
use sqlx::{FromRow, PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use super::AddressNamesPageRequest;
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue, NameQueryMatch,
    UnixSeconds, families::records::seams, families::topology::MISSING_TIMESTAMP,
};

/// The read filter of a stored row, a probe of its name's surface and resource evaluated per
/// row the index walk reaches: the surface, the surface's block and the resource on readable
/// lineage, as `DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER` admits a composed row. A registry
/// child has no surface and is read by its publication alone.
pub(super) const FENCE: &str = "
    AND (l.registry_child OR COALESCE((
        SELECT TRUE
        FROM bigname_phase.name_surfaces surface
        JOIN bigname_phase.chain_lineage surface_lineage
          ON surface_lineage.chain_id = surface.chain_id
         AND surface_lineage.block_hash = surface.block_hash
        JOIN bigname_phase.resources resource ON resource.resource_id = l.resource_id
        JOIN bigname_phase.chain_lineage resource_lineage
          ON resource_lineage.chain_id = resource.chain_id
         AND resource_lineage.block_hash = resource.block_hash
        WHERE surface.logical_name_id = l.logical_name_id
          AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        LIMIT 1), FALSE))";

/// One stored row of the page.
#[derive(Clone, Debug, FromRow)]
pub(super) struct Row {
    pub(super) chain_id: String,
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) display_key: String,
    pub(super) display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) resource_id: Uuid,
    pub(super) token_holder: bool,
    pub(super) effective_controller: bool,
    pub(super) role_holder: bool,
    pub(super) registry_child: bool,
    /// The timestamp key under a timestamp sort, `MISSING_TIMESTAMP` when the name has none.
    pub(super) sort_key: Option<UnixSeconds>,
}

/// A keyset position: the sort key, the name and, to continue inside a name's rows, the
/// resource. Without the resource the position is after every row of the name.
#[derive(Clone, Debug)]
pub(super) struct Position {
    key: Key,
    logical_name_id: String,
    resource_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
enum Key {
    Name(String),
    Timestamp(UnixSeconds),
}

/// One page read of a request.
pub(super) struct Read<'a> {
    pub(super) request: &'a AddressNamesPageRequest<'a>,
    pub(super) address: &'a str,
    chains: &'a [String],
    /// The relations asked for: token holder, effective controller, role holder.
    pub(super) wanted: [bool; 3],
}

impl<'a> Read<'a> {
    pub(super) fn new(
        request: &'a AddressNamesPageRequest<'a>,
        address: &'a str,
        chains: &'a [String],
    ) -> Self {
        Self {
            request,
            address,
            chains,
            wanted: super::anchors::wanted(request.relations),
        }
    }

    fn registration(&self) -> bool {
        self.request.dedupe_by == AddressNamesCurrentDedupe::Resource
    }

    fn key_column(&self) -> &'static str {
        match self.request.sort {
            AddressNamesCurrentSort::Name => "l.display_key",
            AddressNamesCurrentSort::ExpiresAt => "l.expires_key",
            AddressNamesCurrentSort::RegisteredAt => "l.registered_key",
            AddressNamesCurrentSort::CreatedAt => "l.created_key",
        }
    }

    fn descending(&self) -> bool {
        self.request.order == AddressNamesCurrentOrder::Desc
    }

    /// Whether `row` continues the entry `first` began: rows of one name, under name grouping.
    pub(super) fn merges(&self, first: &Row, row: &Row) -> bool {
        !self.registration() && first.logical_name_id == row.logical_name_id
    }

    /// The request cursor's position: after every row of its name under name grouping, after
    /// its row under registration grouping, where a resource has one entry row.
    pub(super) fn request_position(&self) -> Result<Option<Position>> {
        let Some(cursor) = self.request.cursor else {
            return Ok(None);
        };
        let key = match (&cursor.sort_value, self.request.sort) {
            (AddressNamesCurrentSortedCursorValue::Name(name), AddressNamesCurrentSort::Name) => {
                Key::Name(name.clone())
            }
            (AddressNamesCurrentSortedCursorValue::Timestamp(value), sort)
                if sort != AddressNamesCurrentSort::Name =>
            {
                Key::Timestamp(value.unwrap_or_else(missing))
            }
            _ => anyhow::bail!("the cursor's position is not of the request's sort"),
        };
        Ok(Some(Position {
            key,
            logical_name_id: cursor.logical_name_id.clone(),
            resource_id: self.registration().then_some(cursor.resource_id),
        }))
    }

    /// The position just after `row`.
    pub(super) fn position(&self, row: &Row) -> Position {
        Position {
            key: self.key(row),
            logical_name_id: row.logical_name_id.clone(),
            resource_id: Some(row.resource_id),
        }
    }

    fn key(&self, row: &Row) -> Key {
        match row.sort_key {
            Some(value) if self.request.sort != AddressNamesCurrentSort::Name => {
                Key::Timestamp(value)
            }
            _ => Key::Name(row.display_key.clone()),
        }
    }

    /// The cursor after the entry whose representative row is `row`.
    pub(super) fn cursor(&self, row: &Row) -> AddressNamesCurrentSortedCursor {
        let sort_value = match self.key(row) {
            Key::Name(name) => AddressNamesCurrentSortedCursorValue::Name(name),
            Key::Timestamp(value) => AddressNamesCurrentSortedCursorValue::Timestamp(
                (value != missing()).then_some(value),
            ),
        };
        AddressNamesCurrentSortedCursor {
            sort_value,
            logical_name_id: row.logical_name_id.clone(),
            resource_id: row.resource_id,
        }
    }

    /// The request's filters over the address's listed rows aliased `alias`.
    pub(super) fn push_filters(&self, builder: &mut QueryBuilder<'_, Postgres>, alias: &str) {
        let request = self.request;
        builder.push(format!(" {alias}.address = "));
        builder.push_bind(self.address.to_owned());
        builder.push(format!(" AND {alias}.listed"));
        if let Some(namespace) = request.namespace {
            builder.push(format!(" AND {alias}.namespace = "));
            builder.push_bind(namespace.to_owned());
        }
        if self.wanted != [true; 3] {
            let flags: Vec<String> = ["token_holder", "effective_controller", "role_holder"]
                .into_iter()
                .zip(self.wanted)
                .filter(|(_, wanted)| *wanted)
                .map(|(flag, _)| format!("{alias}.{flag}"))
                .collect();
            builder.push(format!(" AND ({})", flags.join(" OR ")));
        }
        if let Some(q) = request.q {
            match q.mode {
                // The q index keys the first 100 characters of the name: a range over that cut,
                // then the whole text checked against the whole name.
                NameQueryMatch::Prefix => {
                    let head: String = q.text.chars().take(100).collect();
                    builder.push(format!(" AND left({alias}.normalized_name, 100) LIKE "));
                    builder.push_bind(format!("{}%", like_escaped(&head)));
                    builder.push(format!(
                        " ESCAPE '\\' AND starts_with({alias}.normalized_name, "
                    ));
                    builder.push_bind(q.text.to_owned());
                    builder.push(")");
                }
                NameQueryMatch::Contains => {
                    builder.push(format!(" AND {alias}.normalized_name LIKE "));
                    builder.push_bind(q.like_pattern());
                    builder.push(" ESCAPE '\\'");
                }
            }
        }
        if let Some(authorities) = request.authority.filter(|values| !values.is_empty()) {
            builder.push(format!(" AND {alias}.public_authority = ANY("));
            builder.push_bind(
                authorities
                    .iter()
                    .map(|authority| (*authority).to_owned())
                    .collect::<Vec<_>>(),
            );
            builder.push(")");
        }
        if let Some(migrated) = request.is_migrated {
            builder.push(format!(" AND {alias}.migrated = "));
            builder.push_bind(migrated);
        }
        if let Some(parent) = request.parent {
            builder.push(format!(" AND md5({alias}.parent_normalized_name) = md5("));
            builder.push_bind(parent.to_owned());
            builder.push(format!(") AND {alias}.parent_normalized_name = "));
            builder.push_bind(parent.to_owned());
        }
    }

    /// Whether the request filters rows beyond the address and its listed relations, so a
    /// resource's entry row may not be its stored lead.
    fn filtered(&self) -> bool {
        let request = self.request;
        request.namespace.is_some()
            || self.wanted != [true; 3]
            || request.q.is_some()
            || request.authority.is_some_and(|values| !values.is_empty())
            || request.is_migrated.is_some()
            || request.parent.is_some()
    }

    /// Under registration grouping, only the entry row of each resource: the stored lead when
    /// nothing else is filtered, else, when `first` asks for it, the first row in name order the
    /// filters keep. A count of resources needs no more than the lead.
    fn push_entry_rows(&self, builder: &mut QueryBuilder<'_, Postgres>, first: bool) {
        if !self.registration() {
            return;
        }
        if !self.filtered() {
            builder.push(" AND l.registration_lead");
            return;
        }
        if !first {
            return;
        }
        builder.push(
            " AND NOT EXISTS (SELECT 1 FROM bigname_phase.project_address_listing o
              WHERE o.chain_id = l.chain_id AND o.resource_id = l.resource_id AND",
        );
        self.push_filters(builder, "o");
        builder
            .push(" AND (o.display_key, o.logical_name_id) < (l.display_key, l.logical_name_id))");
    }

    /// Up to `limit` rows after `after`, in the page's order.
    pub(super) async fn rows(
        &self,
        conn: &mut PgConnection,
        after: Option<&Position>,
        limit: usize,
    ) -> Result<Vec<Row>> {
        #[cfg(any(test, feature = "test-support"))]
        explain(conn, "page", || self.rows_statement(after, limit)).await?;
        self.rows_statement(after, limit)?
            .build_query_as::<Row>()
            .fetch_all(&mut *conn)
            .await
            .with_context(|| format!("failed to read the listed names of {}", self.address))
    }

    fn rows_statement(
        &self,
        after: Option<&Position>,
        limit: usize,
    ) -> Result<QueryBuilder<'static, Postgres>> {
        let key = self.key_column();
        let (direction, comparison) = if self.descending() {
            ("DESC", "<")
        } else {
            ("ASC", ">")
        };
        let mut builder = QueryBuilder::<Postgres>::new(
            "/* storage:address_names.listing_page */
             SELECT l.chain_id, l.logical_name_id, l.namespace, l.display_key, l.display_name,
                    l.normalized_name, l.namehash, l.resource_id, l.token_holder,
                    l.effective_controller, l.role_holder, l.registry_child, ",
        );
        if self.request.sort == AddressNamesCurrentSort::Name {
            builder.push("NULL::numeric");
        } else {
            builder.push(key);
        }
        builder.push(" AS sort_key FROM bigname_phase.project_address_listing l WHERE");
        self.push_filters(&mut builder, "l");
        builder.push(" AND l.chain_id = ANY(");
        builder.push_bind(self.chains.to_vec());
        builder.push(")");
        self.push_entry_rows(&mut builder, true);
        builder.push(FENCE);
        if let Some(after) = after {
            builder.push(format!(" AND ({key}, l.logical_name_id"));
            if after.resource_id.is_some() {
                builder.push(", l.resource_id");
            }
            builder.push(format!(") {comparison} ("));
            match &after.key {
                Key::Name(name) => builder.push_bind(name.clone()),
                Key::Timestamp(value) => builder.push_bind(*value),
            };
            builder.push(", ");
            builder.push_bind(after.logical_name_id.clone());
            if let Some(resource_id) = after.resource_id {
                builder.push(", ");
                builder.push_bind(resource_id);
            }
            builder.push(")");
        }
        builder.push(format!(
            " ORDER BY {key} {direction}, l.logical_name_id {direction}, \
             l.resource_id {direction} LIMIT "
        ));
        builder.push_bind(i64::try_from(limit).context("page limit does not fit")?);
        Ok(builder)
    }

    /// The request's total: every entry when `exact`, else the entries up to
    /// `seams::exact_total_cap`, `None` above it. One entry per name, or per resource under
    /// registration grouping. It is the stored count: no read filter and no chain is applied.
    pub(super) async fn total(&self, conn: &mut PgConnection, exact: bool) -> Result<Option<u64>> {
        if exact {
            return self.count(conn).await.map(Some);
        }
        let cap = seams::exact_total_cap();
        #[cfg(any(test, feature = "test-support"))]
        explain(conn, "bounded_total", || self.bounded_total_statement(cap)).await?;
        let (rows, entries): (i64, i64) = self
            .bounded_total_statement(cap)?
            .build_query_as()
            .fetch_one(&mut *conn)
            .await
            .with_context(|| format!("failed to count the listed names of {}", self.address))?;
        let cap = cap as u64;
        let (rows, entries) = (rows as u64, entries as u64);
        if rows <= cap {
            return Ok(Some(entries));
        }
        if entries > cap {
            return Ok(None);
        }
        // More rows than the cap can still be no more entries than it: a name's rows on two
        // chains, or a filtered resource's rows under registration grouping.
        let total = self.count(conn).await?;
        Ok((total <= cap).then_some(total))
    }

    /// What one entry counts as: its name, or its resource under registration grouping.
    fn entry_column(&self) -> &'static str {
        if self.registration() {
            "l.resource_id::text"
        } else {
            "l.logical_name_id"
        }
    }

    fn bounded_total_statement(&self, cap: usize) -> Result<QueryBuilder<'static, Postgres>> {
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "/* storage:address_names.listing_bounded_total */
             SELECT count(*), count(DISTINCT entry) FROM (
                 SELECT {entry} AS entry FROM bigname_phase.project_address_listing l WHERE",
            entry = self.entry_column()
        ));
        self.push_filters(&mut builder, "l");
        self.push_entry_rows(&mut builder, false);
        builder.push(" LIMIT ");
        builder.push_bind(i64::try_from(cap).context("total cap does not fit")? + 1);
        builder.push(") bounded");
        Ok(builder)
    }

    async fn count(&self, conn: &mut PgConnection) -> Result<u64> {
        #[cfg(any(test, feature = "test-support"))]
        explain(conn, "total", || Ok(self.count_statement())).await?;
        let total: i64 = self
            .count_statement()
            .build_query_scalar()
            .fetch_one(&mut *conn)
            .await
            .with_context(|| format!("failed to count the listed names of {}", self.address))?;
        Ok(total as u64)
    }

    fn count_statement(&self) -> QueryBuilder<'static, Postgres> {
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "/* storage:address_names.listing_total */
             SELECT count(DISTINCT {entry}) FROM bigname_phase.project_address_listing l WHERE",
            entry = self.entry_column()
        ));
        self.push_filters(&mut builder, "l");
        self.push_entry_rows(&mut builder, false);
        builder
    }
}

/// Under `seams::with_listing_plans`, the plans of the statement `build` makes, noted before it
/// runs: custom, and generic as a prepared statement's sixth execution may be, both run.
#[cfg(any(test, feature = "test-support"))]
pub(super) async fn explain(
    conn: &mut PgConnection,
    statement: &'static str,
    build: impl Fn() -> Result<QueryBuilder<'static, Postgres>>,
) -> Result<()> {
    if !seams::listing_plans_wanted() {
        return Ok(());
    }
    // `Executor::execute`, not `RawSql::execute`: the latter's future is not `Send` for every
    // connection lifetime, which the API's handlers need.
    use sqlx::{Arguments, Execute, Executor};

    let mut plans = Vec::with_capacity(2);
    for mode in ["force_custom_plan", "force_generic_plan"] {
        let mut query = build()?;
        sqlx::query(&format!("SET LOCAL plan_cache_mode = {mode}"))
            .execute(&mut *conn)
            .await?;
        (&mut *conn)
            .execute(sqlx::raw_sql(&format!(
                "PREPARE listing_plan AS {}",
                query.sql()
            )))
            .await
            .with_context(|| format!("failed to prepare the listing {statement} statement"))?;
        let arguments = query
            .build()
            .take_arguments()
            .map_err(anyhow::Error::from_boxed)?
            .unwrap_or_default();
        // EXECUTE takes no protocol parameters, so PostgreSQL renders each value as a literal.
        let placeholders = (1..=arguments.len())
            .map(|index| format!("quote_nullable(${index})"))
            .collect::<Vec<_>>()
            .join(", ");
        let literals: Vec<String> =
            sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]::text[]"), arguments)
                .persistent(false)
                .fetch_one(&mut *conn)
                .await?;
        let plan: serde_json::Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE listing_plan({})",
            literals.join(", ")
        ))
        .persistent(false)
        .fetch_one(&mut *conn)
        .await
        .with_context(|| format!("failed to explain the listing {statement} statement"))?;
        (&mut *conn)
            .execute(sqlx::raw_sql("DEALLOCATE listing_plan"))
            .await?;
        plans.push(plan);
    }
    (&mut *conn)
        .execute(sqlx::raw_sql("SET LOCAL plan_cache_mode = DEFAULT"))
        .await?;
    let generic = plans.pop().expect("two plans");
    let custom = plans.pop().expect("two plans");
    seams::note_listing_plan(seams::ListingPlan {
        statement,
        custom,
        generic,
    });
    Ok(())
}

fn missing() -> UnixSeconds {
    UnixSeconds::from_seconds(i128::from(MISSING_TIMESTAMP)).expect("the missing key fits")
}

/// `value` with the LIKE wildcards and the escape character escaped.
fn like_escaped(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
