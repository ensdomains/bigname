//! The walk's candidate order and its sort-key checks (`super`).
use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use super::{AddressNamesPageRequest, WalkInputs};
use crate::{
    AddressNamesCurrentOrder, AddressNamesCurrentSort, AddressNamesCurrentSortedCursorValue,
    address_names::{push_expires_at_timestamp_expr, push_registered_at_timestamp_expr},
    families::{name::FamilyPublication, records::ComposedName},
};

/// Every candidate in the page's order, and where the cursor puts the walk's start.
pub(super) struct WalkOrder {
    pub(super) ids: Vec<String>,
    pub(super) start: usize,
    /// With `sort=name`, each name's walk key, which its composed row must serve.
    display: HashMap<String, String>,
}

impl WalkOrder {
    pub(super) fn agrees(&self, sort: AddressNamesCurrentSort, name: &ComposedName) -> bool {
        sort.is_timestamp()
            || self.display.get(&name.logical_name_id) == Some(&name.canonical_display_name)
    }
}

/// `FROM` the candidates whose surface composition reads (`name::loaders::surfaces`), at or
/// below their chain's publication, aliased `surface`.
fn push_servable_surfaces<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    ids: Vec<String>,
    publications: &BTreeMap<String, FamilyPublication>,
) {
    builder.push(" FROM UNNEST(");
    builder.push_bind(ids);
    builder.push(
        "::text[]) AS candidate(logical_name_id)
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = candidate.logical_name_id
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         JOIN UNNEST(",
    );
    builder.push_bind(publications.keys().cloned().collect::<Vec<_>>());
    builder.push("::text[], ");
    builder.push_bind(
        publications
            .values()
            .map(|publication| publication.block_number)
            .collect::<Vec<_>>(),
    );
    builder.push(format!(
        "::bigint[]) AS published(chain_id, block_number)
           ON published.chain_id = surface.chain_id
          AND surface.block_number <= published.block_number
         WHERE {composed}
           AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
        composed = crate::rendered_name::composed_surface_sql("surface"),
    ));
}

/// Orders every candidate by the page's sort key, then its name id.
pub(super) async fn walk_order(
    conn: &mut PgConnection,
    request: &AddressNamesPageRequest<'_>,
    inputs: &WalkInputs<'_>,
) -> Result<Option<WalkOrder>> {
    let ids: Vec<String> = inputs.by_chain.values().flatten().cloned().collect();
    let children: Vec<String> = inputs.children.keys().map(|id| (*id).to_owned()).collect();
    let mut display = HashMap::new();
    let mut builder =
        QueryBuilder::<Postgres>::new("/* storage:families.records.address_name_walk */ ");
    if request.sort.is_timestamp() {
        builder.push("WITH keyed AS (SELECT surface.logical_name_id, ");
        push_walk_timestamp(&mut builder, request.sort);
        builder.push(" AS sort_timestamp FROM (SELECT surface.*");
        push_servable_surfaces(&mut builder, ids, &inputs.publications);
        builder.push(
            ") surface
             LEFT JOIN bigname_phase.project_name_summary summary
               ON summary.chain_id = surface.chain_id
              AND summary.logical_name_id = surface.logical_name_id
             UNION ALL SELECT child.logical_name_id, NULL::NUMERIC FROM UNNEST(",
        );
        builder.push_bind(children);
        builder.push("::text[]) AS child(logical_name_id)) SELECT logical_name_id, ");
    } else {
        // A surface without raw bytes sorts by the name it is served under.
        let mut surfaces = QueryBuilder::<Postgres>::new(format!(
            "/* storage:families.records.address_name_walk_surfaces */
             SELECT surface.logical_name_id, {name} AS raw_name",
            name = crate::rendered_name::rendered_name_sql("surface"),
        ));
        push_servable_surfaces(&mut surfaces, ids, &inputs.publications);
        let rows = surfaces
            .build()
            .fetch_all(&mut *conn)
            .await
            .context("failed to load the walk's name surfaces")?;
        for row in rows {
            let id: String = row.try_get("logical_name_id")?;
            let raw: String = row.try_get("raw_name")?;
            let Ok(normalized) = crate::rendered_name::parse(&raw) else {
                // Composition refuses the name; the full read reports it.
                return Ok(None);
            };
            display.insert(id, normalized.canonical_display_name);
        }
        let mut keyed_ids: Vec<String> = display.keys().cloned().collect();
        let mut keys: Vec<String> = display.values().cloned().collect();
        for (id, child) in &inputs.children {
            keyed_ids.push((*id).to_owned());
            keys.push(child.row.display_name.clone());
        }
        builder.push("WITH keyed AS (SELECT * FROM UNNEST(");
        builder.push_bind(keyed_ids);
        builder.push("::text[], ");
        builder.push_bind(keys);
        builder.push("::text[]) AS keyed(logical_name_id, display)) SELECT logical_name_id, ");
    }
    push_from_cursor(&mut builder, request);
    builder.push(" AS from_cursor FROM keyed ORDER BY ");
    let direction = match request.order {
        AddressNamesCurrentOrder::Asc => "ASC",
        AddressNamesCurrentOrder::Desc => "DESC",
    };
    if request.sort.is_timestamp() {
        builder.push(null_rank("sort_timestamp", request.order));
        builder.push(format!(" ASC, sort_timestamp {direction}"));
    } else {
        builder.push(format!("display {direction}"));
    }
    builder.push(", logical_name_id ASC");
    let rows = builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .context("failed to order the address's candidate names")?;
    let mut ordered = Vec::with_capacity(rows.len());
    let mut start = None;
    for (index, row) in rows.iter().enumerate() {
        ordered.push(row.try_get::<String, _>("logical_name_id")?);
        if start.is_none() && row.try_get::<bool, _>("from_cursor")? {
            start = Some(index);
        }
    }
    Ok(Some(WalkOrder {
        start: start.unwrap_or(ordered.len()),
        ids: ordered,
        display,
    }))
}

/// A candidate's stored sort timestamp in the page's units: the name summary's exact expiry
/// seconds or registration time. The walk never runs for `sort=created_at` (`super`).
fn push_walk_timestamp(builder: &mut QueryBuilder<'_, Postgres>, sort: AddressNamesCurrentSort) {
    builder.push(match sort {
        AddressNamesCurrentSort::ExpiresAt => "summary.expires_at",
        AddressNamesCurrentSort::RegisteredAt => "EXTRACT(EPOCH FROM summary.registered_at)",
        AddressNamesCurrentSort::CreatedAt | AddressNamesCurrentSort::Name => "NULL::NUMERIC",
    });
}

/// The page statement's sort timestamp of a composed name row aliased `nc`.
fn push_served_timestamp(builder: &mut QueryBuilder<'_, Postgres>, sort: AddressNamesCurrentSort) {
    match sort {
        AddressNamesCurrentSort::ExpiresAt => push_expires_at_timestamp_expr(builder),
        AddressNamesCurrentSort::RegisteredAt => {
            builder.push("EXTRACT(EPOCH FROM ");
            push_registered_at_timestamp_expr(builder);
            builder.push(")");
        }
        AddressNamesCurrentSort::CreatedAt | AddressNamesCurrentSort::Name => {
            builder.push("NULL::NUMERIC");
        }
    }
}

/// Whether every composed name row serves the sort timestamp the walk ordered it by.
pub(super) async fn timestamps_agree(
    conn: &mut PgConnection,
    sort: AddressNamesCurrentSort,
    names: &[Value],
) -> Result<bool> {
    if names.is_empty() {
        return Ok(true);
    }
    let names = Value::Array(names.to_vec());
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:families.records.address_name_walk_keys */
         SELECT nc.logical_name_id FROM JSONB_TO_RECORDSET(",
    );
    builder.push_bind(&names);
    builder.push(
        ") AS nc(logical_name_id text, declared_summary jsonb, provenance jsonb)
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = nc.logical_name_id
         LEFT JOIN bigname_phase.project_name_summary summary
           ON summary.chain_id = surface.chain_id
          AND summary.logical_name_id = surface.logical_name_id
         WHERE (",
    );
    push_served_timestamp(&mut builder, sort);
    builder.push(") IS DISTINCT FROM (");
    push_walk_timestamp(&mut builder, sort);
    builder.push(") LIMIT 1");
    let disagreeing: Option<String> = builder
        .build_query_scalar()
        .fetch_optional(&mut *conn)
        .await
        .context("failed to check the walked names' sort keys")?;
    Ok(disagreeing.is_none())
}

/// `CASE` ranking a null sort timestamp where the page statement puts it.
fn null_rank(column: &str, order: AddressNamesCurrentOrder) -> String {
    match order {
        AddressNamesCurrentOrder::Asc => format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END"),
        AddressNamesCurrentOrder::Desc => format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END"),
    }
}

/// Whether a keyed candidate is at or after the cursor's sort position and name id in walk
/// order: its own group can still follow the cursor, which the page statement decides.
fn push_from_cursor<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    request: &AddressNamesPageRequest<'a>,
) {
    let Some(cursor) = request.cursor else {
        builder.push("TRUE");
        return;
    };
    let after = match request.order {
        AddressNamesCurrentOrder::Asc => " > ",
        AddressNamesCurrentOrder::Desc => " < ",
    };
    match (&cursor.sort_value, request.sort.is_timestamp()) {
        (AddressNamesCurrentSortedCursorValue::Name(name), false) => {
            builder.push("(display");
            builder.push(after);
            builder.push_bind(name.as_str());
            builder.push(" OR (display = ");
            builder.push_bind(name.as_str());
            builder.push(" AND logical_name_id >= ");
            builder.push_bind(cursor.logical_name_id.as_str());
            builder.push("))");
        }
        (AddressNamesCurrentSortedCursorValue::Timestamp(value), true) => {
            let rank = null_rank("sort_timestamp", request.order);
            let cursor_rank = match (value.is_none(), request.order) {
                (true, AddressNamesCurrentOrder::Asc) | (false, AddressNamesCurrentOrder::Desc) => {
                    0
                }
                _ => 1,
            };
            builder.push(format!(
                "({rank} > {cursor_rank} OR ({rank} = {cursor_rank} AND "
            ));
            match value {
                None => {
                    builder.push("sort_timestamp IS NULL");
                }
                Some(value) => {
                    builder.push("(sort_timestamp");
                    builder.push(after);
                    builder.push_bind(*value);
                    builder.push(" OR sort_timestamp = ");
                    builder.push_bind(*value);
                    builder.push(")");
                }
            }
            builder.push(" AND (sort_timestamp IS DISTINCT FROM ");
            match value {
                None => builder.push("NULL"),
                Some(value) => builder.push_bind(*value),
            };
            builder.push(" OR logical_name_id >= ");
            builder.push_bind(cursor.logical_name_id.as_str());
            builder.push(")))");
        }
        // The page statement refuses a cursor of another sort.
        _ => {
            builder.push("TRUE");
        }
    }
}
