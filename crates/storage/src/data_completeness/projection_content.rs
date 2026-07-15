use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use crate::{
    address_names::DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    children::{DECLARED_SURFACE_CLASS, DEFAULT_CHILDREN_CURRENT_READ_FILTER},
    name_current::DEFAULT_NAME_CURRENT_READ_FILTER,
    permissions::DEFAULT_PERMISSIONS_CURRENT_READ_FILTER,
    record_inventory::DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionContentScopeCount {
    pub scope: String,
    pub count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionContentCounts {
    pub scope_kind: &'static str,
    pub raw_total_count: i64,
    pub raw_scoped_counts: Vec<ProjectionContentScopeCount>,
    pub servable_total_count: i64,
    pub servable_scoped_counts: Vec<ProjectionContentScopeCount>,
}

/// Count one current projection both as stored and through the same supporting-row validity
/// rules used by its normal serving reads.
pub async fn load_projection_content_counts(
    pool: &PgPool,
    projection: &str,
) -> Result<ProjectionContentCounts> {
    let (scope_kind, raw_query, servable_query) = projection_queries(projection)?;
    let raw_scoped_counts = load_scoped_counts(pool, projection, "raw", raw_query.as_str()).await?;
    let servable_scoped_counts =
        load_scoped_counts(pool, projection, "servable", servable_query.as_str()).await?;

    Ok(ProjectionContentCounts {
        scope_kind,
        raw_total_count: raw_scoped_counts.iter().map(|entry| entry.count).sum(),
        raw_scoped_counts,
        servable_total_count: servable_scoped_counts.iter().map(|entry| entry.count).sum(),
        servable_scoped_counts,
    })
}

fn projection_queries(projection: &str) -> Result<(&'static str, String, String)> {
    let queries = match projection {
        "name_current" => (
            "namespace",
            grouped_query("name_current", "namespace"),
            format!(
                r#"
                SELECT nc.namespace AS scope, COUNT(*)::BIGINT AS count
                FROM name_current nc
                JOIN name_surfaces surface
                  ON surface.logical_name_id = nc.logical_name_id
                LEFT JOIN resources resource
                  ON resource.resource_id = nc.resource_id
                LEFT JOIN surface_bindings binding
                  ON binding.surface_binding_id = nc.surface_binding_id
                LEFT JOIN token_lineages token_lineage
                  ON token_lineage.token_lineage_id = nc.token_lineage_id
                WHERE TRUE
                {DEFAULT_NAME_CURRENT_READ_FILTER}
                GROUP BY nc.namespace
                ORDER BY nc.namespace
                "#,
            ),
        ),
        "children_current" => (
            "namespace",
            grouped_query("children_current", "namespace"),
            format!(
                r#"
                SELECT cc.namespace AS scope, COUNT(*)::BIGINT AS count
                FROM children_current cc
                JOIN name_surfaces parent
                  ON parent.logical_name_id = cc.parent_logical_name_id
                LEFT JOIN name_surfaces child
                  ON child.logical_name_id = cc.child_logical_name_id
                WHERE cc.surface_class = '{DECLARED_SURFACE_CLASS}'
                {DEFAULT_CHILDREN_CURRENT_READ_FILTER}
                GROUP BY cc.namespace
                ORDER BY cc.namespace
                "#,
            ),
        ),
        "permissions_current" => (
            "chain",
            resource_grouped_query("permissions_current", "pc"),
            format!(
                r#"
                SELECT resource.chain_id AS scope, COUNT(*)::BIGINT AS count
                FROM permissions_current pc
                JOIN resources resource
                  ON resource.resource_id = pc.resource_id
                WHERE TRUE
                {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
                GROUP BY resource.chain_id
                ORDER BY resource.chain_id
                "#,
            ),
        ),
        "record_inventory_current" => (
            "chain",
            resource_grouped_query("record_inventory_current", "ric"),
            format!(
                r#"
                SELECT resource.chain_id AS scope, COUNT(*)::BIGINT AS count
                FROM record_inventory_current ric
                JOIN resources resource
                  ON resource.resource_id = ric.resource_id
                WHERE TRUE
                {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
                GROUP BY resource.chain_id
                ORDER BY resource.chain_id
                "#,
            ),
        ),
        "resolver_current" => {
            let query = grouped_query("resolver_current", "chain_id");
            ("chain", query.clone(), query)
        }
        "address_names_current" => (
            "namespace",
            grouped_query("address_names_current", "namespace"),
            format!(
                r#"
                SELECT anc.namespace AS scope, COUNT(*)::BIGINT AS count
                FROM address_names_current anc
                JOIN name_surfaces surface
                  ON surface.logical_name_id = anc.logical_name_id
                JOIN resources resource
                  ON resource.resource_id = anc.resource_id
                JOIN surface_bindings binding
                  ON binding.surface_binding_id = anc.surface_binding_id
                LEFT JOIN token_lineages token_lineage
                  ON token_lineage.token_lineage_id = anc.token_lineage_id
                WHERE TRUE
                {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
                GROUP BY anc.namespace
                ORDER BY anc.namespace
                "#,
            ),
        ),
        "primary_names_current" => {
            let query = grouped_query("primary_names_current", "namespace");
            ("namespace", query.clone(), query)
        }
        _ => bail!("current projection {projection} has no content-count rule"),
    };
    Ok(queries)
}

fn grouped_query(projection: &str, scope_column: &str) -> String {
    format!(
        "SELECT {scope_column} AS scope, COUNT(*)::BIGINT AS count \
         FROM {projection} GROUP BY {scope_column} ORDER BY {scope_column}"
    )
}

fn resource_grouped_query(projection: &str, alias: &str) -> String {
    format!(
        "SELECT resource.chain_id AS scope, COUNT(*)::BIGINT AS count \
         FROM {projection} {alias} JOIN resources resource \
         ON resource.resource_id = {alias}.resource_id \
         GROUP BY resource.chain_id ORDER BY resource.chain_id"
    )
}

async fn load_scoped_counts(
    pool: &PgPool,
    projection: &str,
    count_kind: &str,
    query: &str,
) -> Result<Vec<ProjectionContentScopeCount>> {
    let rows = sqlx::query(query)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load {count_kind} content counts for {projection}"))?;
    rows.into_iter()
        .map(|row| {
            Ok(ProjectionContentScopeCount {
                scope: row.try_get("scope")?,
                count: row.try_get("count")?,
            })
        })
        .collect()
}
