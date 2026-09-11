//! Eligible grant rows for a bounded inline expansion; the unlimited loader remains the oracle.
use super::*;

pub async fn load_bounded_effective_permissions_by_resource_ids(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
    max_rows: u64,
) -> Result<Vec<EffectivePermissionRow>> {
    let limit = checked_page_limit_i64(
        max_rows,
        "positive grant budget required",
        "grant budget too large",
    )?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    build_query("", ids, namespace, limit)
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(decode_effective_permission_row)
        .collect()
}

fn build_query<'a>(
    prefix: &str,
    ids: &'a [Uuid],
    namespace: Option<&'a str>,
    limit: i64,
) -> QueryBuilder<'a, Postgres> {
    let mut query = QueryBuilder::new(prefix);
    push_union_start(&mut query);
    push_ids(&mut query, "pc.resource_id", ids);
    push_namespace_filter(&mut query, "pc.resource_id", namespace);
    query
        .push(DEFAULT_PERMISSIONS_CURRENT_READ_FILTER)
        .push(" LIMIT ")
        .push_bind(limit);
    push_bounded_operator_start(&mut query, limit);
    push_ids(&mut query, "summary.resource_id", ids);
    push_namespace_filter(&mut query, "summary.resource_id", namespace);
    query.push(" LIMIT ").push_bind(limit);
    push_union_end(&mut query);
    // Any sentinel proves overflow. Under budget every eligible row is returned and the API
    // orders it. Avoid sorting the full expansion just to discover that it is too large.
    query
        .push("SELECT * FROM effective_permissions LIMIT ")
        .push_bind(limit);
    query
}

// Keep each account expansion correlated to one selected summary. Its eligible-row
// sentinel prevents flattening into a join that repeatedly scans the selected summaries.
fn push_bounded_operator_start(query: &mut QueryBuilder<'_, Postgres>, limit: i64) {
    query
        .push("), operator_candidates AS (SELECT ")
        .push(OPERATOR_COLUMNS)
        .push(
            " FROM bigname_phase.permissions_current_resource_summary summary \
             CROSS JOIN LATERAL (SELECT aps.* \
             FROM bigname_phase.account_permission_state_current aps \
             WHERE aps.chain_id=summary.registry_binding_provenance->>'chain_id' \
               AND aps.authority_contract=summary.registry_contract \
               AND aps.owner=summary.registry_owner \
               AND aps.approved AND aps.authority_kind='registry' \
               AND aps.relation_kind='operator'",
        )
        .push(ACCOUNT_READ_FILTER)
        .push(" LIMIT ")
        .push_bind(limit)
        .push(") aps WHERE ")
        .push(CURRENT_PERMISSION_SUMMARY_READ_FILTER);
}

/// Explain the same capped query with default planner settings; this does not force indexes.
pub async fn explain_bounded_effective_permissions_by_resource_ids(
    pool: &PgPool,
    ids: &[Uuid],
    namespace: Option<&str>,
    max_rows: u64,
) -> Result<Value> {
    let limit = checked_page_limit_i64(
        max_rows,
        "positive grant budget required",
        "grant budget too large",
    )?;
    if ids.is_empty() {
        bail!("explain requires selected resource IDs");
    }
    let row = build_query(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ",
        ids,
        namespace,
        limit,
    )
    .build()
    .fetch_one(pool)
    .await?;
    Ok(row.try_get(0)?)
}
