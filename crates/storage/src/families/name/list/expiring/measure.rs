//! Manual measurement of the exact selector statement, including executed generic plans.
//! PREPARE/EXECUTE is deliberate: EXPLAIN of a directly bound SELECT does not establish that
//! PostgreSQL used the generic plan of the SELECT itself.
use super::*;

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Explain the actual selector after applying the requested plan-cache mode. The values below
/// follow `push_expiring_selection`'s bindings; PostgreSQL checks their arity and types. This is
/// a test-support seam, never a production query path. Call on a dedicated measurement connection.
pub async fn explain_expiring_selection(
    conn: &mut PgConnection,
    filter: &NameCurrentExpiringFilter,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    mode: &str,
) -> Result<Value> {
    ensure!(matches!(
        mode,
        "auto" | "force_custom_plan" | "force_generic_plan"
    ));
    filter.validate_windows()?;
    let limit = page_size.checked_add(1).context("limit overflow")?;
    let query = if let [window] = filter.windows.as_slice() {
        expiring_names_query(
            "",
            &ExpiringSelection::of(filter, *window, order, cursor),
            limit,
        )?
    } else {
        expiring_union_query("", filter, order, cursor, limit)?
    };
    let mut values = Vec::new();
    for window in &filter.windows {
        values.push(literal(&filter.namespace));
        match filter.authorities.as_deref() {
            None => {}
            Some([authority]) => values.push(literal(authority)),
            Some(authorities) => values.push(format!(
                "ARRAY[{}]::text[]",
                authorities
                    .iter()
                    .map(|value| literal(value))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
        if let Some(parent) = &filter.parent {
            let (one_below, deeper) = crate::name_current::parent_like_patterns(parent);
            values.push(literal(&one_below));
            values.push(literal(&deeper));
        }
        if let Some(at) = window.expires_after {
            values.push(at.to_string());
        }
        if let Some(at) = window.expires_before {
            values.push(at.to_string());
        }
        if let Some(cursor) = cursor {
            let NameCurrentListCursorValue::Timestamp(Some(at)) = cursor.sort_value else {
                bail!("an expiry cursor must have a timestamp");
            };
            values.extend([
                at.to_string(),
                at.to_string(),
                at.to_string(),
                literal(&cursor.namespace),
                literal(&cursor.normalized_name),
                literal(&cursor.namehash),
            ]);
        }
        values.push(limit.to_string());
    }
    if filter.windows.len() > 1 {
        values.push(limit.to_string());
    }
    sqlx::raw_sql(&format!(
        "SET plan_cache_mode = {mode}; SET standard_conforming_strings = on"
    ))
    .execute(&mut *conn)
    .await?;
    sqlx::raw_sql(&format!("PREPARE expiry_measure AS {}", query.sql()))
        .execute(&mut *conn)
        .await?;
    let explained: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE expiry_measure({})",
        values.join(",")
    ))
    .fetch_one(&mut *conn)
    .await?;
    let (generic, custom): (i64, i64) = sqlx::query_as(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = 'expiry_measure'")
        .fetch_one(&mut *conn).await?;
    sqlx::raw_sql("DEALLOCATE expiry_measure")
        .execute(&mut *conn)
        .await?;
    Ok(
        serde_json::json!({"mode":mode, "generic_plans":generic, "custom_plans":custom,
        "plan":explained, "sql":query.sql()}),
    )
}
