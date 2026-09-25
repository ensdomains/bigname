use crate::{ProjectError, Result};
use sqlx::{Postgres, Transaction};

// Every operator consumes its own keys. Analyze the actual newly consumed frontier so
// broad history joins can use set plans while isolated updates retain indexed probes.
pub(super) async fn query(
    transaction: &mut Transaction<'_, Postgres>,
    operator: &str,
    statement: &str,
) -> Result<String> {
    let mut result = statement.to_owned();
    let mut keys = 0_u64;
    for (kind, column) in [("names", "logical_name_id"), ("resources", "resource_id")] {
        let source = format!("project_scope_{kind} ");
        if !statement.contains(&format!("FROM {source}"))
            && !statement.contains(&format!("JOIN {source}"))
        {
            continue;
        }
        let frontier = format!("project_binding_frontier_{kind}");
        sqlx::query(&format!(
            "/* project:scope.frontier.truncate_{frontier} */ TRUNCATE {frontier}"
        ))
        .execute(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to clear binding frontier", e))?;
        let count = sqlx::query(&format!(
            "/* project:scope.frontier.insert_{frontier} */ WITH added AS (INSERT INTO project_binding_seen_{kind}
             SELECT $1, scope.{column} FROM project_scope_{kind} scope
             WHERE NOT EXISTS (SELECT 1 FROM project_binding_seen_{kind} seen
                 WHERE seen.operator = $1 AND seen.{column} = scope.{column})
             ON CONFLICT DO NOTHING RETURNING {column})
             INSERT INTO {frontier} SELECT {column} FROM added"
        ))
        .bind(operator)
        .execute(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to populate binding frontier", e))?
        .rows_affected();
        keys += count;
        sqlx::query(&format!(
            "/* project:scope.frontier.analyze_{frontier} */ ANALYZE {frontier}"
        ))
        .execute(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to analyze binding frontier", e))?;
        result = result
            .replace(&format!("FROM {source}"), &format!("FROM {frontier} "))
            .replace(&format!("JOIN {source}"), &format!("JOIN {frontier} "));
    }
    let broad = keys > 256;
    let bounded_history = result.contains("OFFSET (0)");
    tracing::debug!(
        operator,
        new_keys = keys,
        strategy = if bounded_history {
            "bounded_history"
        } else if broad {
            "set_based"
        } else {
            "keyed"
        },
        "Project binding frontier prepared"
    );
    if broad {
        result = result.replace(" OFFSET 0", "");
    }
    Ok(result)
}
