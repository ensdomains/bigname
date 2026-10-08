//! Atomic lexical derivation from identity and verified label evidence. Callers acquire all
//! label locks before changing source rows, then refresh names before committing that write.
mod documents;
pub mod tokens;

use anyhow::{Context, Result, ensure};
use sqlx::PgConnection;
use std::collections::BTreeMap;

const LABEL_LOCK_NAMESPACE: i32 = 228_001;
const LABEL_BUCKET_MASK: i32 = 255;

/// A fresh statement after a name-lock wait must see labels committed by the preceding writer.
/// Every production caller opens READ COMMITTED explicitly; reject a caller's older snapshot.
pub async fn require_read_committed(conn: &mut PgConnection) -> Result<()> {
    let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut *conn)
        .await?;
    ensure!(
        isolation == "read committed",
        "identity search writes require READ COMMITTED"
    );
    Ok(())
}

/// Prepare the complete write: label buckets first, then the sorted affected existing surfaces,
/// before mutating labels or identity rows. Buckets bound shared lock memory even for a full
/// rainbow recompute. Hash collisions conservatively serialize unrelated writers; exclusive
/// wins before any bucket is acquired. No new bucket or upgrade is allowed after a row lock.
pub async fn prepare(
    conn: &mut PgConnection,
    changed: &[String],
    paths: &[Vec<String>],
    names: &[String],
) -> Result<()> {
    require_read_committed(conn).await?;
    let stored: Vec<Vec<String>> = sqlx::query_scalar(
        "/* storage:identity_search.paths */ SELECT labelhashes FROM name_surfaces
         WHERE logical_name_id=ANY($1)",
    )
    .bind(names)
    .fetch_all(&mut *conn)
    .await?;
    let mut labels = BTreeMap::new();
    for label in paths.iter().chain(&stored).flatten() {
        labels.insert(label.to_ascii_lowercase(), false);
    }
    for label in changed {
        labels.insert(label.to_ascii_lowercase(), true);
    }
    let (labels, exclusive): (Vec<_>, Vec<_>) = labels.into_iter().unzip();
    sqlx::query(
        "/* storage:identity_search.label_locks */
         SELECT CASE WHEN exclusive THEN pg_advisory_xact_lock($1, lock_key)
                     ELSE pg_advisory_xact_lock_shared($1, lock_key) END
         FROM (SELECT (hashtext(label) & $4) AS lock_key, bool_or(exclusive) AS exclusive
               FROM unnest($2::text[], $3::boolean[]) input(label, exclusive)
               GROUP BY (hashtext(label) & $4)) locks ORDER BY lock_key",
    )
    .bind(LABEL_LOCK_NAMESPACE)
    .bind(labels)
    .bind(exclusive)
    .bind(LABEL_BUCKET_MASK)
    .execute(&mut *conn)
    .await
    .context("failed to lock search labels")?;
    let changed: Vec<String> = changed
        .iter()
        .map(|label| label.to_ascii_lowercase())
        .collect();
    // Existing source-row locks precede every upsert/update, including an import's label write.
    // Ordinary row locks do not consume one shared advisory-lock entry per affected name.
    sqlx::query(&format!(
        "/* storage:identity_search.name_locks */ SELECT logical_name_id
         FROM name_surfaces WHERE {AFFECTED} ORDER BY logical_name_id FOR NO KEY UPDATE",
    ))
    .bind(names)
    .bind(&changed)
    .execute(&mut *conn)
    .await
    .context("failed to lock affected search identities")?;
    Ok(())
}

/// After the prepared source mutation, derive complete spelling from fresh statements. Held
/// label buckets stop new affected paths entering; the inserted surfaces are locked by their
/// own insert. Imports of different labels in one name use the locks acquired by `prepare`.
/// The affected names are staged once in the connection's temporary table, so no page
/// statement carries them again. Buffers hold at most 100 documents. All fanout remains in
/// the source transaction. A refresh requires the caller's open transaction and fails
/// without one.
pub async fn refresh(conn: &mut PgConnection, names: &[String], labels: &[String]) -> Result<()> {
    require_read_committed(conn).await?;
    if names.is_empty() && labels.is_empty() {
        return Ok(());
    }
    let labels: Vec<String> = labels
        .iter()
        .map(|label| label.to_ascii_lowercase())
        .collect();
    // This runs after all lock waits and the source mutation on READ COMMITTED.
    documents::stage(conn, names, &labels).await?;
    let mut after = Some(String::new());
    while let Some(cursor) = after {
        let (rows, next) = documents::load(conn, &cursor).await?;
        documents::replace(conn, rows).await?;
        after = next;
    }
    Ok(())
}

const AFFECTED: &str = "(logical_name_id=ANY($1) OR
    (raw_name IS NULL AND labelhashes && $2::text[]))";

#[cfg(test)]
mod tests;
