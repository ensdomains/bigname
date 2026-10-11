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

/// The labels whose `label_preimages` rows a write may insert or rewrite. Each one's bucket is
/// taken exclusive. A changed label is one whose rendering the write can change, and every
/// unnamed surface carrying it is locked.
#[derive(Clone, Copy, Debug)]
pub enum LabelWrite<'a> {
    /// An insert-only writer, which never changes a stored row's bytes or verdict: Interpret and
    /// the rainbow import. A label that already has a row keeps its rendering, so only the
    /// labels without one are changed.
    Insert(&'a [String]),
    /// A writer that rewrites stored rows: the normalization recompute. Every label is changed.
    Rewrite(&'a [String]),
}

impl LabelWrite<'static> {
    pub const NONE: Self = Self::Insert(&[]);
}

/// Prepare the complete write: label buckets first, then the sorted affected existing surfaces,
/// before mutating labels or identity rows. Buckets bound shared lock memory even for a full
/// rainbow recompute. Hash collisions conservatively serialize unrelated writers; exclusive
/// wins before any bucket is acquired. No new bucket or upgrade is allowed after a row lock.
///
/// Every writer that inserts or rewrites a `label_preimages` row takes that label's bucket
/// exclusive here first, and nothing deletes from the table. So a row read after the bucket
/// locks stays as read until commit, and an insert-only writer locks no surface for a label
/// that is already stored. Returns the changed labels, lowercased, sorted and distinct.
pub async fn prepare(
    conn: &mut PgConnection,
    write: LabelWrite<'_>,
    paths: &[Vec<String>],
    names: &[String],
) -> Result<Vec<String>> {
    require_read_committed(conn).await?;
    let (written, insert_only) = match write {
        LabelWrite::Insert(labels) => (labels, true),
        LabelWrite::Rewrite(labels) => (labels, false),
    };
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
    for label in written {
        labels.insert(label.to_ascii_lowercase(), true);
    }
    let mut changed: Vec<String> = labels
        .iter()
        .filter(|(_, exclusive)| **exclusive)
        .map(|(label, _)| label.clone())
        .collect();
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
    if insert_only && !changed.is_empty() {
        changed = sqlx::query_scalar(
            "/* storage:identity_search.new_labels */ SELECT label
             FROM unnest($1::text[]) AS input(label)
             WHERE NOT EXISTS (SELECT 1 FROM label_preimages WHERE labelhash=label)
             ORDER BY label",
        )
        .bind(&changed)
        .fetch_all(&mut *conn)
        .await
        .context("failed to select new search labels")?;
    }
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
    Ok(changed)
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
