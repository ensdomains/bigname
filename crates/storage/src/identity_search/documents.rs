use anyhow::{Context, Result};
use bigname_domain::normalization::normalize_name;
use serde_json::json;
use sqlx::{Executor, FromRow, PgConnection, Row};

use super::tokens;

#[derive(FromRow)]
pub(super) struct Source {
    pub logical_name_id: String,
    chain_id: String,
    namespace: String,
    namehash: String,
    raw_name: Option<String>,
    visibility_state: String,
    name: Option<String>,
}

/// Rows a page statement reads and `replace` buffers.
const PAGE: usize = 100;
/// Names bound by one staging statement, which bounds the largest message of a refresh.
const STAGED_PER_STATEMENT: usize = 20_000;
/// Staged sets larger than this get planner statistics. Smaller ones write no catalog row.
pub(super) const ANALYZE_ABOVE: u64 = 50_000;
const STAGED: &str = "pg_temp.identity_search_names";
/// DELETE, not TRUNCATE. Measured over 1,000 transactions, a TRUNCATE of a table created by an
/// earlier transaction writes new `pg_class` rows each time, and DELETE writes none.
const CLEAR: &str = "DELETE FROM pg_temp.identity_search_names";

/// Write the affected name set once: the given names, and every surface without raw bytes whose
/// path holds a given label. Pages then read it in key order and bind only a cursor. The label
/// matches are complete here because the caller holds those label buckets.
///
/// The table belongs to the connection and is created on its first refresh. PostgreSQL empties
/// it at each commit, a rollback discards its rows, and a later refresh in the same transaction
/// deletes them first. So no refresh reads another's names, and a refresh on a connection that
/// already has the table changes no system catalog unless it gathers statistics.
pub(super) async fn stage(
    conn: &mut PgConnection,
    names: &[String],
    labels: &[String],
) -> Result<()> {
    // An unbound string is sent as one simple query, which may hold both statements. The
    // existence test keeps `IF NOT EXISTS` from raising a notice on every refresh.
    conn.execute(
        format!(
            "/* storage:identity_search.names_table */
             DO $$ BEGIN
                 IF to_regclass('{STAGED}') IS NULL THEN
                     CREATE TEMP TABLE identity_search_names (logical_name_id text PRIMARY KEY)
                         ON COMMIT DELETE ROWS;
                 END IF;
             END $$;
             {CLEAR}"
        )
        .as_str(),
    )
    .await
    .context("failed to create the search name staging table")?;
    // Outside a transaction block every statement commits by itself, which would empty the
    // table before the first page. LOCK TABLE is refused there and changes nothing here.
    sqlx::query(&format!(
        "/* storage:identity_search.open_transaction */ LOCK TABLE {STAGED} IN ROW EXCLUSIVE MODE"
    ))
    .execute(&mut *conn)
    .await
    .context("an identity search refresh requires the caller's open transaction")?;
    let mut staged = 0;
    for names in names.chunks(STAGED_PER_STATEMENT) {
        staged += sqlx::query(&format!(
            "/* storage:identity_search.stage_names */ INSERT INTO {STAGED} (logical_name_id)
             SELECT name FROM unnest($1::text[]) AS input(name) ON CONFLICT DO NOTHING"
        ))
        .bind(names)
        .execute(&mut *conn)
        .await
        .context("failed to stage the affected search names")?
        .rows_affected();
    }
    if !labels.is_empty() {
        staged += sqlx::query(&format!(
            "/* storage:identity_search.stage_labels */ INSERT INTO {STAGED} (logical_name_id)
             SELECT logical_name_id FROM name_surfaces
             WHERE raw_name IS NULL AND labelhashes && $1::text[] ON CONFLICT DO NOTHING"
        ))
        .bind(labels)
        .execute(&mut *conn)
        .await
        .context("failed to stage the search names of the changed labels")?
        .rows_affected();
    }
    if staged > ANALYZE_ABOVE {
        sqlx::query(&format!(
            "/* storage:identity_search.names_stats */ ANALYZE {STAGED}"
        ))
        .execute(&mut *conn)
        .await
        .context("failed to gather statistics on the staged search names")?;
    }
    Ok(())
}

/// The stored surfaces among the next staged names after `after`, and the cursor for the page
/// after that, if any. The limit applies to the staged keys alone, so the statement reads at
/// most one page of surfaces and renders at most one page of names however the join is planned.
/// A staged name with no surface advances the cursor and yields no row.
///
/// The statement is planned on every execution. A plan cached while the connection's table was
/// small would otherwise sort a later, larger set on every page of the same transaction.
pub(super) async fn load(
    conn: &mut PgConnection,
    after: &str,
) -> Result<(Vec<Source>, Option<String>)> {
    let rows = sqlx::query(&format!(
        "/* storage:identity_search.spellings */
         SELECT page.logical_name_id, surface.logical_name_id IS NOT NULL AS stored,
                surface.chain_id, surface.namespace, surface.namehash,
                surface.raw_name, surface.visibility_state, {rendered} AS name
         FROM (SELECT logical_name_id FROM {STAGED} WHERE logical_name_id > $1
               ORDER BY logical_name_id LIMIT {PAGE}) page
         LEFT JOIN name_surfaces surface ON surface.logical_name_id = page.logical_name_id
         ORDER BY page.logical_name_id",
        rendered = crate::families::name::rendered::rendered_name_sql_unqualified("surface"),
    ))
    .bind(after)
    .persistent(false)
    .fetch_all(&mut *conn)
    .await
    .context("failed to derive current search spellings")?;
    let next = match rows.last() {
        Some(last) if rows.len() == PAGE => Some(last.try_get("logical_name_id")?),
        _ => None,
    };
    let mut sources = Vec::with_capacity(rows.len());
    for row in &rows {
        if row.try_get("stored")? {
            sources.push(Source::from_row(row)?);
        }
    }
    Ok((sources, next))
}

pub(super) async fn replace(conn: &mut PgConnection, source: Vec<Source>) -> Result<()> {
    let mut documents = Vec::new();
    let mut removed = Vec::new();
    for row in source {
        let Some(name) = row.name.filter(|name| !name.is_empty()) else {
            removed.push(row.logical_name_id);
            continue;
        };
        if row.visibility_state != "active"
            || row.raw_name.as_ref().is_some_and(|raw| raw.len() > 2000)
        {
            removed.push(row.logical_name_id);
            continue;
        }
        let (name, display, class) = if row.raw_name.is_some() {
            let shaped =
                normalize_name(&name).context("active raw search name fails normalization")?;
            let display = (shaped.canonical_display_name != shaped.normalized_name)
                .then_some(shaped.canonical_display_name);
            (shaped.normalized_name, display, 0_i16)
        } else {
            let class = if name.len() <= 2000 { 1_i16 } else { 2_i16 };
            (name, None, class)
        };
        documents.push(
            json!({"logical_name_id":row.logical_name_id,"chain_id":row.chain_id,
            "namespace":row.namespace,"namehash":row.namehash,"name":name,
            "display_name_override":display,"spelling_class":class}),
        );
    }
    if !removed.is_empty() {
        sqlx::query(
            "/* storage:identity_search.remove */ DELETE FROM name_search_documents
                     WHERE logical_name_id=ANY($1)",
        )
        .bind(removed)
        .execute(&mut *conn)
        .await?;
    }
    if documents.is_empty() {
        return Ok(());
    }
    // Unchanged documents return no row, so idempotent observations do no posting work.
    let changed: Vec<(i64, String, i16, String)> = sqlx::query_as(
        "/* storage:identity_search.documents */
         INSERT INTO name_search_documents AS stored
            (logical_name_id, chain_id, namespace, namehash, name, display_name_override, spelling_class)
         SELECT logical_name_id, chain_id, namespace, namehash, name, display_name_override, spelling_class
         FROM jsonb_to_recordset($1) AS input(logical_name_id text, chain_id text, namespace text,
              namehash text, name text, display_name_override text, spelling_class smallint)
         ON CONFLICT(logical_name_id) DO UPDATE SET chain_id=EXCLUDED.chain_id,
             namespace=EXCLUDED.namespace, namehash=EXCLUDED.namehash, name=EXCLUDED.name,
             display_name_override=EXCLUDED.display_name_override, spelling_class=EXCLUDED.spelling_class
         WHERE (stored.chain_id,stored.namespace,stored.namehash,stored.name,stored.display_name_override,stored.spelling_class)
           IS DISTINCT FROM (EXCLUDED.chain_id,EXCLUDED.namespace,EXCLUDED.namehash,EXCLUDED.name,EXCLUDED.display_name_override,EXCLUDED.spelling_class)
         RETURNING search_id, namespace, spelling_class, name",
    ).bind(serde_json::Value::Array(documents)).fetch_all(&mut *conn).await?;
    if changed.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = changed.iter().map(|row| row.0).collect();
    let (mut documents, mut namespaces, mut classes) = (Vec::new(), Vec::new(), Vec::new());
    let (mut kinds, mut lengths, mut bytes) = (Vec::new(), Vec::new(), Vec::new());
    for (id, namespace, class, name) in changed {
        for (kind, length, token) in tokens::postings(&name) {
            documents.push(id);
            namespaces.push(namespace.clone());
            classes.push(class);
            kinds.push(kind);
            lengths.push(length);
            bytes.push(token);
        }
    }
    let terms =
        "unnest($1::bigint[],$2::text[],$3::smallint[],$4::smallint[],$5::smallint[],$6::bytea[])
                 AS wanted(search_id,namespace,spelling_class,token_kind,token_length,token_bytes)";
    sqlx::query(&format!(
        "/* storage:identity_search.remove_postings */ DELETE FROM name_search_postings old
         WHERE old.search_id=ANY($7) AND NOT EXISTS (SELECT 1 FROM {terms}
          WHERE (old.search_id,old.namespace,old.spelling_class,old.token_kind,old.token_length,old.token_bytes)
              =(wanted.search_id,wanted.namespace,wanted.spelling_class,wanted.token_kind,wanted.token_length,wanted.token_bytes))"
    )).bind(&documents).bind(&namespaces).bind(&classes).bind(&kinds).bind(&lengths).bind(&bytes)
        .bind(ids).execute(&mut *conn).await?;
    sqlx::query(&format!(
        "/* storage:identity_search.add_postings */ INSERT INTO name_search_postings
            (search_id,namespace,spelling_class,token_kind,token_length,token_bytes)
         SELECT search_id,namespace,spelling_class,token_kind,token_length,token_bytes
         FROM {terms} ON CONFLICT DO NOTHING"
    ))
    .bind(&documents)
    .bind(&namespaces)
    .bind(&classes)
    .bind(&kinds)
    .bind(&lengths)
    .bind(&bytes)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
