use anyhow::{Context, Result};
use bigname_domain::normalization::normalize_name;
use serde_json::json;
use sqlx::{FromRow, PgConnection};

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

pub(super) async fn load(
    conn: &mut PgConnection,
    names: &[String],
    labels: &[String],
    after: &str,
) -> Result<Vec<Source>> {
    sqlx::query_as(&format!(
        "/* storage:identity_search.spellings */
         SELECT surface.logical_name_id, surface.chain_id, surface.namespace, surface.namehash,
                surface.raw_name, surface.visibility_state, {rendered} AS name
         FROM name_surfaces surface WHERE {affected} AND logical_name_id > $3
         ORDER BY logical_name_id LIMIT 100",
        rendered = crate::families::name::rendered::rendered_name_sql_unqualified("surface"),
        affected = super::AFFECTED,
    ))
    .bind(names)
    .bind(labels)
    .bind(after)
    .fetch_all(&mut *conn)
    .await
    .context("failed to derive current search spellings")
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
