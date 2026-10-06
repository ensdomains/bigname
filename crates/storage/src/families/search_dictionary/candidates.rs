//! Exact lexical candidates, with live identity and Project eligibility before each page limit.
use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgConnection;

use crate::NameCurrentListFilter;

type Candidate = (String, String, String, String);
const RAW_SHORT: &str = r"/* storage:families.name.search_candidates */
     SELECT surface.logical_name_id, surface.raw_name, surface.namespace, surface.namehash
     FROM bigname_phase.name_surfaces surface
     JOIN bigname_phase.chain_lineage lineage
       ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
     JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
     WHERE (NOT $8::bool OR NOT EXISTS (SELECT 1 FROM bigname_phase.project_name_summary fields WHERE fields.logical_name_id=surface.logical_name_id AND fields.chain_id=surface.chain_id AND NOT fields.search_supported)) AND surface.visibility_state = 'active' AND surface.raw_name <> ''
       AND octet_length(surface.raw_name) <= 2000
       AND surface.block_number <= marker.current_block_number
       AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
       AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
       AND ($1::text[] IS NULL OR surface.namespace = ANY($1))
       AND ($2::text IS NULL OR surface.raw_name = $2)
       AND ($3::text IS NULL OR surface.raw_name LIKE $3 ESCAPE '\')
       AND (surface.raw_name, surface.namespace, surface.namehash)
           > (COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''))
     ORDER BY surface.raw_name ASC, surface.namespace ASC, surface.namehash ASC
     LIMIT $7";
const K: usize = 1024;

const DENSE_SHORT: &str = r#"SELECT rendered.logical_name_id, rendered.name, rendered.namespace, rendered.namehash
      FROM bigname_phase.name_search_documents rendered
      WHERE rendered.spelling_class = 1
        AND (NOT $8::bool OR NOT EXISTS (SELECT 1 FROM bigname_phase.project_name_summary fields WHERE fields.logical_name_id=rendered.logical_name_id AND fields.chain_id=rendered.chain_id AND NOT fields.search_supported))
        AND ($1::text[] IS NULL OR rendered.namespace = ANY($1))
        AND ($2::text IS NULL OR rendered.name = $2)
        AND ($3::text IS NULL OR rendered.name LIKE $3 ESCAPE '\')
        AND (rendered.name, rendered.namespace, rendered.namehash)
            > (COALESCE($4, ''), COALESCE($5, ''), COALESCE($6, ''))
        AND EXISTS (
        SELECT surface.logical_name_id
      FROM bigname_phase.name_surfaces surface
      JOIN bigname_phase.chain_lineage lineage
        ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
      JOIN bigname_phase.project_family_marker marker ON marker.chain_id = surface.chain_id
      WHERE surface.logical_name_id = rendered.logical_name_id
        AND surface.namespace = rendered.namespace AND surface.namehash = rendered.namehash
        AND surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels, 0) IS NULL AND surface.visibility_state = 'active' AND surface.raw_name IS DISTINCT FROM ''
        AND surface.block_number <= marker.current_block_number
        AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        OFFSET 0
      )
      ORDER BY rendered.name ASC, rendered.namespace ASC, rendered.namehash ASC
      LIMIT $7;"#;

const FINITE: &str = r#"SELECT rendered.logical_name_id,rendered.name,rendered.namespace,rendered.namehash
FROM bigname_phase.name_search_documents rendered
WHERE rendered.search_id=ANY($9::bigint[])
 AND (NOT $8::bool OR NOT EXISTS (SELECT 1 FROM bigname_phase.project_name_summary fields WHERE fields.logical_name_id=rendered.logical_name_id AND fields.chain_id=rendered.chain_id AND NOT fields.search_supported))
 AND ($1::text[] IS NULL OR rendered.namespace=ANY($1))
 AND ($2::text IS NULL OR rendered.name=$2)
 AND ($3::text IS NULL OR rendered.name LIKE $3 ESCAPE '\')
 AND (rendered.name,rendered.namespace,rendered.namehash)>(COALESCE($4,''),COALESCE($5,''),COALESCE($6,''))
 AND EXISTS (
 SELECT 1 FROM bigname_phase.name_surfaces surface
 JOIN bigname_phase.chain_lineage lineage ON lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
 JOIN bigname_phase.project_family_marker marker ON marker.chain_id=surface.chain_id
 WHERE surface.logical_name_id=rendered.logical_name_id
 AND surface.namespace=rendered.namespace AND surface.namehash=rendered.namehash
 AND surface.visibility_state='active' AND surface.block_number<=marker.current_block_number
 AND surface.canonicality_state IN ('canonical','safe','finalized') AND lineage.canonicality_state IN ('canonical','safe','finalized')
 AND ((rendered.spelling_class=0 AND surface.raw_name<>'' AND octet_length(surface.raw_name)<=2000)
 OR (rendered.spelling_class IN (1,2) AND surface.raw_name IS NULL AND hash_array_extended(surface.raw_labels,0) IS NULL))
 OFFSET 0)
ORDER BY rendered.name,rendered.namespace,rendered.namehash LIMIT $7;"#;

const PROBE: &str = r#"SELECT search_id FROM bigname_phase.name_search_postings
WHERE namespace=$1 AND spelling_class=$2 AND token_kind=$3 AND token_length=$4 AND token_bytes=$5
ORDER BY search_id LIMIT $6;"#;

const ALL_TOKEN: &str = r#"SELECT search_id FROM bigname_phase.name_search_postings
WHERE namespace=$1 AND spelling_class=$2 AND token_kind=$3 AND token_length=$4 AND token_bytes=$5
ORDER BY search_id;"#;

const CLASS_IDS: &str = r#"SELECT search_id FROM bigname_phase.name_search_documents WHERE namespace=$1 AND spelling_class=$2 ORDER BY search_id;"#;

const MERGE: &str = r#"SELECT logical_name_id,name,namespace,namehash FROM unnest($1::text[],$2::text[],$3::text[],$4::text[]) merged(logical_name_id,name,namespace,namehash)
ORDER BY name,namespace,namehash LIMIT $5;"#;

fn terms(filter: &NameCurrentListFilter) -> Vec<(i16, String)> {
    let (mode, query) = if let Some(name) = &filter.name {
        ("exact", name.clone())
    } else if let Some(prefix) = &filter.prefix {
        ("prefix", prefix.clone())
    } else if let Some(contains) = &filter.contains {
        ("contains", contains.clone())
    } else if let Some(contains) = &filter.contains_nocase {
        ("contains", contains.to_ascii_lowercase())
    } else {
        return Vec::new();
    };
    crate::identity_search::tokens::required(&query, mode == "prefix")
}

pub(super) async fn load(
    conn: &mut PgConnection,
    filter: &NameCurrentListFilter,
    namespaces: Option<Vec<String>>,
    like: Option<String>,
    after: Option<&(String, String, String)>,
    limit: usize,
    supported_before_limit: bool,
) -> Result<Vec<Candidate>> {
    let scope = match &namespaces {
        Some(namespaces) => namespaces.clone(),
        None => sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT namespace FROM bigname_phase.name_search_documents ORDER BY namespace",
        )
        .fetch_all(&mut *conn)
        .await
        .context("failed to read search namespaces")?,
    };
    let necessary = terms(filter);
    let mut dense = BTreeSet::new();
    let mut selected = [Vec::new(), Vec::new(), Vec::new()];
    for class in 0_i16..=2 {
        for namespace in &scope {
            if necessary.is_empty() {
                if class < 2 {
                    dense.insert(class);
                    break;
                }
                let ids = sqlx::query_scalar::<_, i64>(CLASS_IDS)
                    .bind(namespace)
                    .bind(class)
                    .fetch_all(&mut *conn)
                    .await?;
                selected[class as usize].extend(ids);
                continue;
            }
            let mut complete = None;
            for (kind, token) in &necessary {
                let ids = sqlx::query_scalar::<_, i64>(PROBE)
                    .bind(namespace)
                    .bind(class)
                    .bind(kind)
                    .bind(i16::try_from(token.chars().count())?)
                    .bind(token.as_bytes())
                    .bind(i64::try_from(K + 1)?)
                    .fetch_all(&mut *conn)
                    .await?;
                if ids.len() <= K {
                    complete = Some(ids);
                    break;
                }
            }
            if let Some(ids) = complete {
                selected[class as usize].extend(ids);
            } else if class < 2 {
                dense.insert(class);
            } else {
                let (kind, token) = &necessary[0];
                let ids = sqlx::query_scalar::<_, i64>(ALL_TOKEN)
                    .bind(namespace)
                    .bind(class)
                    .bind(kind)
                    .bind(i16::try_from(token.chars().count())?)
                    .bind(token.as_bytes())
                    .fetch_all(&mut *conn)
                    .await?;
                selected[class as usize].extend(ids);
            }
        }
    }
    let limit = i64::try_from(limit).context("search batch exceeds i64")?;
    let mut candidates = Vec::new();
    for class in 0_i16..=2 {
        let statement = if dense.contains(&class) {
            if class == 0 { RAW_SHORT } else { DENSE_SHORT }
        } else {
            FINITE
        };
        let mut query = sqlx::query_as::<_, Candidate>(statement)
            .bind(namespaces.clone())
            .bind(filter.name.as_deref())
            .bind(like.as_deref())
            .bind(after.map(|(name, ..)| name.as_str()))
            .bind(after.map(|(_, namespace, _)| namespace.as_str()))
            .bind(after.map(|(.., namehash)| namehash.as_str()))
            .bind(limit)
            .bind(supported_before_limit);
        if !dense.contains(&class) {
            let ids = &mut selected[class as usize];
            ids.sort_unstable();
            ids.dedup();
            query = query.bind(ids.as_slice());
        }
        candidates.extend(query.fetch_all(&mut *conn).await?);
    }
    sqlx::query_as::<_, Candidate>(MERGE)
        .bind(
            candidates
                .iter()
                .map(|row| row.0.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            candidates
                .iter()
                .map(|row| row.1.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            candidates
                .iter()
                .map(|row| row.2.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            candidates
                .iter()
                .map(|row| row.3.clone())
                .collect::<Vec<_>>(),
        )
        .bind(limit)
        .fetch_all(conn)
        .await
        .context("failed to merge search candidates")
}
