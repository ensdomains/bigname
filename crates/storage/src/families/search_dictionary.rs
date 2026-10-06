//! Search reads identity-owned spelling and Project-owned public fields on one publication.
//! Writers maintain both stores in their existing transactions; API reads never repair them.
mod candidates;
mod pointer_parity;
/// Read-only, disposable Gate 1 comparison with the original per-resource qualifier.
pub use pointer_parity::verify as verify_basenames_pointer_batch;
pub mod shape;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;
use uuid::Uuid;

use super::{
    basenames_context,
    name::{FamilyPublication, servable_publication},
};
use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListFilter,
    public_name_fields::{SearchFields, resolve_created_at},
};

#[derive(Clone, Debug)]
pub struct SearchRow {
    pub name: String,
    pub display_name: String,
    pub namespace: String,
    pub namehash: String,
    pub owner: Option<String>,
    pub authority: Option<String>,
    pub fields: SearchFields,
    pub created_at: Option<String>,
}

pub struct SearchPage {
    pub rows: Vec<SearchRow>,
    pub next_cursor: Option<NameCurrentListCursor>,
}

/// The API keeps its existing admission, metadata, revalidation and public serialization.
/// Only the selected storage page changes; every field/context read shares this snapshot.
pub async fn load_page(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<SearchPage> {
    load_page_for_scope(db, filter, cursor, page_size, true).await
}

/// An unavailable explicit scope retains the original lexical walk's publication checks,
/// including candidates which will be omitted for unsupported authority. Empty matches still
/// return an empty page. Ready scopes exclude unsupported summaries before their page limit.
pub async fn load_page_for_scope(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
    scope_ready: bool,
) -> Result<SearchPage> {
    ensure!(
        filter.address.is_none() && filter.resolver.is_none() && filter.is_migrated != Some(true),
        "compact search accepts only the public search predicates"
    );
    ensure!(
        filter.supported_only && page_size > 0,
        "compact search requires supported rows and a positive page"
    );
    let limit = usize::try_from(page_size)?
        .checked_add(1)
        .context("search lookahead overflow")?;
    let namespaces = filter
        .namespaces
        .as_ref()
        .filter(|values| !values.is_empty())
        .cloned()
        .or_else(|| filter.namespace.as_ref().map(|value| vec![value.clone()]));
    let like = filter
        .prefix
        .as_ref()
        .map(|value| format!("{}%", escape_like(value)))
        .or_else(|| {
            filter
                .contains
                .as_ref()
                .map(|value| format!("%{}%", escape_like(value)))
        })
        .or_else(|| {
            filter
                .contains_nocase
                .as_ref()
                .map(|value| format!("%{}%", escape_like(&value.to_ascii_lowercase())))
        });
    let mut after = cursor.map(|value| {
        (
            value.normalized_name.clone(),
            value.namespace.clone(),
            value.namehash.clone(),
        )
    });
    let mut snapshot = db.into().snapshot().await?;
    let batch = if scope_ready {
        limit
    } else {
        limit.saturating_mul(4).max(200)
    };
    let mut rows = Vec::new();
    loop {
        let candidates = candidates::load(
            &mut snapshot,
            filter,
            namespaces.clone(),
            like.clone(),
            after.as_ref(),
            batch,
            scope_ready,
        )
        .await?;
        let exhausted = candidates.len() < batch;
        after = candidates
            .last()
            .map(|(_, name, namespace, hash)| (name.clone(), namespace.clone(), hash.clone()))
            .or(after);
        rows.extend(load_rows(&mut snapshot, &candidates, !scope_ready).await?);
        if scope_ready || exhausted || rows.len() >= limit {
            break;
        }
    }
    let next_cursor = if rows.len() > usize::try_from(page_size)? {
        rows.truncate(usize::try_from(page_size)?);
        rows.last().map(|row| NameCurrentListCursor {
            sort_value: NameCurrentListCursorValue::Name(row.display_name.clone()),
            namespace: row.namespace.clone(),
            normalized_name: row.name.clone(),
            namehash: row.namehash.clone(),
        })
    } else {
        None
    };
    snapshot.close().await?;
    Ok(SearchPage { rows, next_cursor })
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}

async fn load_rows(
    conn: &mut PgConnection,
    candidates: &[(String, String, String, String)],
    omit_unsupported: bool,
) -> Result<Vec<SearchRow>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<_> = candidates.iter().map(|(id, ..)| id.clone()).collect();
    let source=sqlx::query("/* storage:search.fields */
        SELECT document.name,document.namespace,document.namehash,surface.chain_id,fields.search_supported,
               document.display_name_override,fields.owner,fields.public_authority,fields.search_fields,
               fields.search_creation_transport_resource_id
        FROM unnest($1::text[]) WITH ORDINALITY requested(logical_name_id,position)
        JOIN bigname_phase.name_surfaces surface USING(logical_name_id)
        LEFT JOIN bigname_phase.name_search_documents document
          ON document.logical_name_id=surface.logical_name_id AND document.chain_id=surface.chain_id
        LEFT JOIN bigname_phase.project_name_summary fields
          ON fields.logical_name_id=surface.logical_name_id AND fields.chain_id=surface.chain_id
        ORDER BY requested.position")
        .bind(ids).fetch_all(&mut *conn).await?;
    ensure!(
        source.len() == candidates.len(),
        "search identities are incomplete"
    );
    let mut publications: BTreeMap<String, FamilyPublication> = BTreeMap::new();
    for record in &source {
        let chain: String = record.try_get("chain_id")?;
        if !publications.contains_key(&chain) {
            let publication = servable_publication(conn, &chain).await?;
            publications.insert(chain, publication);
        }
    }
    let mut rows = Vec::with_capacity(source.len());
    let mut transport = Vec::new();
    for (record, candidate) in source.into_iter().zip(candidates) {
        if omit_unsupported && record.try_get::<Option<bool>, _>("search_supported")? == Some(false)
        {
            continue;
        }
        ensure!(
            record.try_get::<Option<bool>, _>("search_supported")? == Some(true),
            "search summary is incomplete for a published identity"
        );
        let fields: Value = record.try_get("search_fields")?;
        let fields: SearchFields =
            serde_json::from_value(fields).context("invalid stored search fields")?;
        let chain: String = record.try_get("chain_id")?;
        let publication = publications
            .get(&chain)
            .context("search fields reference an unadmitted chain")?;
        let name: String = record.try_get("name")?;
        let namespace: String = record.try_get("namespace")?;
        let namehash: String = record.try_get("namehash")?;
        ensure!(
            (&name, &namespace, &namehash) == (&candidate.1, &candidate.2, &candidate.3),
            "search identity differs between selection and payload read"
        );
        let positions = json!({"source":{"timestamp":publication.block_timestamp_json}});
        let created_at = resolve_created_at(
            fields.registration.created_at_declared.as_deref(),
            &positions,
        );
        if fields.registration.created_at_declared.is_none()
            && let Some(resource) =
                record.try_get::<Option<Uuid>, _>("search_creation_transport_resource_id")?
        {
            ensure!(
                namespace == "basenames" && chain == "base-mainnet",
                "invalid search creation transport recipe"
            );
            transport.push((rows.len(), resource));
        }
        rows.push(SearchRow {
            display_name: record
                .try_get::<Option<String>, _>("display_name_override")?
                .unwrap_or_else(|| name.clone()),
            name,
            namespace,
            namehash,
            owner: record.try_get("owner")?,
            authority: record.try_get("public_authority")?,
            fields,
            created_at,
        });
    }
    if !transport.is_empty() {
        let publication = publications
            .get("base-mainnet")
            .context("Base search publication missing")?;
        if let Some(execution) = basenames_context::execution(conn, publication).await? {
            let resources: Vec<Uuid> = transport.iter().map(|(_, id)| *id).collect();
            let qualified = basenames_context::qualified_pointers(conn, &resources).await?;
            let positions = json!({"base":{"timestamp":publication.block_timestamp_json},"ethereum":{"timestamp":execution.timestamp}});
            for (index, resource) in transport {
                if qualified.contains_key(&resource) {
                    rows[index].created_at = resolve_created_at(None, &positions);
                }
            }
        }
    }
    Ok(rows)
}
