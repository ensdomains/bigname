//! Retained full-composition oracle and same-ID component comparison for the search experiment.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;

use super::{CoverageShape, batch, search_candidates, search_rows, source_row};
use crate::{
    NameCurrentListCursor, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListSort, name_current::list_page_from,
};

/// The original search walk and full composition, kept only for bounded equivalence tests.
pub async fn load_full_search_page(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<NameCurrentListPage> {
    anyhow::ensure!(filter.address.is_none(), "search has no address filter");
    let batch_size = usize::try_from(page_size)?
        .saturating_add(1)
        .saturating_mul(4)
        .max(200);
    let mut after = cursor.map(|cursor| {
        (
            cursor.normalized_name.clone(),
            cursor.namespace.clone(),
            cursor.namehash.clone(),
        )
    });
    let mut conn = batch::read_snapshot(pool).await?;
    let mut names = BTreeSet::new();
    let mut source = BTreeMap::new();
    loop {
        let candidates = search_candidates(&mut conn, filter, after.as_ref(), batch_size).await?;
        let exhausted = candidates.len() < batch_size;
        after = candidates
            .last()
            .map(|(_, name, space, hash)| (name.clone(), space.clone(), hash.clone()))
            .or(after);
        let fresh: Vec<String> = candidates
            .into_iter()
            .map(|(name, ..)| name)
            .filter(|name| names.insert(name.clone()))
            .collect();
        source.extend(batch::load(&mut conn, &fresh, CoverageShape::Plain).await?);
        let values = Value::Array(source.values().map(source_row).collect());
        let page = list_page_from(
            &mut *conn,
            filter,
            (NameCurrentListSort::Name, NameCurrentListOrder::Asc),
            cursor,
            page_size,
            &values,
        )
        .await?;
        if exhausted || page.next_cursor.is_some() {
            conn.commit().await?;
            return Ok(page);
        }
    }
}

/// Same IDs, publication snapshot, final predicates and row decoder; only composition differs.
pub async fn load_search_component(
    pool: &PgPool,
    filter: &NameCurrentListFilter,
    ids: &[String],
    lean: bool,
) -> Result<NameCurrentListPage> {
    let mut conn = batch::read_snapshot(pool).await?;
    let rows = if lean {
        search_rows::load(&mut conn, ids).await?
    } else {
        batch::load(&mut conn, ids, CoverageShape::Plain).await?
    };
    let values = Value::Array(rows.values().map(source_row).collect());
    let page = list_page_from(
        &mut *conn,
        filter,
        (NameCurrentListSort::Name, NameCurrentListOrder::Asc),
        None,
        u64::try_from(ids.len())?.max(1),
        &values,
    )
    .await?;
    conn.commit().await?;
    Ok(page)
}
