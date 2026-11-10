//! `relation=former_owner` over the stored address listing: the names whose ended registration
//! the address last held (`families/records/former_owners.rs` says which), a keyset walk of
//! `project_address_listing_former_owner_idx` for the address. Only the page's names are composed.
//!
//! Rows sort by the served expiry, then namespace, name and namehash, as the index keys them: a
//! name past 2,000 bytes by its first 500 characters, as `display_key` cuts it. A row without a
//! served expiry is stored as `MISSING_TIMESTAMP`, the smallest value, and an expiry window leaves
//! it out.
use anyhow::{Context, Result};
use sqlx::{FromRow, Postgres, QueryBuilder};

use crate::{
    NameCurrentListCursor, NameCurrentListCursorValue, NameCurrentListOrder, UnixSeconds,
    families::{
        name::{CoverageShape, load_composed},
        records::{FormerOwnerFilter, FormerOwnerPage, load_family_former_owner_page, seams},
        topology::MISSING_TIMESTAMP,
    },
};

/// The index's name key, `normalized_name` cut as `display_key` is.
const NAME_KEY: &str = "CASE WHEN octet_length(l.normalized_name) <= 2000 THEN l.normalized_name \
     ELSE left(l.normalized_name, 500) END";

/// A position in the index's key: expiry, namespace, name, namehash.
type Position = (UnixSeconds, String, String, String);

#[derive(FromRow)]
struct Former {
    logical_name_id: String,
    former_expires_key: UnixSeconds,
    namespace: String,
    normalized_name: String,
    namehash: String,
}

/// The page of names `filter.address` formerly held, on the chains whose publication the read
/// holds (`super::held_publications`).
pub async fn load_address_former_owner_page(
    db: impl Into<crate::ReadDb<'_>>,
    filter: &FormerOwnerFilter<'_>,
    order: NameCurrentListOrder,
    cursor: Option<&NameCurrentListCursor>,
    page_size: u64,
) -> Result<FormerOwnerPage> {
    let address = filter.address.to_ascii_lowercase();
    let after = cursor.map(cursor_key).transpose()?;
    let size = usize::try_from(page_size).context("page_size does not fit in usize")?;
    if seams::composed_address_names() {
        return load_family_former_owner_page(db, filter, order, cursor, page_size).await;
    }
    let mut snapshot = db.into().snapshot().await?;
    seams::note_address_name_path("listing");
    let chains: Vec<String> = super::held_publications(&mut snapshot, &address)
        .await?
        .into_keys()
        .collect();
    let statement = || former_statement(&address, &chains, filter, order, after.as_ref(), size);
    #[cfg(any(test, feature = "test-support"))]
    super::statement::explain(&mut *snapshot, "former_page", statement).await?;
    let mut held: Vec<Former> = statement()?
        .build_query_as()
        .fetch_all(&mut *snapshot)
        .await
        .with_context(|| format!("failed to read the former names of {address}"))?;
    let more = held.len() > size;
    held.truncate(size);
    let next_cursor = more
        .then(|| held.last())
        .flatten()
        .map(|last| NameCurrentListCursor {
            sort_value: NameCurrentListCursorValue::Timestamp(
                (last.former_expires_key != missing()).then_some(last.former_expires_key),
            ),
            namespace: last.namespace.clone(),
            normalized_name: last.normalized_name.clone(),
            namehash: last.namehash.clone(),
        });
    let ids: Vec<String> = held
        .iter()
        .map(|held| held.logical_name_id.clone())
        .collect();
    seams::note_composed_batch(ids.len());
    let mut composed = load_composed(&mut snapshot, &ids, CoverageShape::Plain).await?;
    snapshot.close().await?;
    // A stored row whose name no longer composes in this snapshot leaves the page.
    let rows = ids.iter().filter_map(|id| composed.remove(id)).collect();
    Ok(FormerOwnerPage { rows, next_cursor })
}

/// The page statement: the address's former-owner rows on `chains` in the index's order, after
/// `after`, one more than `size`.
fn former_statement(
    address: &str,
    chains: &[String],
    filter: &FormerOwnerFilter<'_>,
    order: NameCurrentListOrder,
    after: Option<&Position>,
    size: usize,
) -> Result<QueryBuilder<'static, Postgres>> {
    let (direction, comparison) = match order {
        NameCurrentListOrder::Asc => ("ASC", ">"),
        NameCurrentListOrder::Desc => ("DESC", "<"),
    };
    let mut builder = QueryBuilder::<Postgres>::new(
        "/* storage:address_names.listing_former_page */
         SELECT l.logical_name_id, l.former_expires_key, l.namespace, l.normalized_name, l.namehash
         FROM bigname_phase.project_address_listing l
         WHERE l.address = ",
    );
    builder.push_bind(address.to_owned());
    builder.push(" AND l.former_owner AND l.chain_id = ANY(");
    builder.push_bind(chains.to_vec());
    builder.push(")");
    if let Some(namespace) = filter.namespace {
        builder.push(" AND l.namespace = ");
        builder.push_bind(namespace.to_owned());
    }
    if let Some(parent) = filter.parent {
        builder.push(" AND l.parent_normalized_name = ");
        builder.push_bind(parent.to_owned());
    }
    if let Some(expires_after) = filter.expires_after {
        builder.push(format!(
            " AND l.former_expires_key <> {MISSING_TIMESTAMP} AND l.former_expires_key >= "
        ));
        builder.push_bind(expires_after);
    }
    if let Some(expires_before) = filter.expires_before {
        builder.push(format!(
            " AND l.former_expires_key <> {MISSING_TIMESTAMP} AND l.former_expires_key < "
        ));
        builder.push_bind(expires_before);
    }
    if let Some((expiry, namespace, name, namehash)) = after {
        builder.push(format!(
            " AND (l.former_expires_key, l.namespace, ({NAME_KEY}), l.namehash) {comparison} ("
        ));
        builder.push_bind(*expiry);
        builder.push(", ");
        builder.push_bind(namespace.clone());
        builder.push(", CASE WHEN octet_length(");
        builder.push_bind(name.clone());
        builder.push("::text) <= 2000 THEN ");
        builder.push_bind(name.clone());
        builder.push("::text ELSE left(");
        builder.push_bind(name.clone());
        builder.push("::text, 500) END, ");
        builder.push_bind(namehash.clone());
        builder.push(")");
    }
    builder.push(format!(
        " ORDER BY l.former_expires_key {direction}, l.namespace {direction}, ({NAME_KEY}) \
         {direction}, l.namehash {direction} LIMIT "
    ));
    builder.push_bind(i64::try_from(size + 1).context("page size does not fit")?);
    Ok(builder)
}

/// The cursor's position in the index's key, its missing expiry as `MISSING_TIMESTAMP`.
fn cursor_key(cursor: &NameCurrentListCursor) -> Result<Position> {
    let NameCurrentListCursorValue::Timestamp(expiry) = cursor.sort_value else {
        anyhow::bail!("a former-owner cursor must carry an expiry position");
    };
    Ok((
        expiry.unwrap_or_else(missing),
        cursor.namespace.clone(),
        cursor.normalized_name.clone(),
        cursor.namehash.clone(),
    ))
}

fn missing() -> UnixSeconds {
    UnixSeconds::from_seconds(i128::from(MISSING_TIMESTAMP)).expect("the missing key fits")
}
