//! `GET /v1/addresses/{address}/names` on the ownership relations over the stored address
//! listing (`project_address_listing`, written by Project from the relation fold the route
//! composed at read before, `families/records/address_listing.rs`): a page is a keyset walk of
//! one of the listing's sort indexes for the address, and only the page's names are loaded
//! afterwards. The page, its total and the registry children it reloads read one snapshot.
//!
//! A page lists the rows of the chains whose publication the read holds. Those are the chains
//! with a family marker on which the address has a row in the request's namespace, and none when
//! it has no such row (`held_publications`). A row the read filter would hide (its surface, the surface's
//! block or its resource no longer canonical) stays out of the page, as does a row whose surface
//! the compositor no longer reads (shadowed since the publication); a registry child stands on
//! its publication alone. The total is the stored count and applies no such fence.
//!
//! Rows of one name on two chains are one entry, their relations merged. Under
//! `dedupe=registration` the entry of a resource is its first row in name order among the rows
//! the filters keep, with the relations of all of them.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgConnection;

mod anchors;
mod entries;
mod former;
mod statement;

pub(crate) use anchors::load_stored_current_anchors;
#[cfg(any(test, feature = "test-support"))]
pub use anchors::{load_composed_address_anchors, load_stored_address_anchors};
pub use former::load_address_former_owner_page;

use super::{AddressNamesPageRequest, load_family_address_names_capped_page};
use crate::{
    AddressNamesCurrentCappedPage,
    families::{
        name::{FamilyPublication, servable_publication},
        records::seams,
    },
};

/// The page of `request`. Its total is exact when `exact_total` asks for it, and otherwise
/// when the address lists at most `seams::exact_total_cap` entries under the request's filters;
/// above that it is null.
pub async fn load_address_names_listing_page(
    db: impl Into<crate::ReadDb<'_>>,
    request: &AddressNamesPageRequest<'_>,
    exact_total: bool,
) -> Result<AddressNamesCurrentCappedPage> {
    let mut snapshot = db.into().snapshot().await?;
    let page = if seams::composed_address_names() {
        load_family_address_names_capped_page(&mut *snapshot, request, exact_total).await?
    } else {
        seams::note_address_name_path("listing");
        listing_page(&mut snapshot, request, exact_total).await?
    };
    snapshot.close().await?;
    Ok(page)
}

async fn listing_page(
    conn: &mut PgConnection,
    request: &AddressNamesPageRequest<'_>,
    exact_total: bool,
) -> Result<AddressNamesCurrentCappedPage> {
    let address = request.address.to_ascii_lowercase();
    let publications = held_publications(conn, &address, request.namespace).await?;
    let chains: Vec<String> = publications.keys().cloned().collect();
    let read = statement::Read::new(request, &address, &chains);
    let page_size = usize::try_from(request.page_size).context("page_size does not fit")?;
    let limit = page_size + 1;
    let mut groups: Vec<Vec<statement::Row>> = Vec::new();
    let mut after = read.request_position()?;
    loop {
        let rows = read.rows(conn, after.as_ref(), limit).await?;
        let fetched = rows.len();
        after = rows.last().map(|row| read.position(row));
        for row in rows {
            match groups.last_mut() {
                Some(group) if read.merges(&group[0], &row) => group.push(row),
                _ => groups.push(vec![row]),
            }
        }
        // The last group may continue in the rows after the limit; it is complete once a
        // later group has begun.
        if fetched < limit || groups.len() > page_size {
            break;
        }
    }
    seams::after_listing_page().await;
    let total_count = read.total(conn, exact_total).await?;
    let more = groups.len() > page_size;
    groups.truncate(page_size);
    let next_cursor = more
        .then(|| groups.last())
        .flatten()
        .map(|group| read.cursor(entries::representative(group)));
    let entries = entries::entries(conn, &read, &publications, &groups).await?;
    Ok(AddressNamesCurrentCappedPage {
        entries,
        next_cursor,
        total_count,
    })
}

/// The publications a read of `address` holds: those of the chains with a family marker on which
/// the address has a stored row in `namespace` (any namespace when it is `None`), one probe a
/// marker, refusing a chain that is not servable. With no such row no chain is checked and the
/// set is empty: the route's own namespace admission stands, and a marker of another chain
/// cannot refuse the read. The page lists only these chains' rows.
///
/// The probe reads every row of the address, listed or not, so a former-only row makes the
/// ownership read check its chain too, which only adds a refusal while that chain rebuilds.
///
/// A chain with no marker holds no stored row: Project writes the marker in the transaction that
/// writes a chain's first rows, and a schema-migration that deletes a chain's marker must delete
/// that chain's listing rows in the same transaction. The total is not held to these chains,
/// because no listing index carries the chain and the filter would cost the count a heap read
/// per row it counts.
pub(super) async fn held_publications(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
) -> Result<BTreeMap<String, FamilyPublication>> {
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:address_names.listing_chains */
         SELECT marker.chain_id FROM bigname_phase.project_family_marker marker
         WHERE EXISTS (SELECT 1 FROM bigname_phase.project_address_listing l
                       WHERE l.chain_id = marker.chain_id AND l.address = $1
                         AND ($2::text IS NULL OR l.namespace = $2))
         ORDER BY marker.chain_id",
    )
    .bind(address)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to find the chains {address} has listed names on"))?;
    let mut publications = BTreeMap::new();
    for chain_id in chains {
        let publication = servable_publication(conn, &chain_id).await?;
        publications.insert(chain_id, publication);
    }
    Ok(publications)
}

/// The API's handlers need these futures `Send`. A read that breaks it fails to compile here, at
/// the value held across an await.
#[cfg(test)]
#[test]
fn the_listing_reads_are_send() {
    fn send<F: std::future::Future + Send>(_: F) {}
    fn reads(
        pool: &sqlx::PgPool,
        conn: &mut PgConnection,
        request: &AddressNamesPageRequest<'_>,
        filter: &crate::families::records::FormerOwnerFilter<'_>,
        published: &BTreeMap<String, i64>,
    ) {
        send(load_stored_current_anchors(pool, "", None, None, published));
        send(load_address_names_listing_page(pool, request, false));
        send(listing_page(conn, request, false));
        send(load_address_former_owner_page(
            pool,
            filter,
            crate::NameCurrentListOrder::Asc,
            None,
            1,
        ));
        send(statement::explain(conn, "send", || unreachable!()));
    }
    let _ = reads;
}
