//! Test support for the storage tests that write family rows by hand: the child listing and the
//! counts Project would store for them, composed by the same statement Project runs
//! (`listing_rows`) at each chain's family marker block, replacing whatever the chains stored.
use anyhow::{Context, Result, ensure};
use sqlx::PgConnection;

use super::{
    super::{LISTING_COLUMNS, ListingBlock, listing_rows},
    FamilyChildRow, FamilyChildrenPage, RegistryLabels, page,
    served_page::served_page,
};
use crate::{ChildrenCurrentKeysetCursor, ChildrenCurrentPageFilter};

/// Every parent of `$1` that can list a child: an edge's parent and a subregistry pointer's name.
const PARENTS: &str = "SELECT DISTINCT parent FROM (
        SELECT edge.namespace || ':' || edge.parent_node AS parent
        FROM bigname_phase.project_child_edge_candidate edge WHERE edge.chain_id = $1
        UNION SELECT pointer.logical_name_id FROM bigname_phase.project_parent_subregistry pointer
        WHERE pointer.chain_id = $1
    ) parents ORDER BY parent";

const COUNTS: &str = "INSERT INTO bigname_phase.project_child_listing_count
        (chain_id, parent_logical_name_id, registry_address, child_count)
    SELECT $1, listing.parent_logical_name_id, '', count(*)
    FROM bigname_phase.project_child_listing listing WHERE listing.chain_id = $1
    GROUP BY listing.parent_logical_name_id
    UNION ALL
    SELECT $1, listing.parent_logical_name_id, listing.registry_address, count(*)
    FROM bigname_phase.project_child_listing listing
    WHERE listing.chain_id = $1 AND listing.registry_address IS NOT NULL
    GROUP BY listing.parent_logical_name_id, listing.registry_address";

/// Store the child listing and counts of every chain with a family marker block, as Project
/// would at that block from the family rows the test wrote.
pub(crate) async fn compose_listing(conn: &mut PgConnection) -> Result<()> {
    let chains: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT chain_id, current_block_number, extract(epoch FROM block_timestamp)::bigint
         FROM bigname_phase.project_family_marker
         WHERE current_block_number IS NOT NULL AND block_timestamp IS NOT NULL
         ORDER BY chain_id",
    )
    .fetch_all(&mut *conn)
    .await
    .context("failed to read the marker blocks")?;
    for (chain, number, timestamp_seconds) in chains {
        for table in ["project_child_listing", "project_child_listing_count"] {
            sqlx::query(&format!(
                "DELETE FROM bigname_phase.{table} WHERE chain_id = $1"
            ))
            .bind(&chain)
            .execute(&mut *conn)
            .await?;
        }
        let parents: Vec<String> = sqlx::query_scalar(PARENTS)
            .bind(&chain)
            .fetch_all(&mut *conn)
            .await?;
        let head = format!("INSERT INTO bigname_phase.project_child_listing ({LISTING_COLUMNS})");
        let block = ListingBlock {
            chain_id: &chain,
            number,
            timestamp_seconds,
        };
        listing_rows(&head, block, &parents, None)
            .build()
            .execute(&mut *conn)
            .await
            .with_context(|| format!("failed to compose the child listing of {chain}"))?;
        sqlx::query(COUNTS).bind(&chain).execute(&mut *conn).await?;
    }
    Ok(())
}

/// The stored page after [`compose_listing`], checked against the page the served relation
/// composes at read: the same total and, for a page that holds every child the filters admit,
/// the same rows.
pub(crate) async fn checked_page(
    conn: &mut PgConnection,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    registry: Option<RegistryLabels<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<FamilyChildrenPage> {
    compose_listing(conn).await?;
    let stored = page(
        conn,
        parent_logical_name_id,
        filter,
        registry,
        cursor,
        page_size,
        None,
    )
    .await?;
    let served = served_page(
        conn,
        parent_logical_name_id,
        filter,
        registry,
        cursor,
        page_size,
    )
    .await?;
    ensure!(
        stored.total_count == served.total_count,
        "{parent_logical_name_id}: the stored total is {}, the relation's {}",
        stored.total_count,
        served.total_count
    );
    if cursor.is_none() && stored.next_cursor.is_none() {
        let by_child = |rows: &[FamilyChildRow]| {
            let mut rows = rows.to_vec();
            rows.sort_by(|left, right| {
                left.child_logical_name_id.cmp(&right.child_logical_name_id)
            });
            rows
        };
        ensure!(
            by_child(&stored.rows) == by_child(&served.rows),
            "{parent_logical_name_id}: the stored rows {:?} are not the relation's {:?}",
            stored.rows,
            served.rows
        );
    }
    Ok(stored)
}
