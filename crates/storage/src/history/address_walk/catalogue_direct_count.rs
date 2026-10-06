//! Complete scalar counts for kinds whose eligibility is only name/resource membership.
//! The two event arms are disjoint; no complete identifier set crosses the SQL connection.

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{AddressRead, catalogue_source, seams, source};
use crate::history::{EventHistoryReadFilter, HistoryScope};

pub(super) async fn count(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    limit: Option<i64>,
) -> Result<i64> {
    let _timer = seams::Timer::new("catalogue_direct_count_micros");
    let mut query = QueryBuilder::<Postgres>::new("");
    push_query(&mut query, read, filter, limit);
    let count = query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(connection)
        .await
        .context("failed to count direct catalogue events")?;
    seams::count("catalogue_direct_count", 1);
    Ok(count)
}

pub(super) fn push_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    limit: Option<i64>,
) {
    catalogue_source::push_with(query, filter, None);
    query.push(" SELECT count(*) FROM (");
    if read.scope != HistoryScope::Resource {
        push_arm(query, read, filter, false, limit);
    }
    if read.scope == HistoryScope::Both {
        query.push(" UNION ALL ");
    }
    if read.scope != HistoryScope::Surface {
        push_arm(query, read, filter, true, limit);
    }
    if let Some(limit) = limit {
        query.push(" LIMIT ").push_bind(limit);
    }
    query.push(") direct_count");
}

fn push_arm<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    resource: bool,
    limit: Option<i64>,
) {
    query.push("SELECT 1 FROM bigname_phase.project_address_history_anchor anchor CROSS JOIN LATERAL (SELECT 1 FROM normalized_events ne");
    source::push_arm_filters(query, read, filter, None);
    query
        .push(" AND ")
        .push(crate::history::direct_accounts::ANCHORED_EVENT)
        .push(" AND ne.chain_id = anchor.chain_id AND ");
    query.push(if resource {
        "ne.resource_id = CASE WHEN anchor.anchor_kind = 1 THEN anchor.anchor_id::uuid END"
    } else {
        "ne.logical_name_id = anchor.anchor_id"
    });
    if resource && read.scope != HistoryScope::Resource {
        // An event with both anchors belongs to the NAME arm exactly once, irrespective of
        // which resource or relation supplied its other eligible witness.
        query.push(" AND NOT EXISTS (SELECT 1 FROM bigname_phase.project_address_history_anchor covered WHERE covered.chain_id = ne.chain_id AND covered.anchor_kind = 0 AND covered.anchor_id = ne.logical_name_id AND covered.address = ");
        query.push_bind(read.address);
        if let Some(namespace) = read.namespace {
            query.push(" AND covered.namespace = ").push_bind(namespace);
        }
        query
            .push(" AND ((covered.current_mask | covered.historical_mask) & ")
            .push_bind(catalogue_source::relation_mask(read))
            .push("::smallint) <> 0)");
    }
    if let Some(limit) = limit {
        query.push(" LIMIT ").push_bind(limit);
    }
    // Keep the event scan correlated to its exact anchor even when its estimated fanout is
    // large. The outer cap can stop this iterator; exact mode exhausts the same predicates.
    query.push(" OFFSET 0) event");
    catalogue_source::push_anchor_filter(query, read, filter, false);
    query
        .push(" AND anchor.anchor_kind = ")
        .push_bind(i16::from(resource));
}
