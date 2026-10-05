//! A positive lower bound for the unchanged capped count. Distinct historical names own
//! disjoint direct events; failure to pass the cap says nothing about the collection total.

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{AddressRead, catalogue_source, seams, source::push_arm_filters};
use crate::history::{EventHistoryReadFilter, HistoryScope};

pub(super) async fn prove_over_cap(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    cap: u64,
) -> Result<Option<u64>> {
    let _timer = seams::Timer::new("catalogue_count_proof_micros");
    if read.scope == HistoryScope::Resource || filter.match_no_events {
        return Ok(None);
    }
    let limit = i64::try_from(cap.checked_add(1).context("history count cap overflow")?)?;
    let mut count = 0_i64;
    let mut after: Option<String> = None;
    loop {
        let mut names = QueryBuilder::<Postgres>::new("");
        push_names_query(&mut names, read, filter, after.as_deref());
        let names: Vec<String> = names
            .build_query_scalar()
            .persistent(false)
            .fetch_all(&mut *connection)
            .await
            .context("failed to walk qualified historical names for the count proof")?;
        let _live = seams::Live::new("catalogue_proof_names", names.len());
        seams::count("catalogue_proof_names_visited", names.len());
        let Some(last) = names.last() else {
            return Ok(None);
        };
        after = Some(last.clone());
        let remaining = limit - count;
        let mut proof = QueryBuilder::<Postgres>::new("");
        push_proof_query(&mut proof, read, filter, &names, remaining);
        let witnessed: i64 = proof
            .build_query_scalar()
            .persistent(false)
            .fetch_one(&mut *connection)
            .await
            .context("failed to count qualified direct events for the capped-count proof")?;
        count += witnessed;
        seams::count("catalogue_proof_events", witnessed as usize);
        seams::count("catalogue_proof_batches", 1);
        if count >= limit {
            seams::count("catalogue_count_proved", 1);
            return Ok(Some(count as u64));
        }
        if names.len() < seams::batch_size() {
            return Ok(None);
        }
    }
}

pub(super) fn push_names_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    after: Option<&'a str>,
) {
    catalogue_source::push_with(query, filter, None);
    query.push(" SELECT DISTINCT anchor.anchor_id FROM bigname_phase.project_address_history_anchor anchor");
    catalogue_source::push_anchor_filter(query, read, filter, true);
    if let Some(after) = after {
        query.push(" AND anchor.anchor_id > ").push_bind(after);
    }
    query
        .push(" ORDER BY anchor.anchor_id LIMIT ")
        .push_bind(seams::batch_size() as i64);
}

pub(super) fn push_proof_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    names: &'a [String],
    remaining: i64,
) {
    query.push("SELECT count(*) FROM (SELECT 1 FROM unnest(");
    query.push_bind(names).push(
        "::text[]) name(logical_name_id) CROSS JOIN LATERAL (SELECT 1 FROM normalized_events ne",
    );
    push_arm_filters(query, read, filter, None);
    query.push(" AND ne.logical_name_id = name.logical_name_id AND ne.event_kind <> 'RootPermissionChanged' AND ne.event_identity NOT LIKE '%:ResolverChanged:registry-fallback-handoff:%' LIMIT ").push_bind(remaining);
    query
        .push(") event LIMIT ")
        .push_bind(remaining)
        .push(") proof");
}
