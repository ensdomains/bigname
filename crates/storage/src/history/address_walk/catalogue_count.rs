//! Complete direct-event counts and positive bounds for the unchanged capped count.
//! Failure to pass a lower-bound cap says nothing about the collection total.

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    AddressRead, catalogue_direct_count, catalogue_source, seams,
    source::{direct_only_kind, push_arm_filters},
};
use crate::history::{EventHistoryReadFilter, HistoryScope, HistorySummaryMode};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CountOutcome {
    Exact(u64),
    OverCap(u64),
    Unknown,
}

impl CountOutcome {
    pub(super) fn value(self) -> Option<u64> {
        match self {
            Self::Exact(count) | Self::OverCap(count) => Some(count),
            Self::Unknown => None,
        }
    }
}

pub(super) async fn count(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    summary: HistorySummaryMode,
) -> Result<CountOutcome> {
    if summary == HistorySummaryMode::None {
        return Ok(CountOutcome::Unknown);
    }
    if filter.match_no_events {
        return Ok(CountOutcome::Exact(0));
    }
    let cap = match summary {
        HistorySummaryMode::CappedCount(cap) => Some(cap),
        HistorySummaryMode::Count => None,
        _ => return Ok(CountOutcome::Unknown),
    };
    if !filter.event_kinds.is_empty()
        && filter.event_kinds.iter().all(|kind| direct_only_kind(kind))
    {
        let limit = cap
            .map(|cap| {
                i64::try_from(cap.checked_add(1).context("history count cap overflow")?)
                    .context("history count cap exceeds SQL limit")
            })
            .transpose()?;
        let count =
            u64::try_from(catalogue_direct_count::count(connection, read, filter, limit).await?)?;
        return Ok(if cap.is_some_and(|cap| count > cap) {
            CountOutcome::OverCap(count)
        } else {
            CountOutcome::Exact(count)
        });
    }
    let Some(cap) = cap else {
        return Ok(CountOutcome::Unknown);
    };
    if let Some(count) = prove_over_cap(connection, read, filter, cap).await? {
        return Ok(CountOutcome::OverCap(count));
    }
    Ok(CountOutcome::Unknown)
}

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
