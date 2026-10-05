use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    AddressRead, Witness, duplicates,
    matches::Membership,
    seams::{self, Live},
    source,
};
use crate::history::{EventHistoryReadFilter, keyset::HistoryKeyset};

pub(super) struct Accumulator {
    pub(super) ids: Vec<i64>,
    pub(super) count: u64,
    page_limit: usize,
    count_limit: Option<u64>,
    live: Live,
}
impl Accumulator {
    pub(super) fn new(page_limit: usize, count_limit: Option<u64>) -> Self {
        Self {
            ids: Vec::with_capacity(page_limit),
            count: 0,
            page_limit,
            count_limit,
            live: Live::new("retained_page_ids", 0),
        }
    }
    fn accept(&mut self, id: i64) {
        if self.ids.len() < self.page_limit {
            self.ids.push(id);
            self.live.set(self.ids.len());
        }
        if self.count_limit.is_none_or(|limit| self.count < limit) {
            self.count += 1;
        }
    }
    fn complete(&self) -> bool {
        self.ids.len() >= self.page_limit
            && self.count_limit.is_some_and(|limit| self.count >= limit)
    }
}

/// Stream fixed witness batches. One scalar carries an event split across FETCH boundaries;
/// the full address and the already visited event identities are never retained by the API.
pub(super) async fn collect(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'_>>,
    identity: Option<&str>,
    membership: &mut Membership,
    output: &mut Accumulator,
) -> Result<()> {
    let mut query =
        QueryBuilder::<Postgres>::new("DECLARE address_history_candidates NO SCROLL CURSOR FOR ");
    if identity.is_some() {
        query.push("SELECT * FROM (");
    }
    source::push_candidate_query(&mut query, read, filter, keyset);
    if let Some(identity) = identity {
        query
            .push(") candidate WHERE event_identity = ")
            .push_bind(identity);
    }
    query
        .build()
        .persistent(false)
        .execute(&mut *connection)
        .await
        .context("failed to open address-history candidate cursor")?;
    let mut pending: Option<(i64, bool)> = None;
    let mut finished = false;
    while !finished {
        let rows = sqlx::query_as::<_, Witness>(&format!(
            "FETCH FORWARD {} FROM address_history_candidates",
            seams::batch_size()
        ))
        .fetch_all(&mut *connection)
        .await
        .context("failed to fetch address-history witnesses")?;
        let _live = Live::new("witness_rows", rows.len());
        seams::count("witness_batches", 1);
        seams::count("witness_rows_scanned", rows.len());
        let terminal = rows.len() < seams::batch_size();
        let mut matched = membership
            .validate(
                connection,
                read,
                &rows,
                pending.filter(|(_, valid)| *valid).map(|(id, _)| id),
            )
            .await?;
        let _matched_live = Live::new("validated_witnesses", matched.len());
        duplicates::retain_winners(connection, read, filter, membership, &rows, &mut matched)
            .await?;
        for (row, valid) in rows.into_iter().zip(matched) {
            if pending.is_some_and(|(id, _)| id != row.normalized_event_id) {
                if let Some((id, true)) = pending.take() {
                    output.accept(id);
                }
                if output.complete() {
                    finished = true;
                    break;
                }
            }
            let group = pending.get_or_insert((row.normalized_event_id, false));
            group.1 |= valid;
        }
        if terminal {
            if let Some((id, true)) = pending.take() {
                output.accept(id);
            }
            finished = true;
        }
    }
    sqlx::query("CLOSE address_history_candidates")
        .execute(connection)
        .await?;
    Ok(())
}
