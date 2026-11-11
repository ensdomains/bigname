//! The stored event counts (`docs/projections.md`, "Event counts") as the count of the event read
//! they count. A read is counted from them only when every filter it carries is a column of the
//! count's key, the session can read the table, and on the read's own snapshot every chain's
//! family marker is servable at the block the read is bound to. Every other read counts its rows.

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter,
    count_contract::{EMITTER, POINTER_CLEARED, POINTER_SET},
};
use crate::families::name::{FamilyPublicationUnavailable, servable_publication};

/// The table the counts are read from, schema-qualified.
pub(super) const TABLE: &str = "bigname_phase.project_event_count";

const EMITTED: &[i16] = &[EMITTER];
const RESOLVER: &[i16] = &[EMITTER, POINTER_SET, POINTER_CLEARED];

/// The key rows whose sum is a read's count, and the block each chain's sum is bound to.
#[derive(Debug)]
pub(super) struct StoredCountKey<'a> {
    pub(super) chains: Vec<(&'a str, i64)>,
    pub(super) anchor_kinds: &'static [i16],
    pub(super) anchor: &'a str,
    pub(super) namespace: Option<&'a str>,
    pub(super) event_kinds: &'a [String],
}

/// The key a read's count is the sum of, or `None` when the read carries a filter the key does not
/// hold: a name, registration or address anchor, a record key, a block or timestamp window, a
/// resolver with a different `contract_address`, or no anchor at all. Each chain of the read's
/// window must end at that chain's published block and start nowhere, which is the window a read
/// without block or timestamp bounds gets.
pub(super) fn key(
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
) -> Option<StoredCountKey<'_>> {
    if !canonical_only
        || filter.diagnostics
        || !filter.selectors.is_empty()
        || filter.registration_id.is_some()
        || filter.record_key.is_some()
        || filter.from_block.is_some()
        || filter.to_block.is_some()
        || (filter.event_kinds.is_empty() && !filter.match_no_events)
    {
        return None;
    }
    let bounds = filter.publication_block_bounds.as_ref()?;
    let mut chains = Vec::new();
    for range in &filter.block_window.as_ref()?.ranges {
        let bound = *bounds.get(&range.chain_id)?;
        if range.from_block.is_some() || range.to_block != Some(bound) {
            return None;
        }
        chains.push((range.chain_id.as_str(), bound));
    }
    // The read binds both addresses as given, so a key is equal exactly when the read reduces to
    // one emitter.
    let (anchor_kinds, anchor) = match (&filter.resolver, filter.contract_address.as_deref()) {
        (None, Some(contract)) => (EMITTED, contract),
        (Some(resolver), None) => {
            chains.retain(|(chain, _)| *chain == resolver.chain_id);
            (RESOLVER, resolver.address.as_str())
        }
        (Some(resolver), Some(contract)) if contract == resolver.address => {
            chains.retain(|(chain, _)| *chain == resolver.chain_id);
            (EMITTED, contract)
        }
        _ => return None,
    };
    if chains.is_empty() {
        return None;
    }
    Some(StoredCountKey {
        chains,
        anchor_kinds,
        anchor,
        namespace: filter.namespace.as_deref(),
        event_kinds: if filter.match_no_events {
            &[]
        } else {
            &filter.event_kinds
        },
    })
}

/// Whether this session may read the stored counts. Without the privilege a read would fail
/// inside the page's transaction, so every caller checks it first and counts rows instead.
pub(super) async fn readable(connection: &mut PgConnection) -> Result<bool> {
    sqlx::query_scalar(
        "/* storage:history.stored_event_count_readable */
         SELECT COALESCE(has_table_privilege(to_regclass($1), 'SELECT'), FALSE)",
    )
    .bind(TABLE)
    .fetch_one(connection)
    .await
    .context("failed to check the privilege on the stored event counts")
}

/// Whether the API role can read the stored event counts. Without it every event count walks
/// the rows, exact up to its cap.
pub async fn stored_event_counts_readable(db: impl Into<crate::ReadDb<'_>>) -> Result<bool> {
    let mut connection = db.into().acquire().await?;
    readable(&mut connection).await
}

/// The read's count from the stored event counts, or `None` when the read must count its rows.
/// Runs on the read's own snapshot, so the count describes the publication its rows do.
pub(super) async fn load(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<Option<u64>> {
    // Red probe: the stored counts never serve.
    if filter.namespace.is_some() || filter.namespace.is_none() {
        return Ok(receipt("rows_filter", None));
    }
    let Some(key) = key(filter, canonical_only) else {
        return Ok(receipt("rows_filter", None));
    };
    if !readable(connection).await? {
        return Ok(receipt("rows_privilege", None));
    }
    for (chain, bound) in &key.chains {
        match servable_publication(connection, chain).await {
            Ok(publication) if publication.block_number == *bound => {}
            Ok(_) => return Ok(receipt("rows_publication", None)),
            Err(error) if error.is::<FamilyPublicationUnavailable>() => {
                return Ok(receipt("rows_publication", None));
            }
            Err(error) => return Err(error),
        }
    }
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_stored_count_query(&mut builder, &key);
    let count = builder
        .build_query_scalar::<i64>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to sum the stored event counts")?;
    let count = u64::try_from(count).context("negative stored event count")?;
    Ok(receipt("stored", Some(count)))
}

/// The sum of the key's rows. A key with no row sums to zero.
pub(super) fn push_stored_count_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    key: &StoredCountKey<'a>,
) {
    builder.push(
        "/* storage:history.stored_event_count */
         SELECT COALESCE(SUM(event_count), 0)::BIGINT FROM ",
    );
    builder.push(TABLE);
    builder.push(" WHERE chain_id = ANY(");
    builder.push_bind(
        key.chains
            .iter()
            .map(|(chain, _)| (*chain).to_owned())
            .collect::<Vec<_>>(),
    );
    builder.push(") AND anchor_kind = ANY(");
    builder.push_bind(key.anchor_kinds.to_vec());
    builder.push(") AND anchor = ");
    builder.push_bind(key.anchor);
    if let Some(namespace) = key.namespace {
        builder.push(" AND namespace = ");
        builder.push_bind(namespace);
    }
    builder.push(" AND event_kind = ANY(");
    builder.push_bind(key.event_kinds.to_vec());
    builder.push(")");
}

fn receipt(path: &'static str, count: Option<u64>) -> Option<u64> {
    tracing::debug!(path, count = ?count, "event history count");
    count
}
