//! Ordered prefixes of the shared Project sources. SQL retains overlapping sources and every
//! witness of the selected event keys; Rust never allocates an address-wide set of heads.

use sqlx::{Postgres, QueryBuilder};

use super::{
    AddressRead,
    source::{EVENT_COLUMNS, push_arm_filters},
};
use crate::history::{
    EventHistoryReadFilter, HistoryOrder, HistoryScope, catalogue_contract as contract,
    keyset::{HistoryKeyset, push_history_cursor_cte},
    paging::push_history_order_terms,
};

#[derive(Clone, Copy)]
pub(super) enum Bucket {
    At(i64),
    After(i64),
    Any,
}

pub(super) fn relation_mask(read: &AddressRead<'_>) -> i16 {
    read.relations
        .filter(|relations| !relations.is_empty())
        .map_or(7, |relations| {
            relations.iter().fold(0, |mask, relation| {
                mask | contract::relation_mask(relation.as_str())
            })
        })
}

pub(super) fn push_with<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
) {
    if let Some(keyset) = keyset {
        push_history_cursor_cte(query, keyset.cursor);
        query.push(", ");
    } else {
        query.push("WITH ");
    }
    query.push("catalogue_filter AS (SELECT ");
    query.push_bind(
        filter
            .event_kinds
            .iter()
            .fold(0_i64, |mask, kind| mask | contract::event_kind_mask(kind)),
    );
    query.push("::bigint AS event_mask, ");
    query.push(contract::record_key_bloom_sql("filter_key"));
    query
        .push(" AS key_bloom FROM (SELECT ")
        .push_bind(filter.record_key.as_deref());
    query.push("::text AS filter_key) input)");
}

pub(super) fn push_summary_filter<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    alias: &str,
    filter: &EventHistoryReadFilter,
) {
    query.push(format!(" AND {alias}.first_bucket IS NOT NULL"));
    if filter.match_no_events
        || filter
            .block_window
            .as_ref()
            .is_some_and(|window| window.ranges.is_empty())
    {
        query.push(" AND FALSE");
    }
    if !filter.event_kinds.is_empty() {
        query.push(format!(
            " AND ({alias}.event_mask & (SELECT event_mask FROM catalogue_filter)) <> 0"
        ));
    }
    if filter.record_key.is_some() {
        query.push(format!(" AND ({alias}.key_bloom & (SELECT key_bloom FROM catalogue_filter)) = (SELECT key_bloom FROM catalogue_filter)"));
    }
    let (minimum, maximum) = bucket_window(filter);
    if let Some(minimum) = minimum {
        query
            .push(format!(" AND {alias}.last_bucket >= "))
            .push_bind(minimum);
    }
    if let Some(maximum) = maximum {
        query
            .push(format!(" AND {alias}.first_bucket <= "))
            .push_bind(maximum);
    }
}

fn bucket_window(filter: &EventHistoryReadFilter) -> (Option<i64>, Option<i64>) {
    let window = filter.block_window.as_ref();
    let from = window.and_then(|window| {
        window
            .ranges
            .iter()
            .map(|range| range.from_block.unwrap_or(0))
            .min()
    });
    let to = window
        .filter(|window| window.ranges.iter().all(|range| range.to_block.is_some()))
        .and_then(|window| {
            window
                .ranges
                .iter()
                .filter_map(|range| range.to_block)
                .max()
        });
    let minimum = from
        .into_iter()
        .chain(filter.from_block)
        .max()
        .map(|block| block / contract::BUCKET_BLOCKS);
    let maximum = to
        .into_iter()
        .chain(filter.to_block)
        .min()
        .map(|block| block / contract::BUCKET_BLOCKS);
    (minimum, maximum)
}

pub(super) fn push_anchor_filter<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &EventHistoryReadFilter,
    historical_names: bool,
) {
    query
        .push(" WHERE anchor.address = ")
        .push_bind(read.address);
    if let Some(namespace) = read.namespace {
        query.push(" AND anchor.namespace = ").push_bind(namespace);
    }
    query.push(if historical_names {
        " AND anchor.anchor_kind = 0 AND anchor.historical_mask <> 0 AND (anchor.historical_mask & "
    } else {
        " AND ((anchor.current_mask | anchor.historical_mask) & "
    });
    query
        .push_bind(relation_mask(read))
        .push("::smallint) <> 0");
    if !historical_names {
        match read.scope {
            HistoryScope::Surface => {
                query.push(" AND anchor.anchor_kind = 0");
            }
            HistoryScope::Resource => {
                query.push(" AND anchor.anchor_kind = 1");
            }
            HistoryScope::Both => {}
        }
    }
    push_summary_filter(query, "anchor", filter);
}

fn push_sources<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &EventHistoryReadFilter,
    bucket: i64,
    overlap_sources: bool,
) {
    query.push(", catalogue_anchors AS MATERIALIZED (SELECT anchor.chain_id, anchor.anchor_kind, anchor.anchor_id FROM bigname_phase.project_address_history_anchor anchor");
    push_anchor_filter(query, read, filter, false);
    query
        .push(" AND anchor.bucket_range @> ")
        .push_bind(bucket)
        .push("::bigint)");
    query.push(", catalogue_sources AS MATERIALIZED (");
    // The complete PK admits at most one direct source for each anchor. Keep that lookup
    // correlated so a high bucket estimate cannot turn it into a whole-source hash join.
    query.push("SELECT DISTINCT source.chain_id, source.source_kind, source.source_key, source.resolver_address, 0::integer AS witness_kind, NULL::uuid AS witness_resource FROM catalogue_anchors anchor CROSS JOIN LATERAL (SELECT source.chain_id, source.source_kind, source.source_key, source.resolver_address FROM bigname_phase.project_history_source source WHERE source.chain_id = anchor.chain_id AND source.source_kind = anchor.anchor_kind AND source.source_key = anchor.anchor_id AND source.resolver_address = ''");
    push_summary_filter(query, "source", filter);
    if overlap_sources {
        query
            .push(" AND source.bucket_range @> ")
            .push_bind(bucket)
            .push("::bigint");
    }
    query.push(" OFFSET 0) source");
    // Edges are discovery only. The exact pair validator still decides whether a write/link
    // belongs to the resource, including records written before the pointer or selected link.
    for link in [false, true] {
        query.push(" UNION SELECT DISTINCT edge.chain_id, ");
        query.push(if link {
            "4::smallint, edge.link_event_identity, ''::text"
        } else {
            "edge.source_kind, edge.source_key, edge.source_resolver"
        });
        query.push(", 3::integer, edge.resource_id FROM catalogue_anchors anchor JOIN bigname_phase.project_history_source_edge edge ON anchor.anchor_kind = 1 AND edge.chain_id = anchor.chain_id AND edge.resource_id = CASE WHEN anchor.anchor_kind = 1 THEN anchor.anchor_id::uuid END WHERE TRUE");
        push_summary_filter(query, "edge", filter);
        if overlap_sources {
            query
                .push(" AND edge.bucket_range @> ")
                .push_bind(bucket)
                .push("::bigint");
        }
        if link {
            query.push(" AND edge.link_event_identity <> ''");
        }
    }
    query.push(")");
}

fn root_enabled(read: &AddressRead<'_>) -> bool {
    read.scope != HistoryScope::Surface && relation_mask(read) & contract::ROLE_HOLDER != 0
}

fn push_bucket(query: &mut QueryBuilder<'_, Postgres>, bucket: Bucket, order: HistoryOrder) {
    match bucket {
        Bucket::At(contract::NULL_BUCKET) => {
            query.push(" AND ne.block_number IS NULL");
        }
        Bucket::At(bucket) => {
            query
                .push(" AND ne.block_number >= ")
                .push_bind(bucket.saturating_mul(contract::BUCKET_BLOCKS));
            query.push(" AND ne.block_number <= ").push_bind(
                bucket
                    .saturating_mul(contract::BUCKET_BLOCKS)
                    .saturating_add(contract::BUCKET_BLOCKS - 1),
            );
        }
        Bucket::After(bucket) => match order {
            HistoryOrder::Asc if bucket == contract::NULL_BUCKET => {
                query.push(" AND ne.block_number IS NOT NULL");
            }
            HistoryOrder::Asc => {
                query.push(" AND ne.block_number > ").push_bind(
                    bucket
                        .saturating_mul(contract::BUCKET_BLOCKS)
                        .saturating_add(contract::BUCKET_BLOCKS - 1),
                );
            }
            HistoryOrder::Desc if bucket == contract::NULL_BUCKET => {
                query.push(" AND FALSE");
            }
            HistoryOrder::Desc => {
                query
                    .push(" AND (ne.block_number IS NULL OR ne.block_number < ")
                    .push_bind(bucket.saturating_mul(contract::BUCKET_BLOCKS))
                    .push(")");
            }
        },
        Bucket::Any => {}
    }
}

fn push_root<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    bucket: Bucket,
    identity: Option<&'a str>,
    limit: i64,
) {
    query.push(format!("SELECT ne.*, 0::integer AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, NULL::uuid AS witness_resource FROM (SELECT {EVENT_COLUMNS} FROM normalized_events ne"));
    push_arm_filters(query, read, filter, keyset);
    query.push(" AND ne.event_kind = 'RootPermissionChanged' AND lower(ne.after_state ->> 'subject') = ").push_bind(read.address);
    if let Some(namespace) = read.namespace {
        query.push(" AND ne.namespace = ").push_bind(namespace);
    }
    push_probe_tail(query, filter, bucket, identity, limit);
    query.push(") ne");
}

fn push_probe_tail<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    filter: &EventHistoryReadFilter,
    bucket: Bucket,
    identity: Option<&'a str>,
    limit: i64,
) {
    push_bucket(query, bucket, filter.order);
    if let Some(identity) = identity {
        query.push(" AND ne.event_identity = ").push_bind(identity);
    }
    query.push(" ORDER BY ");
    push_history_order_terms(query, filter.order);
    query.push(" LIMIT ").push_bind(limit);
}

fn push_prefixes<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    bucket: Bucket,
    identity: Option<&'a str>,
    limit: i64,
) {
    query.push(", catalogue_prefixes AS MATERIALIZED (");
    for (kind, predicate) in [
        (0, "ne.logical_name_id = source.source_key"),
        (
            1,
            "ne.resource_id = CASE WHEN source.source_kind = 1 THEN source.source_key::uuid END",
        ),
        (
            2,
            "lower(ne.after_state ->> 'node') = source.source_key AND ne.logical_name_id IS NULL AND ne.after_state ->> 'node' IS NOT NULL AND ne.event_kind IN ('RecordChanged', 'RecordVersionChanged') AND ne.source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver')",
        ),
        (
            3,
            "lower(ne.after_state ->> 'resolver') = source.resolver_address AND ne.after_state ->> 'resolver_record_id' = source.source_key AND ne.event_kind = 'RecordChanged' AND ne.after_state ->> 'storage_model' = 'resolver_record_id'",
        ),
        (4, "ne.event_identity = source.source_key"),
    ] {
        if kind > 0 {
            query.push(" UNION ALL ");
        }
        query.push(format!("SELECT ne.*, source.witness_kind, NULL::text AS current_chain, NULL::text AS current_name, source.witness_resource FROM catalogue_sources source CROSS JOIN LATERAL (SELECT {EVENT_COLUMNS} FROM normalized_events ne"));
        push_arm_filters(query, read, filter, keyset);
        query.push(" AND ne.event_kind <> 'RootPermissionChanged' AND ne.chain_id = source.chain_id AND ").push(predicate);
        push_probe_tail(query, filter, bucket, identity, limit);
        query
            .push(") ne WHERE source.source_kind = ")
            .push_bind(kind as i16);
    }
    if root_enabled(read) {
        query.push(" UNION ALL ");
        push_root(query, read, filter, keyset, bucket, identity, limit);
    }
    query.push(")");
}

pub(super) fn push_candidate_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    bucket: i64,
    identity: Option<&'a str>,
    limit: i64,
) {
    push_with(query, filter, keyset);
    push_sources(query, read, filter, bucket, true);
    push_prefixes(
        query,
        read,
        filter,
        keyset,
        Bucket::At(bucket),
        identity,
        limit,
    );
    query.push(", catalogue_events AS MATERIALIZED (SELECT DISTINCT ne.normalized_event_id, ne.event_identity, ne.chain_id, ne.block_number, ne.block_hash, ne.transaction_index, ne.log_index FROM catalogue_prefixes ne ORDER BY ");
    push_history_order_terms(query, filter.order);
    query.push(" LIMIT ").push_bind(limit).push(") SELECT DISTINCT ne.* FROM catalogue_prefixes ne JOIN catalogue_events event USING (normalized_event_id) ORDER BY ");
    push_history_order_terms(query, filter.order);
    query.push(", ne.witness_kind, ne.current_chain, ne.current_name, ne.witness_resource, ne.normalized_event_id, ne.node");
}

/// Seek an anchor boundary or the next actual event of a currently overlapping source. A
/// long-lived envelope with no writes for millions of buckets takes one ordered source seek.
pub(super) fn push_seek_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    after: Option<i64>,
) {
    push_with(query, filter, None);
    if let Some(bucket) = after {
        push_sources(query, read, filter, bucket, false);
        push_prefixes(query, read, filter, None, Bucket::After(bucket), None, 1);
    }
    let (bound, direction, comparison, aggregate) = match filter.order {
        HistoryOrder::Asc => ("first_bucket", "ASC", ">", "min"),
        HistoryOrder::Desc => ("last_bucket", "DESC", "<", "max"),
    };
    query.push(", catalogue_bound AS (SELECT ");
    let (minimum, maximum) = bucket_window(filter);
    let clamp = match filter.order {
        HistoryOrder::Asc => minimum,
        HistoryOrder::Desc => maximum,
    };
    if let Some(clamp) = clamp {
        query.push(if filter.order == HistoryOrder::Asc {
            "greatest("
        } else {
            "least("
        });
        query
            .push(format!("anchor.{bound}, "))
            .push_bind(clamp)
            .push(")");
    } else {
        query.push(format!("anchor.{bound}"));
    }
    query.push(" AS bucket FROM bigname_phase.project_address_history_anchor anchor");
    push_anchor_filter(query, read, filter, false);
    if let Some(after) = after {
        query
            .push(format!(" AND anchor.{bound} {comparison} "))
            .push_bind(after);
    }
    query.push(format!(" ORDER BY anchor.{bound} {direction} LIMIT 1) SELECT {aggregate}(bucket) FROM (SELECT bucket FROM catalogue_bound"));
    if after.is_some() {
        query.push(" UNION ALL SELECT COALESCE(block_number / 256, -1) FROM catalogue_prefixes");
    } else if root_enabled(read) {
        query.push(" UNION ALL SELECT COALESCE(block_number / 256, -1) FROM (");
        push_root(query, read, filter, None, Bucket::Any, None, 1);
        query.push(") root");
    }
    query.push(") buckets");
}

/// The winner universe is every qualified peer in a handoff group, without a bucket, page or
/// continuation boundary. Catalogue membership is exact; handoffs themselves are direct events.
pub(super) fn push_handoff_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    groups: &'a serde_json::Value,
) {
    push_with(query, filter, None);
    query
        .push(", peer_events AS MATERIALIZED (SELECT ne.* FROM jsonb_to_recordset(")
        .push_bind(groups);
    query.push("::jsonb) AS peer(chain text, block bigint, hash text, node text, origin text) CROSS JOIN LATERAL (SELECT ne.* FROM normalized_events ne");
    push_arm_filters(query, read, filter, None);
    query.push(" AND ne.event_kind = 'ResolverChanged' AND ne.chain_id = peer.chain AND ne.block_number = peer.block AND ne.block_hash IS NOT DISTINCT FROM peer.hash AND ne.after_state ->> 'node' IS NOT DISTINCT FROM peer.node AND strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0 AND split_part(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:', 1) = peer.origin OFFSET 0) ne) SELECT ");
    query.push(EVENT_COLUMNS);
    query.push(", 0::integer AS witness_kind, NULL::text AS current_chain, NULL::text AS current_name, NULL::uuid AS witness_resource FROM peer_events ne WHERE EXISTS (SELECT 1 FROM bigname_phase.project_address_history_anchor anchor");
    push_anchor_filter(query, read, filter, false);
    query.push(" AND anchor.chain_id = ne.chain_id AND ((anchor.anchor_kind = 0 AND anchor.anchor_id = ne.logical_name_id) OR (anchor.anchor_kind = 1 AND anchor.anchor_id = ne.resource_id::text))) ORDER BY ne.event_identity");
}
