use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::PgPool;

use super::{
    source_decode::decode_declared_child_event_source,
    sources::{canonical_declared_child_sources_query, declared_child_sources_context},
    types::DeclaredChildEventSource,
};

/// Stream the latest canonical declared-child subregistry event per child surface.
pub fn stream_canonical_declared_child_sources<'a>(
    pool: &'a PgPool,
    parent_logical_name_id: Option<&'a str>,
) -> impl Stream<Item = Result<DeclaredChildEventSource>> + 'a {
    let context = declared_child_sources_context(parent_logical_name_id);
    canonical_declared_child_sources_query(parent_logical_name_id, None, None)
        .fetch(pool)
        .map(move |row| {
            row.with_context(|| context.clone())
                .and_then(decode_declared_child_event_source)
        })
}

/// Stream the latest canonical declared-child sources after one stable source key.
pub fn stream_canonical_declared_child_sources_after<'a>(
    pool: &'a PgPool,
    after_source_key: Option<(&'a str, &'a str, &'a str)>,
    limit: i64,
) -> impl Stream<Item = Result<DeclaredChildEventSource>> + 'a {
    let context = declared_child_sources_context(None);
    canonical_declared_child_sources_query(None, after_source_key, Some(limit))
        .fetch(pool)
        .map(move |row| {
            row.with_context(|| context.clone())
                .and_then(decode_declared_child_event_source)
        })
}
