use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgConnection};

use super::DiscoveryObservationPageSource;
use crate::{ManifestRuntimeProgress, ManifestRuntimeProgressFuture};

pub(super) struct PageSourceManifestProgress<'a, Source>(&'a Source);

impl<'a, Source> PageSourceManifestProgress<'a, Source> {
    pub(super) fn new(source: &'a Source) -> Self {
        Self(source)
    }
}

impl<Source> ManifestRuntimeProgress for PageSourceManifestProgress<'_, Source>
where
    Source: DiscoveryObservationPageSource + Sync,
{
    fn record<'a>(&'a mut self, _pool: &'a PgPool) -> ManifestRuntimeProgressFuture<'a> {
        Box::pin(async move { self.0.record_progress().await })
    }
}

pub(super) async fn load_active_edge_summary_with_progress<Source>(
    executor: &mut PgConnection,
    discovery_source: &str,
    page_limit: i64,
    source: &Source,
) -> Result<(usize, BTreeSet<String>)>
where
    Source: DiscoveryObservationPageSource + Sync,
{
    let mut active_edge_count = 0usize;
    let mut chains = BTreeSet::new();
    let mut after_edge_id = 0i64;
    loop {
        let edge_ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT discovery_edge_id
            FROM discovery_edges
            WHERE discovery_edge_id > $1
              AND discovery_source = $2
              AND deactivated_at IS NULL
            ORDER BY discovery_edge_id
            LIMIT $3
            "#,
        )
        .bind(after_edge_id)
        .bind(discovery_source)
        .bind(page_limit.max(1))
        .fetch_all(&mut *executor)
        .await
        .context("failed to page discovery-edge identities for the active-edge summary")?;
        let Some(last_edge_id) = edge_ids.last().copied() else {
            break;
        };
        after_edge_id = last_edge_id;
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            SELECT chain_id
            FROM discovery_edges
            WHERE discovery_edge_id = ANY($1::BIGINT[])
              AND discovery_source = $2
              AND deactivated_at IS NULL
            ORDER BY discovery_edge_id
            "#,
        )
        .bind(&edge_ids)
        .bind(discovery_source)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load an active discovery-edge summary page")?;
        active_edge_count = active_edge_count
            .checked_add(rows.len())
            .context("active discovery-edge count overflowed usize")?;
        chains.extend(rows);
        source.record_progress().await?;
    }
    Ok((active_edge_count, chains))
}

#[cfg(test)]
#[path = "progress/tests.rs"]
mod tests;
