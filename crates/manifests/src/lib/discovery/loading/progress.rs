use std::{collections::HashMap, future::Future, pin::Pin};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgConnection, postgres::PgRow, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{
    ManifestRuntimeProgress, PROPAGATED_ROLE_PROVENANCE_FIELD, REACHABLE_FROM_ROOT_ADMISSION,
    normalize_address,
};

use super::super::provenance::TRANSITIVE_DISCOVERY_EDGE_KIND;

const ADMISSION_LOAD_PAGE_SIZE: i64 = 1_000;
pub(super) const ADMISSION_LOAD_ROWS: usize = ADMISSION_LOAD_PAGE_SIZE as usize;

pub(super) type AdmissionStateProgressFuture<'a> =
    Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub(super) trait AdmissionStateProgress: Send {
    fn record(&mut self) -> AdmissionStateProgressFuture<'_>;
}

pub(super) struct AdmissionLoadProgress<'a> {
    pool: &'a PgPool,
    callback: &'a mut dyn ManifestRuntimeProgress,
}

impl<'a> AdmissionLoadProgress<'a> {
    pub(super) fn new(pool: &'a PgPool, callback: &'a mut dyn ManifestRuntimeProgress) -> Self {
        Self { pool, callback }
    }
}

impl AdmissionStateProgress for AdmissionLoadProgress<'_> {
    fn record(&mut self) -> AdmissionStateProgressFuture<'_> {
        Box::pin(self.callback.record(self.pool))
    }
}

pub(super) async fn load_active_discovered_parent_rows_with_progress(
    executor: &mut PgConnection,
    excluded_discovery_source: Option<&str>,
    progress: &mut dyn AdmissionStateProgress,
) -> Result<Vec<PgRow>> {
    let mut rows = Vec::new();
    let mut after_id = 0i64;
    loop {
        let edge_ids = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT de.discovery_edge_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE de.discovery_edge_id > $5
              AND mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind = $4
              AND de.admission = $1
              AND de.provenance ? $2
              AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
              AND EXISTS (
                  SELECT 1
                  FROM contract_instance_addresses cia
                  WHERE cia.contract_instance_id = de.to_contract_instance_id
                    AND cia.deactivated_at IS NULL
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM chain_lineage start_block
                  WHERE start_block.chain_id = de.chain_id
                    AND start_block.block_hash = de.active_from_block_hash
                    AND start_block.canonicality_state = 'orphaned'::canonicality_state
              )
            ORDER BY de.discovery_edge_id
            LIMIT $6
            "#,
        )
        .bind(REACHABLE_FROM_ROOT_ADMISSION)
        .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
        .bind(excluded_discovery_source)
        .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
        .bind(after_id)
        .bind(ADMISSION_LOAD_PAGE_SIZE)
        .fetch_all(&mut *executor)
        .await
        .context("failed to page discovery edges for active transitive parents")?;
        let Some(last_id) = edge_ids.last().copied() else {
            break;
        };
        after_id = last_id;

        let mut page = sqlx::query(
            r#"
            SELECT
                mv.manifest_id,
                mv.chain,
                de.provenance ->> 'propagated_role' AS role,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_edge_id = ANY($5::BIGINT[])
              AND mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind = $4
              AND de.admission = $1
              AND de.provenance ? $2
              AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
              AND NOT EXISTS (
                  SELECT 1
                  FROM chain_lineage start_block
                  WHERE start_block.chain_id = de.chain_id
                    AND start_block.block_hash = de.active_from_block_hash
                    AND start_block.canonicality_state = 'orphaned'::canonicality_state
              )
            ORDER BY de.discovery_edge_id
            "#,
        )
        .bind(REACHABLE_FROM_ROOT_ADMISSION)
        .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
        .bind(excluded_discovery_source)
        .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
        .bind(&edge_ids)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load a page of active transitive discovery parents")?;
        rows.append(&mut page);
        progress.record().await?;
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
struct KnownAddressCandidate {
    contract_instance_id: Uuid,
    active: bool,
    admitted_at: OffsetDateTime,
}

pub(super) async fn load_known_contract_instance_addresses_with_progress(
    executor: &mut PgConnection,
    progress: &mut dyn AdmissionStateProgress,
) -> Result<HashMap<(String, String), Uuid>> {
    let mut winners = HashMap::<(String, String), KnownAddressCandidate>::new();
    let mut after_id = 0i64;
    loop {
        let rows = sqlx::query(
            r#"
            SELECT
                contract_instance_address_id,
                chain_id,
                address,
                contract_instance_id,
                deactivated_at IS NULL AS active,
                admitted_at
            FROM contract_instance_addresses
            WHERE contract_instance_address_id > $1
            ORDER BY contract_instance_address_id
            LIMIT $2
            "#,
        )
        .bind(after_id)
        .bind(ADMISSION_LOAD_PAGE_SIZE)
        .fetch_all(&mut *executor)
        .await
        .context("failed to page known contract-instance addresses")?;
        let Some(last_row) = rows.last() else {
            break;
        };
        after_id = last_row
            .try_get("contract_instance_address_id")
            .context("failed to read known address page cursor")?;

        for row in rows {
            let key = (
                row.try_get("chain_id")
                    .context("failed to read known address chain_id")?,
                normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read known address")?,
                ),
            );
            let candidate = KnownAddressCandidate {
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read known address contract_instance_id")?,
                active: row
                    .try_get("active")
                    .context("failed to read known address active state")?,
                admitted_at: row
                    .try_get("admitted_at")
                    .context("failed to read known address admitted_at")?,
            };
            match winners.get_mut(&key) {
                Some(winner) if candidate_precedes(candidate, *winner) => *winner = candidate,
                None => {
                    winners.insert(key, candidate);
                }
                Some(_) => {}
            }
        }
        progress.record().await?;
    }

    let mut result = HashMap::with_capacity(winners.len());
    for (key, winner) in winners {
        result.insert(key, winner.contract_instance_id);
        if result.len().is_multiple_of(ADMISSION_LOAD_ROWS) {
            progress.record().await?;
        }
    }
    if !result.is_empty() && !result.len().is_multiple_of(ADMISSION_LOAD_ROWS) {
        progress.record().await?;
    }
    Ok(result)
}

fn candidate_precedes(left: KnownAddressCandidate, right: KnownAddressCandidate) -> bool {
    if left.active != right.active {
        return left.active;
    }
    left.admitted_at > right.admitted_at
        || (left.admitted_at == right.admitted_at
            && left.contract_instance_id < right.contract_instance_id)
}

#[cfg(test)]
#[path = "progress/tests.rs"]
mod tests;
