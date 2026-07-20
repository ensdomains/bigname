use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgConnection, types::Uuid};

use super::super::types::{ExistingReconciledDiscoveryEdge, ReconciledDiscoveryEdgeSpec};
use crate::{discovery::provenance::evm_event_position, normalize_address};

pub(super) async fn load_active_reconciled_discovery_edge_chains(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<Vec<String>> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT chain_id
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        ORDER BY chain_id
        "#,
    )
    .bind(discovery_source)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load active discovery-edge chains for discovery_source {discovery_source}"
        )
    })
}

pub(super) async fn load_active_reconciled_discovery_edge_count(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL",
    )
    .bind(discovery_source)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!("failed to count active discovery edges for discovery_source {discovery_source}")
    })?;
    usize::try_from(count).context("active discovery edge count exceeds usize")
}

pub(super) async fn load_active_reconciled_discovery_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    let existing_rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address,
            EXISTS (
                SELECT 1
                FROM chain_lineage rb
                WHERE rb.chain_id = de.chain_id
                  AND rb.block_hash = de.active_from_block_hash
                  AND rb.canonicality_state = 'orphaned'::canonicality_state
            ) AS active_from_block_is_orphaned
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load active discovery edges for discovery_source {discovery_source}")
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

pub(super) async fn load_active_reconciled_discovery_edges_by_observation_keys(
    executor: &mut PgConnection,
    discovery_source: &str,
    observation_keys: &[String],
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    if observation_keys.is_empty() {
        return Ok(Vec::new());
    }

    let existing_rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address,
            EXISTS (
                SELECT 1
                FROM chain_lineage rb
                WHERE rb.chain_id = de.chain_id
                  AND rb.block_hash = de.active_from_block_hash
                  AND rb.canonicality_state = 'orphaned'::canonicality_state
            ) AS active_from_block_is_orphaned
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.provenance ->> 'observation_key' = ANY($2::TEXT[])
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .bind(observation_keys)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load touched active discovery edges for discovery_source {discovery_source}"
        )
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

pub(super) async fn load_unreachable_reconciled_discovery_descendant_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    chain: &str,
    parent_contract_instance_ids: &[Uuid],
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    if parent_contract_instance_ids.is_empty() {
        return Ok(Vec::new());
    }

    let existing_rows = sqlx::query(
        r#"
        WITH RECURSIVE reachable_contracts AS (
            SELECT
                mv.manifest_id,
                mv.chain AS chain_id,
                mci.contract_instance_id,
                mci.role
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci
              ON mci.manifest_id = mv.manifest_id
             AND mci.declaration_kind = 'contract'
            WHERE mv.rollout_status = 'active'
              AND EXISTS (
                  SELECT 1
                  FROM manifest_contract_instances root
                  WHERE root.manifest_id = mv.manifest_id
                    AND root.declaration_kind = 'root'
              )
            UNION
            SELECT
                reachable.manifest_id,
                reachable.chain_id,
                edge.to_contract_instance_id,
                reachable.role
            FROM reachable_contracts reachable
            JOIN discovery_edges edge
              ON edge.source_manifest_id = reachable.manifest_id
             AND edge.chain_id = reachable.chain_id
             AND edge.from_contract_instance_id = reachable.contract_instance_id
             AND edge.edge_kind = 'subregistry'
             AND edge.deactivated_at IS NULL
            JOIN manifest_discovery_rules rule
              ON rule.manifest_id = reachable.manifest_id
             AND rule.edge_kind = edge.edge_kind
             AND rule.from_role = reachable.role
             AND rule.admission = edge.admission
            WHERE edge.provenance ->> 'propagated_role' = reachable.role
        ),
        descendant_edges AS (
            SELECT de.discovery_edge_id
            FROM discovery_edges de
            WHERE de.discovery_source = $1
              AND de.chain_id = $2
              AND de.deactivated_at IS NULL
              AND de.from_contract_instance_id = ANY($3::UUID[])
            UNION
            SELECT child.discovery_edge_id
            FROM discovery_edges child
            JOIN descendant_edges parent_ids ON true
            JOIN discovery_edges parent
              ON parent.discovery_edge_id = parent_ids.discovery_edge_id
            WHERE child.discovery_source = $1
              AND child.chain_id = $2
              AND child.deactivated_at IS NULL
              AND parent.edge_kind = 'subregistry'
              AND child.from_contract_instance_id = parent.to_contract_instance_id
        )
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address,
            EXISTS (
                SELECT 1
                FROM chain_lineage rb
                WHERE rb.chain_id = de.chain_id
                  AND rb.block_hash = de.active_from_block_hash
                  AND rb.canonicality_state = 'orphaned'::canonicality_state
            ) AS active_from_block_is_orphaned
        FROM discovery_edges de
        JOIN descendant_edges descendant
          ON descendant.discovery_edge_id = de.discovery_edge_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE NOT EXISTS (
            SELECT 1
            FROM reachable_contracts reachable
            JOIN manifest_discovery_rules rule
              ON rule.manifest_id = reachable.manifest_id
             AND rule.edge_kind = de.edge_kind
             AND rule.from_role = reachable.role
             AND rule.admission = de.admission
            WHERE reachable.manifest_id = de.source_manifest_id
              AND reachable.chain_id = de.chain_id
              AND reachable.contract_instance_id = de.from_contract_instance_id
        )
        "#,
    )
    .bind(discovery_source)
    .bind(chain)
    .bind(parent_contract_instance_ids)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load unreachable descendant discovery edges for discovery_source {discovery_source}"
        )
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

pub(super) fn edge_from_row(row: sqlx::postgres::PgRow) -> Result<ExistingReconciledDiscoveryEdge> {
    let observation_key = row
        .try_get::<Option<String>, _>("observation_key")
        .context("failed to read observation_key")?
        .context("active reconciled discovery edge is missing provenance.observation_key")?;
    let mut provenance = row
        .try_get::<serde_json::Value, _>("provenance")
        .context("failed to read provenance")?;
    let active_from_event_position = evm_event_position(&provenance)
        .context("active reconciled discovery edge has invalid EVM event-position provenance")?;
    let provenance_object = provenance
        .as_object_mut()
        .context("active reconciled discovery edge provenance must be a JSON object")?;
    provenance_object.remove("active_to_transaction_index");
    provenance_object.remove("active_to_log_index");
    Ok(ExistingReconciledDiscoveryEdge {
        discovery_edge_id: row
            .try_get("discovery_edge_id")
            .context("failed to read discovery_edge_id")?,
        to_address: normalize_address(
            &row.try_get::<String, _>("to_address")
                .context("failed to read to_address")?,
        ),
        active_from_block_is_orphaned: row
            .try_get("active_from_block_is_orphaned")
            .context("failed to read active_from_block_is_orphaned")?,
        spec: ReconciledDiscoveryEdgeSpec {
            observation_key,
            chain: row.try_get("chain_id").context("failed to read chain_id")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read edge_kind")?,
            from_contract_instance_id: row
                .try_get("from_contract_instance_id")
                .context("failed to read from_contract_instance_id")?,
            to_contract_instance_id: row
                .try_get("to_contract_instance_id")
                .context("failed to read to_contract_instance_id")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read discovery_source")?,
            source_manifest_id: row
                .try_get::<Option<i64>, _>("source_manifest_id")
                .context("failed to read source_manifest_id")?
                .unwrap_or(-1),
            admission: row
                .try_get("admission")
                .context("failed to read admission")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read active_from_block_hash")?,
            active_from_event_position,
            provenance_json: provenance.to_string(),
        },
    })
}
