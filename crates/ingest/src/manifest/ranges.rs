use sqlx::PgPool;

use crate::{IngestError, Result};

#[derive(sqlx::FromRow)]
struct DisjointRange {
    manifest: String,
    edge_kind: String,
    address: String,
    edge_from: Option<i64>,
    edge_to: Option<i64>,
    address_from: Option<i64>,
    address_to: Option<i64>,
}

pub(super) async fn validate(pool: &PgPool, chain_id: &str) -> Result<()> {
    let inverted: Option<(String, String, String, String, i64, i64)> = sqlx::query_as(
        "
        SELECT manifest.file_path,
               declaration.declaration_kind,
               declaration.declaration_name,
               lower(address.address),
               GREATEST(
                   COALESCE(declaration.start_block_number, 0),
                   COALESCE(address.active_from_block_number, 0)
               ) AS effective_start,
               address.active_to_block_number
        FROM manifest_versions manifest
        JOIN manifest_contract_instances declaration
          ON declaration.manifest_id = manifest.manifest_id
         AND declaration.chain_id = manifest.chain_id
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = declaration.contract_instance_id
         AND address.chain_id = manifest.chain_id
        WHERE manifest.chain_id = $1
          AND manifest.rollout_status = 'active'
          AND (
              address.deactivated_at IS NULL
              OR address.active_to_block_number IS NOT NULL
          )
          AND NOT EXISTS (
              SELECT 1
              FROM contract_instance_addresses candidate
              WHERE candidate.contract_instance_id = declaration.contract_instance_id
                AND candidate.chain_id = manifest.chain_id
                AND (
                    candidate.deactivated_at IS NULL
                    OR candidate.active_to_block_number IS NOT NULL
                )
                AND (
                    candidate.active_to_block_number IS NULL
                    OR GREATEST(
                        COALESCE(declaration.start_block_number, 0),
                        COALESCE(candidate.active_from_block_number, 0)
                    ) <= candidate.active_to_block_number
                )
          )
        ORDER BY manifest.file_path, declaration.declaration_kind,
                 declaration.declaration_name, address.active_to_block_number DESC
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to validate declared ingest ranges for chain {chain_id}"),
            error,
        )
    })?;
    if let Some((manifest, kind, name, address, start, end)) = inverted {
        return Err(IngestError::configuration(format!(
            "manifest {manifest} declaration {kind}:{name} for address {address} has inverted watch bounds: effective start block {start} is after address end block {end}"
        )));
    }

    let disjoint: Option<DisjointRange> = sqlx::query_as(
        "
        SELECT source_manifest.file_path AS manifest,
               edge.edge_kind,
               lower(address.address) AS address,
               edge.active_from_block_number AS edge_from,
               edge.active_to_block_number AS edge_to,
               address.active_from_block_number AS address_from,
               address.active_to_block_number AS address_to
        FROM discovery_edges edge
        JOIN manifest_versions source_manifest
          ON source_manifest.manifest_id = edge.source_manifest_id
         AND source_manifest.chain_id = edge.chain_id
        LEFT JOIN manifest_versions target_manifest
          ON target_manifest.rollout_status = 'active'
         AND target_manifest.namespace = source_manifest.namespace
         AND target_manifest.chain_id = edge.chain_id
         AND target_manifest.deployment_label = source_manifest.deployment_label
         AND target_manifest.source_family = CASE
             WHEN edge.edge_kind = 'resolver'
              AND source_manifest.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN edge.edge_kind = 'resolver'
              AND source_manifest.source_family IN (
                  'ens_v2_registry_l1',
                  'ens_v2_root_l1'
              )
                 THEN 'ens_v2_resolver_l1'
             WHEN edge.edge_kind = 'resolver'
              AND source_manifest.source_family = 'basenames_base_registry'
                 THEN 'basenames_base_resolver'
             ELSE NULL
         END
        JOIN contract_instance_addresses address
          ON address.contract_instance_id = edge.to_contract_instance_id
         AND address.chain_id = edge.chain_id
        WHERE edge.chain_id = $1
          AND source_manifest.rollout_status = 'active'
          AND edge.canonicality_state <> 'orphaned'
          AND edge.edge_kind IN ('resolver', 'registry_announcement')
          AND (
              edge.edge_kind <> 'resolver'
              OR source_manifest.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'ens_v2_root_l1',
                  'basenames_base_registry'
              )
              OR target_manifest.manifest_id IS NOT NULL
          )
          AND (
              edge.deactivated_at IS NULL
              OR edge.active_to_block_number IS NOT NULL
          )
          AND (
              address.deactivated_at IS NULL
              OR address.active_to_block_number IS NOT NULL
              OR edge.active_to_block_number IS NOT NULL
          )
          AND NOT EXISTS (
              SELECT 1
              FROM contract_instance_addresses candidate
              WHERE candidate.contract_instance_id = edge.to_contract_instance_id
                AND candidate.chain_id = edge.chain_id
                AND (
                    candidate.deactivated_at IS NULL
                    OR candidate.active_to_block_number IS NOT NULL
                    OR edge.active_to_block_number IS NOT NULL
                )
                AND (
                    edge.active_from_block_number IS NULL
                    OR candidate.active_to_block_number IS NULL
                    OR edge.active_from_block_number <= candidate.active_to_block_number
                )
                AND (
                    candidate.active_from_block_number IS NULL
                    OR edge.active_to_block_number IS NULL
                    OR candidate.active_from_block_number <= edge.active_to_block_number
                )
          )
        ORDER BY source_manifest.file_path, edge.edge_kind, address.address,
                 address.active_from_block_number
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to validate discovered ingest ranges for chain {chain_id}"),
            error,
        )
    })?;
    if let Some(disjoint) = disjoint {
        return Err(IngestError::configuration(format!(
            "manifest {} discovery edge {} for address {} has non-overlapping watch windows: edge {} and address {}",
            disjoint.manifest,
            disjoint.edge_kind,
            disjoint.address,
            window(disjoint.edge_from, disjoint.edge_to),
            window(disjoint.address_from, disjoint.address_to)
        )));
    }

    Ok(())
}

fn window(from: Option<i64>, to: Option<i64>) -> String {
    format!(
        "{}..={}",
        from.map_or_else(|| "unbounded".to_owned(), |value| value.to_string()),
        to.map_or_else(|| "unbounded".to_owned(), |value| value.to_string())
    )
}
