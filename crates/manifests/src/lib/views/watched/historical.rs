use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::WatchedContract;

use super::{sort_and_dedup_watched_contracts, watched_contracts_from_rows};

/// Load current manifest declarations plus every bounded discovery interval
/// retained under the active manifest corpus for full-closure replay.
pub async fn load_historical_watched_contracts_by_chain(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM (
            SELECT
                mv.chain AS chain,
                mv.source_family AS source_family,
                cia.address AS address,
                mci.contract_instance_id AS contract_instance_id,
                CASE
                    WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                    ELSE 'manifest_contract'
                END::TEXT AS source,
                mv.manifest_id AS source_manifest_id,
                CASE
                    WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
                END AS active_from_block_number,
                cia.active_to_block_number AS active_to_block_number
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            LEFT JOIN LATERAL (
                SELECT (entry ->> 'start_block')::BIGINT AS start_block
                FROM jsonb_array_elements(
                    CASE
                        WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                        ELSE mv.manifest_payload -> 'contracts'
                    END
                ) entry
                WHERE (
                        mci.declaration_kind = 'root'
                        AND entry ->> 'name' = mci.declaration_name
                    )
                   OR (
                        mci.declaration_kind = 'contract'
                        AND entry ->> 'role' = mci.declaration_name
                    )
                ORDER BY start_block NULLS LAST
                LIMIT 1
            ) manifest_range ON TRUE
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1

            UNION

            SELECT
                de.chain_id AS chain,
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                cia.address AS address,
                de.to_contract_instance_id AS contract_instance_id,
                'discovery_edge'::TEXT AS source,
                COALESCE(target_mv.manifest_id, de.source_manifest_id) AS source_manifest_id,
                CASE
                    WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                    WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                    ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                    WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                    ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
                END AS active_to_block_number
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            LEFT JOIN manifest_versions target_mv
              ON target_mv.rollout_status = 'active'
             AND target_mv.namespace = mv.namespace
             AND target_mv.chain = de.chain_id
             AND target_mv.deployment_epoch = mv.deployment_epoch
             AND target_mv.source_family = CASE
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
            WHERE mv.rollout_status = 'active'
              AND de.chain_id = $1
              AND de.edge_kind <> 'migration'
              AND (de.deactivated_at IS NULL OR de.active_to_block_number IS NOT NULL)
              AND (
                  de.edge_kind <> 'resolver'
                  OR mv.source_family NOT IN (
                      'ens_v1_registry_l1',
                      'ens_v2_registry_l1',
                      'basenames_base_registry'
                  )
                  OR target_mv.manifest_id IS NOT NULL
              )
              AND (
                  de.active_from_block_number IS NULL
                  OR cia.active_to_block_number IS NULL
                  OR de.active_from_block_number <= cia.active_to_block_number
              )
              AND (
                  cia.active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR cia.active_from_block_number <= de.active_to_block_number
              )
        ) watched_contracts
        ORDER BY 1, 2, 3, 5, 6, 4, 7, 8
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load historical watched contracts for chain {chain}"))?;

    let mut watched_contracts = watched_contracts_from_rows(rows)?;
    sort_and_dedup_watched_contracts(&mut watched_contracts);
    Ok(watched_contracts)
}
