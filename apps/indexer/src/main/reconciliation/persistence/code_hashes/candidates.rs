use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::Row;

pub(super) async fn load_code_observation_candidates_by_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    generic_resolver_topic0s: &[String],
    new_resolver_topic0: &str,
) -> Result<BTreeMap<String, BTreeMap<String, bool>>> {
    if block_hashes.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT
                block_hash,
                LOWER(emitting_address) AS address,
                BOOL_OR(LOWER(topics[1]) = ANY($3::TEXT[])) AS selected
            FROM raw_logs
            WHERE chain_id = $1
              AND block_hash = ANY($2::TEXT[])
            GROUP BY block_hash, LOWER(emitting_address)

            UNION ALL

            SELECT
                block_hash,
                LOWER(after_state->>'resolver') AS address,
                TRUE AS selected
            FROM normalized_events
            WHERE chain_id = $1
              AND block_hash = ANY($2::TEXT[])
              AND event_kind = 'ResolverChanged'
              AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND canonicality_state <> 'orphaned'
              AND LOWER(after_state->>'resolver') ~ '^0x[0-9a-f]{40}$'
              AND LOWER(after_state->>'resolver') <>
                  '0x0000000000000000000000000000000000000000'

            UNION ALL

            SELECT
                resolver_log.block_hash,
                LOWER('0x' || ENCODE(SUBSTRING(resolver_log.data FROM 13 FOR 20), 'hex')) AS address,
                TRUE AS selected
            FROM raw_logs resolver_log
            WHERE resolver_log.chain_id = $1
              AND resolver_log.block_hash = ANY($2::TEXT[])
              AND LOWER(resolver_log.topics[1]) = $4
              AND OCTET_LENGTH(resolver_log.data) = 32
              AND SUBSTRING(resolver_log.data FROM 1 FOR 12) = DECODE(REPEAT('00', 12), 'hex')
              AND SUBSTRING(resolver_log.data FROM 13 FOR 20) <> DECODE(REPEAT('00', 20), 'hex')
              AND resolver_log.canonicality_state <> 'orphaned'
              AND EXISTS (
                  SELECT 1
                  FROM contract_instance_addresses address
                  JOIN manifest_contract_instances manifest_contract
                    ON manifest_contract.contract_instance_id = address.contract_instance_id
                  JOIN manifest_versions manifest
                    ON manifest.manifest_id = manifest_contract.manifest_id
                   AND manifest.rollout_status = 'active'
                  WHERE address.chain_id = resolver_log.chain_id
                    AND LOWER(address.address) = LOWER(resolver_log.emitting_address)
                    AND manifest.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                    AND COALESCE(address.active_from_block_number, 0) <= resolver_log.block_number
                    AND COALESCE(address.active_to_block_number, 9223372036854775807) >= resolver_log.block_number
              )
        )
        SELECT
            block_hash,
            address AS emitting_address,
            BOOL_OR(selected) AS topic0_selected
        FROM candidates
        GROUP BY block_hash, address
        ORDER BY block_hash, address
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(generic_resolver_topic0s)
    .bind(new_resolver_topic0)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load code-observation candidates for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    let mut addresses_by_block_hash = BTreeMap::<String, BTreeMap<String, bool>>::new();
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from raw-log emitter row")?;
        let emitting_address = row
            .try_get::<String, _>("emitting_address")
            .context("missing emitting_address from raw-log emitter row")?;
        let topic0_selected = row
            .try_get::<Option<bool>, _>("topic0_selected")
            .context("missing topic0_selected from raw-log emitter row")?
            .unwrap_or(false);
        addresses_by_block_hash
            .entry(block_hash)
            .or_default()
            .insert(emitting_address, topic0_selected);
    }

    Ok(addresses_by_block_hash)
}
