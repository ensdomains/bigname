use super::rows::decode_primary_name_current_snapshot;
use super::types::{PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, normalize_address};
use anyhow::{Context, Result};
use sqlx::PgPool;

pub const DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER: &str = r#"
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = pnc.claim_provenance ->> 'chain_id'
        AND projection_lineage.block_hash =
            pnc.claim_provenance ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
"#;

/// Load one declared primary-name claim-state row by exact address, namespace, and coin_type.
pub async fn load_primary_name_current(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentRow>> {
    load_primary_name_current_snapshot(pool, address, namespace, coin_type)
        .await
        .map(|snapshot| snapshot.map(|snapshot| snapshot.row))
}

/// Load one declared primary-name claim snapshot by exact address, namespace, and coin_type.
pub async fn load_primary_name_current_snapshot(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<PrimaryNameCurrentSnapshot>> {
    let normalized_address = normalize_address(address);
    let row = sqlx::query(&format!(
        r#"
        SELECT
            pnc.address,
            pnc.namespace,
            pnc.coin_type,
            CASE WHEN hydration.readable THEN pnc.claim_status
                 ELSE pnc.claim_provenance
                     -> 'canonical_head_multicall_hydration'
                     -> 'baseline' ->> 'claim_status'
            END AS claim_status,
            CASE WHEN hydration.readable THEN pnc.raw_claim_name
                 ELSE pnc.claim_provenance
                     -> 'canonical_head_multicall_hydration'
                     -> 'baseline' ->> 'raw_claim_name'
            END AS raw_claim_name,
            CASE WHEN hydration.readable THEN pnc.claim_name_is_normalized
                 ELSE (pnc.claim_provenance
                     -> 'canonical_head_multicall_hydration'
                     -> 'baseline' ->> 'claim_name_is_normalized')::boolean
            END AS claim_name_is_normalized,
            CASE WHEN hydration.readable THEN pnc.claim_provenance
                 ELSE pnc.claim_provenance - 'canonical_head_multicall_hydration'
            END AS claim_provenance
        FROM bigname_phase.primary_names_current pnc
        CROSS JOIN LATERAL (
            SELECT NOT (pnc.claim_provenance ? 'canonical_head_multicall_hydration')
                OR EXISTS (
                    SELECT 1
                    FROM bigname_phase.chain_lineage hydration_lineage
                    WHERE hydration_lineage.chain_id = pnc.claim_provenance
                        -> 'canonical_head_multicall_hydration' ->> 'chain_id'
                      AND hydration_lineage.block_number::text = pnc.claim_provenance
                        -> 'canonical_head_multicall_hydration' ->> 'block_number'
                      AND hydration_lineage.block_hash = pnc.claim_provenance
                        -> 'canonical_head_multicall_hydration' ->> 'block_hash'
                      AND hydration_lineage.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state
                      )
                ) AS readable
        ) hydration
        WHERE pnc.address = $1
          AND pnc.namespace = $2
          AND pnc.coin_type = $3
          {DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(&normalized_address)
    .bind(namespace)
    .bind(coin_type)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load primary_names_current snapshot for address {normalized_address} namespace {namespace} coin_type {coin_type}"
        )
    })?;

    row.map(decode_primary_name_current_snapshot).transpose()
}
