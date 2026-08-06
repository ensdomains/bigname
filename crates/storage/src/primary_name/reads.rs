use super::rows::decode_primary_name_current_snapshot;
use super::types::{PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, normalize_address};
use anyhow::{Context, Result};
use sqlx::PgPool;

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
    let row = sqlx::query(
        r#"
        SELECT
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            claim_provenance
        FROM bigname_phase.primary_names_current pnc
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
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
        "#,
    )
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
