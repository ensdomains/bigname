use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{PgPool, Row};

use crate::ResolverCurrentRow;

pub async fn load_phase_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    let address = resolver_address.to_ascii_lowercase();
    let row = sqlx::query(
        r#"
        SELECT chain_id, resolver_address, declared_summary, support_status,
               unsupported_reason, provenance, chain_positions,
               canonicality_summary, manifest_version, last_recomputed_at
        FROM bigname_phase.resolver_current resolver
        WHERE chain_id = $1 AND lower(resolver_address) = $2
          AND resolver.canonicality_summary ->> 'state' = 'canonical_lineage'
          AND EXISTS (
              SELECT 1
              FROM bigname_phase.chain_lineage projection_lineage
              WHERE projection_lineage.chain_id = resolver.chain_id
                AND projection_lineage.block_hash =
                    resolver.chain_positions ->> 'target_block_hash'
                AND projection_lineage.canonicality_state IN (
                    'canonical'::bigname_phase.canonicality_state,
                    'safe'::bigname_phase.canonicality_state,
                    'finalized'::bigname_phase.canonicality_state
                )
          )
        "#,
    )
    .bind(chain_id)
    .bind(&address)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load phase resolver {chain_id}:{address}"))?;
    row.map(|row| {
        let support_status: String = row.try_get("support_status")?;
        let unsupported_reason: Option<String> = row.try_get("unsupported_reason")?;
        let coverage = if support_status == "supported" {
            json!({"status": "projected", "exhaustiveness": "not_asserted"})
        } else {
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_asserted",
                "unsupported_reason": unsupported_reason,
            })
        };
        Ok(ResolverCurrentRow {
            chain_id: row.try_get("chain_id")?,
            resolver_address: row
                .try_get::<String, _>("resolver_address")?
                .to_ascii_lowercase(),
            declared_summary: row.try_get("declared_summary")?,
            provenance: row.try_get("provenance")?,
            coverage,
            chain_positions: row.try_get("chain_positions")?,
            canonicality_summary: row.try_get("canonicality_summary")?,
            manifest_version: row.try_get("manifest_version")?,
            last_recomputed_at: row.try_get("last_recomputed_at")?,
        })
    })
    .transpose()
}
