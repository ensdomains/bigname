use anyhow::Result;
use sqlx::PgPool;

use crate::ResolverCurrentRow;

pub const DEFAULT_RESOLVER_CURRENT_READ_FILTER: &str = r#"
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
"#;

/// The resolver overview row. Under the publication switch it comes from the F3 classification
/// at the family marker's publication instead (`families::topology::load_family_resolver_current`).
pub async fn load_phase_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    crate::families::topology::load_family_resolver_current(pool, chain_id, resolver_address).await
}
