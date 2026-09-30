use anyhow::Result;
use sqlx::PgPool;

use crate::ResolverCurrentRow;

/// The resolver overview from F3 classification at the selected family publication.
pub async fn load_phase_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    crate::families::topology::load_family_resolver_current(pool, chain_id, resolver_address).await
}
