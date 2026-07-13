mod loading;
mod math;
mod projection;
mod types;

#[cfg(test)]
mod tests;

use anyhow::Result;
use sqlx::PgPool;

pub use types::{GasSponsorshipCurrentRebuildSummary, GasSponsorshipGlobalRebuildSummary};

/// Rebuild per-name sponsored-update accounting rows: one name when
/// `logical_name_id` is given, otherwise every name with registration or
/// sponsored-write facts.
pub async fn rebuild_gas_sponsorship_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<GasSponsorshipCurrentRebuildSummary> {
    projection::rebuild_gas_sponsorship_current(pool, logical_name_id).await
}

/// Rebuild namespace-wide sponsored-gas totals: one namespace when given,
/// otherwise every namespace with sponsored-operation facts.
pub async fn rebuild_gas_sponsorship_global_current(
    pool: &PgPool,
    namespace: Option<&str>,
) -> Result<GasSponsorshipGlobalRebuildSummary> {
    projection::rebuild_gas_sponsorship_global_current(pool, namespace).await
}
