use anyhow::{Result, bail};
use bigname_storage::load_primary_name_current;
use sqlx::PgPool;

use super::VerifiedPrimaryNameTuple;
use super::context::verified_primary_context_label;

pub(crate) async fn ensure_primary_name_anchor_exists(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    if load_primary_name_current(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }

    bail!(
        "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
        tuple.normalized_address,
        tuple.namespace,
        tuple.coin_type
    )
}
