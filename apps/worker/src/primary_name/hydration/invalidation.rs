use anyhow::Result;
use bigname_storage::{
    PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, load_primary_name_current_snapshot,
    verified_primary_name_claim_hooks,
};
use sqlx::PgPool;

use super::super::types::PrimaryNameTupleKey;

pub(super) async fn invalidate_changed_hydration_snapshots(
    pool: &PgPool,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<()> {
    for snapshot in snapshots {
        let previous_snapshot = load_primary_name_current_snapshot(
            pool,
            &snapshot.row.address,
            &snapshot.row.namespace,
            &snapshot.row.coin_type,
        )
        .await?;
        if previous_snapshot.as_ref() == Some(snapshot) {
            continue;
        }
        invalidate_verified_primary_name_hydration_row(pool, &snapshot.row).await?;
    }
    Ok(())
}

pub(super) async fn invalidate_verified_primary_name_hydration_delete(
    pool: &PgPool,
    key: &PrimaryNameTupleKey,
) -> Result<()> {
    if let Some(previous_snapshot) =
        load_primary_name_current_snapshot(pool, &key.address, &key.namespace, &key.coin_type)
            .await?
    {
        invalidate_verified_primary_name_hydration_row(pool, &previous_snapshot.row).await?;
    }
    Ok(())
}

async fn invalidate_verified_primary_name_hydration_row(
    pool: &PgPool,
    row: &PrimaryNameCurrentRow,
) -> Result<()> {
    let hooks = verified_primary_name_claim_hooks(row)?;
    super::super::super::execution::invalidate_verified_primary_name_claim_change(
        pool,
        &hooks.lookup.namespace,
        &hooks.lookup.request_key(),
    )
    .await?;
    Ok(())
}
