use anyhow::{Context, Result};
use bigname_storage::{
    PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, delete_primary_name_current_in_transaction,
    load_primary_name_current_snapshot_for_update_in_transaction,
    lock_primary_name_tuple_in_transaction, normalize_evm_address,
    upsert_primary_name_current_snapshots_in_transaction, verified_primary_name_claim_hooks,
};
use sqlx::{PgPool, Postgres, Transaction};

use super::super::types::PrimaryNameTupleKey;

pub(super) async fn upsert_changed_hydration_snapshots(
    pool: &PgPool,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<Vec<PrimaryNameCurrentSnapshot>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open legacy reverse hydration publish transaction")?;
    let mut ordered_snapshots = snapshots.iter().collect::<Vec<_>>();
    ordered_snapshots.sort_by(|left, right| {
        (
            normalize_evm_address(&left.row.address),
            left.row.namespace.as_str(),
            left.row.coin_type.as_str(),
        )
            .cmp(&(
                normalize_evm_address(&right.row.address),
                right.row.namespace.as_str(),
                right.row.coin_type.as_str(),
            ))
    });

    for snapshot in ordered_snapshots {
        lock_primary_name_tuple_in_transaction(
            &mut transaction,
            &snapshot.row.address,
            &snapshot.row.namespace,
            &snapshot.row.coin_type,
        )
        .await?;
        let previous_snapshot = load_primary_name_current_snapshot_for_update_in_transaction(
            &mut transaction,
            &snapshot.row.address,
            &snapshot.row.namespace,
            &snapshot.row.coin_type,
        )
        .await?;
        if previous_snapshot.as_ref() != Some(snapshot) {
            invalidate_verified_primary_name_hydration_row_in_transaction(
                &mut transaction,
                &snapshot.row,
            )
            .await?;
        }
    }

    #[cfg(test)]
    super::test_hooks::run(pool).await?;
    let persisted =
        upsert_primary_name_current_snapshots_in_transaction(&mut transaction, snapshots).await?;
    transaction
        .commit()
        .await
        .context("failed to commit legacy reverse hydration publish")?;
    Ok(persisted)
}

pub(super) async fn delete_verified_primary_name_hydration_row(
    pool: &PgPool,
    key: &PrimaryNameTupleKey,
) -> Result<u64> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open legacy reverse hydration delete transaction")?;
    lock_primary_name_tuple_in_transaction(
        &mut transaction,
        &key.address,
        &key.namespace,
        &key.coin_type,
    )
    .await?;
    let previous_snapshot = load_primary_name_current_snapshot_for_update_in_transaction(
        &mut transaction,
        &key.address,
        &key.namespace,
        &key.coin_type,
    )
    .await?;
    if let Some(previous_snapshot) = previous_snapshot {
        invalidate_verified_primary_name_hydration_row_in_transaction(
            &mut transaction,
            &previous_snapshot.row,
        )
        .await?;
    }
    let deleted = delete_primary_name_current_in_transaction(
        &mut transaction,
        &key.address,
        &key.namespace,
        &key.coin_type,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit legacy reverse hydration delete")?;
    Ok(deleted)
}

async fn invalidate_verified_primary_name_hydration_row_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    row: &PrimaryNameCurrentRow,
) -> Result<()> {
    let hooks = verified_primary_name_claim_hooks(row)?;
    super::super::super::execution::invalidate_verified_primary_name_claim_change_in_transaction(
        transaction,
        &hooks.lookup.namespace,
        &hooks.lookup.request_key(),
    )
    .await?;
    Ok(())
}
