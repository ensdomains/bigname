use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers},
};

type StoredFinality = (Option<i64>, Option<String>, Option<i64>, Option<String>);

pub(crate) async fn require_monotonic(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    proposed: &HeadMarkers,
) -> RunnerResult<()> {
    let current: Option<StoredFinality> = sqlx::query_as(
        "
            SELECT safe_block_number,
                   safe_block_hash,
                   finalized_block_number,
                   finalized_block_hash
            FROM chain_heads
            WHERE chain_id = $1
            FOR UPDATE
        ",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to lock current head markers for chain {chain_id}: {error}"
        ))
    })?;
    let Some((safe_number, _, finalized_number, finalized_hash)) = current else {
        return Ok(());
    };
    require_not_regressed("safe", safe_number, proposed.safe.as_ref())?;
    require_not_regressed("finalized", finalized_number, proposed.finalized.as_ref())?;
    if let (Some(number), Some(hash), Some(marker)) = (
        finalized_number,
        finalized_hash.as_deref(),
        proposed.finalized.as_ref(),
    ) && marker.number == number
        && marker.hash != hash
    {
        return Err(RunnerError::data_integrity(format!(
            "finalized head marker at height {number} cannot change hash"
        )));
    }
    Ok(())
}

fn require_not_regressed(
    label: &str,
    current_number: Option<i64>,
    proposed: Option<&BlockMarker>,
) -> RunnerResult<()> {
    let Some(current_number) = current_number else {
        return Ok(());
    };
    let Some(proposed) = proposed else {
        return Err(RunnerError::data_integrity(format!(
            "{label} head marker cannot disappear after reaching height {current_number}"
        )));
    };
    if proposed.number < current_number {
        return Err(RunnerError::data_integrity(format!(
            "{label} head marker cannot move backward from height {current_number} to {}",
            proposed.number
        )));
    }
    Ok(())
}
