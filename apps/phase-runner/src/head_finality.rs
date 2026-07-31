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
) -> RunnerResult<Option<BlockMarker>> {
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
    let Some((safe_number, safe_hash, finalized_number, finalized_hash)) = current else {
        return Ok(None);
    };
    let safe = marker_from_pair(safe_number, safe_hash);
    let finalized = marker_from_pair(finalized_number, finalized_hash);
    require_not_regressed("safe", safe.as_ref(), proposed.safe.as_ref())?;
    require_not_regressed("finalized", finalized.as_ref(), proposed.finalized.as_ref())?;
    Ok(finalized.or(safe))
}

pub(crate) async fn path_floor(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    previous_boundary: Option<&BlockMarker>,
) -> RunnerResult<i64> {
    if let Some(marker) = previous_boundary {
        return Ok(marker.number);
    }
    // The lineage walk is only deep on the first publication, before any finality boundary exists.
    sqlx::query_scalar::<_, Option<i64>>(
        "
        SELECT min(block_number)
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state <> 'orphaned'
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load lineage floor for chain {chain_id}: {error}"
        ))
    })?
    .ok_or_else(|| RunnerError::data_integrity(format!("chain {chain_id} has no stored lineage")))
}

fn require_not_regressed(
    label: &str,
    current: Option<&BlockMarker>,
    proposed: Option<&BlockMarker>,
) -> RunnerResult<()> {
    let Some(current) = current else {
        return Ok(());
    };
    let Some(proposed) = proposed else {
        return Err(RunnerError::data_integrity(format!(
            "{label} head marker cannot disappear after reaching height {}",
            current.number
        )));
    };
    if proposed.number < current.number {
        return Err(RunnerError::data_integrity(format!(
            "{label} head marker cannot move backward from height {} to {}",
            current.number, proposed.number
        )));
    }
    if proposed.number == current.number && proposed.hash != current.hash {
        return Err(RunnerError::data_integrity(format!(
            "{label} head marker at height {} cannot change hash",
            current.number
        )));
    }
    Ok(())
}

fn marker_from_pair(number: Option<i64>, hash: Option<String>) -> Option<BlockMarker> {
    number
        .zip(hash)
        .map(|(number, hash)| BlockMarker { number, hash })
}
