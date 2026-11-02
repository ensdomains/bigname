//! Live's publication lag. Live loads blocks up to the node's head, and the runner publishes the
//! readable head `lag` blocks below the newest of them. The blocks above it stay `observed`, which
//! no phase reads. A reorg that replaces only those blocks orphans them without stamping a
//! downstream redo (head_observed.rs). A deeper reorg stamps from the lowest displaced readable
//! block, as an unlagged publication does.
use sqlx::{Postgres, Transaction};

use super::{BlockMarker, HeadMarkers, head_write_error};
use crate::error::{RunnerError, RunnerResult};

/// The heads to publish for `observed`, whose path runs from its latest block down to the
/// publication floor, newest first. The readable head is the path block `lag` below the newest,
/// never below a block of the path that is already readable. Safe and finalized are bounded to
/// it. A lag of 0 publishes `observed` as it is.
pub(super) async fn readable_heads(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    observed: &HeadMarkers,
    path: &[(i64, String)],
    lag: u32,
) -> RunnerResult<HeadMarkers> {
    if lag == 0 {
        return Ok(observed.clone());
    }
    let hashes = path
        .iter()
        .map(|(_, hash)| hash.as_str())
        .collect::<Vec<_>>();
    let readable: Option<i64> = sqlx::query_scalar(
        "SELECT max(block_number)
         FROM chain_lineage
         WHERE chain_id = $1
           AND block_hash = ANY($2)
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(&hashes)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        head_write_error("load the readable part of the latest path", chain_id, error)
    })?;
    let heads = select(chain_id, observed, path, lag, readable)?;
    // The bounded markers are what persists, so they face the stored ones again.
    crate::head_finality::require_monotonic(transaction, chain_id, &heads).await?;
    Ok(heads)
}

/// The path hashes at or below `through`, the blocks a publication makes readable.
pub(super) fn readable_hashes(path: &[(i64, String)], through: i64) -> Vec<&str> {
    path.iter()
        .filter(|(number, _)| *number <= through)
        .map(|(_, hash)| hash.as_str())
        .collect()
}

pub(super) fn select(
    chain_id: &str,
    observed: &HeadMarkers,
    path: &[(i64, String)],
    lag: u32,
    readable: Option<i64>,
) -> RunnerResult<HeadMarkers> {
    let newest = observed.latest.number;
    let floor = path.last().map_or(newest, |(number, _)| *number);
    let number = newest
        .saturating_sub(i64::from(lag))
        .max(readable.unwrap_or(floor))
        .max(floor)
        .min(newest);
    let latest = path
        .iter()
        .find(|(block, _)| *block == number)
        .map(|(number, hash)| BlockMarker {
            number: *number,
            hash: hash.clone(),
        })
        .ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "lagged readable head {number} for chain {chain_id} is not on the latest path"
            ))
        })?;
    let bound = |marker: &Option<BlockMarker>| {
        marker.as_ref().map(|marker| {
            if marker.number > number {
                latest.clone()
            } else {
                marker.clone()
            }
        })
    };
    let safe = bound(&observed.safe);
    let finalized = bound(&observed.finalized);
    Ok(HeadMarkers {
        latest,
        safe,
        finalized,
    })
}

#[cfg(test)]
#[path = "lag_tests.rs"]
mod tests;
