use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    transitions::PhaseStateRow,
};

pub(crate) async fn require_recorded_extent(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    previous: &PhaseStateRow,
    range: BlockRange,
    full_hash_adoption_validated: bool,
) -> RunnerResult<()> {
    let mut to = previous.current_block_number.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "cannot redo chain {chain_id} phase {phase}: the phase has no recorded processed extent"
        ))
    })?;
    if phase == PhaseName::Ingest {
        let latest: Option<i64> =
            sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
                .bind(chain_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        format!("failed to load the latest ingested head for chain {chain_id}"),
                        error,
                    )
                })?;
        to = latest.unwrap_or(to).max(to);
    }
    let from: Option<i64> = sqlx::query_scalar(
        "
        SELECT min(start_block_number)
        FROM ingest_cursors
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load recorded redo extent for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    if (!full_hash_adoption_validated && range.to > to)
        || from.is_some_and(|from| range.from < from)
    {
        let from = from.unwrap_or(0);
        return Err(RunnerError::data_integrity(format!(
            "redo range {}..={} is outside the recorded extent {from}..={to} for chain \
             {chain_id} phase {phase}",
            range.from, range.to
        )));
    }
    Ok(())
}
