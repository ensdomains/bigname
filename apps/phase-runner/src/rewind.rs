use sqlx::PgPool;

use crate::{
    database::RunnerDatabase,
    error::{RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    phase::PhaseName,
    phase_lock::PhaseLock,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RewindOutcome {
    pub previous: BlockMarker,
    pub ancestor: BlockMarker,
}

pub async fn rewind_to_ancestor(
    database: &RunnerDatabase,
    chain_id: &str,
    ancestor: BlockMarker,
) -> RunnerResult<RewindOutcome> {
    let locks = acquire_writer_locks(database, chain_id).await?;
    let result = rewind_with_lock(database.pool(), chain_id, ancestor).await;
    let release = release_writer_locks(locks).await;
    match (result, release) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(release_error)) => {
            Err(error.with_secondary("release phase locks after rewind", release_error))
        }
    }
}

async fn acquire_writer_locks(
    database: &RunnerDatabase,
    chain_id: &str,
) -> RunnerResult<Vec<PhaseLock>> {
    let mut locks = Vec::new();
    for phase in [
        PhaseName::Ingest,
        PhaseName::Interpret,
        PhaseName::Project,
        PhaseName::Live,
    ] {
        match PhaseLock::acquire(database.connect_options(), chain_id, phase).await {
            Ok(lock) => locks.push(lock),
            Err(error) => {
                return match release_writer_locks(locks).await {
                    Ok(()) => Err(error),
                    Err(release_error) => Err(error.with_secondary(
                        "release acquired phase locks after rewind was refused",
                        release_error,
                    )),
                };
            }
        }
    }
    Ok(locks)
}

async fn release_writer_locks(mut locks: Vec<PhaseLock>) -> RunnerResult<()> {
    let mut failure: Option<RunnerError> = None;
    while let Some(lock) = locks.pop() {
        if let Err(error) = lock.release().await {
            failure = Some(match failure {
                None => error,
                Some(previous) => previous.with_secondary("release another rewind lock", error),
            });
        }
    }
    failure.map_or(Ok(()), Err)
}

async fn rewind_with_lock(
    pool: &PgPool,
    chain_id: &str,
    ancestor: BlockMarker,
) -> RunnerResult<RewindOutcome> {
    let stored: Option<(String, String)> = sqlx::query_as(
        "
        SELECT block_hash, canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND block_hash = $3
        ",
    )
    .bind(chain_id)
    .bind(ancestor.number)
    .bind(&ancestor.hash)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load rewind ancestor for chain {chain_id}"),
            error,
        )
    })?;
    match stored.as_ref().map(|(_, state)| state.as_str()) {
        Some("canonical" | "safe" | "finalized") => {}
        Some(state) => {
            return Err(RunnerError::data_integrity(format!(
                "rewind ancestor {} at block {} for chain {chain_id} is {state}, not on the \
                 readable path",
                ancestor.hash, ancestor.number
            )));
        }
        None => {
            return Err(RunnerError::data_integrity(format!(
                "rewind ancestor {} at block {} is not stored for chain {chain_id}",
                ancestor.hash, ancestor.number
            )));
        }
    }

    type HeadRow = (
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    );
    let row: Option<HeadRow> = sqlx::query_as(
        "
        SELECT latest_block_number,
               latest_block_hash,
               safe_block_number,
               safe_block_hash,
               finalized_block_number,
               finalized_block_hash
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load current head before rewinding chain {chain_id}"),
            error,
        )
    })?;
    let (latest_number, latest_hash, safe_number, safe_hash, finalized_number, finalized_hash) =
        row.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "cannot rewind chain {chain_id} without published head markers"
            ))
        })?;
    let previous = BlockMarker::new(latest_number, latest_hash)?;
    if ancestor.number >= previous.number {
        return Err(RunnerError::data_integrity(format!(
            "rewind ancestor block {} must be below current head {} for chain {chain_id}",
            ancestor.number, previous.number
        )));
    }
    let safe = paired_marker(safe_number, safe_hash, "safe")?;
    let finalized = paired_marker(finalized_number, finalized_hash, "finalized")?;
    if safe
        .as_ref()
        .is_some_and(|marker| marker.number > ancestor.number)
    {
        return Err(RunnerError::data_integrity(format!(
            "cannot rewind chain {chain_id} below safe block {}",
            safe.as_ref().expect("checked safe marker").number
        )));
    }
    publish_heads(
        pool,
        chain_id,
        &HeadMarkers {
            latest: ancestor.clone(),
            safe,
            finalized,
        },
    )
    .await?;
    Ok(RewindOutcome { previous, ancestor })
}

fn paired_marker(
    number: Option<i64>,
    hash: Option<String>,
    label: &str,
) -> RunnerResult<Option<BlockMarker>> {
    match (number, hash) {
        (Some(number), Some(hash)) => BlockMarker::new(number, hash).map(Some),
        (None, None) => Ok(None),
        _ => Err(RunnerError::data_integrity(format!(
            "stored {label} head has only a number or only a hash"
        ))),
    }
}
