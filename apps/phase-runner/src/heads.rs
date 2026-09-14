mod persist;

use sqlx::{PgPool, Postgres, Transaction};

use crate::error::{ErrorKind, RunnerError, RunnerResult};
use crate::head_finality::StoredFinality;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockMarker {
    pub number: i64,
    pub hash: String,
}

impl BlockMarker {
    pub fn new(number: i64, hash: impl Into<String>) -> RunnerResult<Self> {
        let hash = hash.into();
        if number < 0 {
            return Err(RunnerError::new(
                ErrorKind::DataIntegrity,
                format!("block marker number must be nonnegative, got {number}"),
            ));
        }
        if hash.trim().is_empty() {
            return Err(RunnerError::new(
                ErrorKind::DataIntegrity,
                "block marker hash must not be empty",
            ));
        }
        Ok(Self { number, hash })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadMarkers {
    pub latest: BlockMarker,
    pub safe: Option<BlockMarker>,
    pub finalized: Option<BlockMarker>,
}

impl HeadMarkers {
    pub fn validate(&self) -> RunnerResult<()> {
        if let Some(safe) = &self.safe
            && safe.number > self.latest.number
        {
            return Err(RunnerError::data_integrity(format!(
                "safe head {} is above latest head {}",
                safe.number, self.latest.number
            )));
        }
        if let Some(finalized) = &self.finalized {
            let Some(safe) = &self.safe else {
                return Err(RunnerError::data_integrity(
                    "a finalized head requires a safe head",
                ));
            };
            if finalized.number > safe.number {
                return Err(RunnerError::data_integrity(format!(
                    "finalized head {} is above safe head {}",
                    finalized.number, safe.number
                )));
            }
        }
        Ok(())
    }
}

/// Keep the stored safe/finalized markers when the provider reports lower ones.
///
/// Hosted RPC load balancers hand consecutive polls to different nodes whose safe and
/// finalized markers can trail each other by tens of blocks. A lower marker from a lagging
/// node is not a chain event: safe and finalized blocks do not un-finalize. The stored marker
/// is kept and the provider's is logged, so the monotonic finality check in publication still
/// catches a genuine regression (a hash change at the same height, or a marker disappearing).
pub(crate) async fn retain_stored_finality(
    pool: &PgPool,
    chain_id: &str,
    proposed: &HeadMarkers,
) -> RunnerResult<HeadMarkers> {
    let stored: Option<StoredFinality> = sqlx::query_as(
        "
            SELECT safe_block_number,
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
        RunnerError::transient(format!(
            "failed to load stored finality markers for chain {chain_id}: {error}"
        ))
    })?;
    let Some((safe_number, safe_hash, finalized_number, finalized_hash)) = stored else {
        return Ok(proposed.clone());
    };
    let stored_safe = safe_number
        .zip(safe_hash)
        .map(|(number, hash)| BlockMarker { number, hash });
    let stored_finalized = finalized_number
        .zip(finalized_hash)
        .map(|(number, hash)| BlockMarker { number, hash });
    Ok(retain_higher_finality(
        chain_id,
        proposed,
        stored_safe.as_ref(),
        stored_finalized.as_ref(),
    ))
}

fn retain_higher_finality(
    chain_id: &str,
    proposed: &HeadMarkers,
    stored_safe: Option<&BlockMarker>,
    stored_finalized: Option<&BlockMarker>,
) -> HeadMarkers {
    HeadMarkers {
        latest: proposed.latest.clone(),
        safe: retain_higher_marker(chain_id, "safe", proposed.safe.as_ref(), stored_safe),
        finalized: retain_higher_marker(
            chain_id,
            "finalized",
            proposed.finalized.as_ref(),
            stored_finalized,
        ),
    }
}

fn retain_higher_marker(
    chain_id: &str,
    label: &str,
    proposed: Option<&BlockMarker>,
    stored: Option<&BlockMarker>,
) -> Option<BlockMarker> {
    match (proposed, stored) {
        (Some(proposed), Some(stored)) if proposed.number < stored.number => {
            tracing::warn!(
                service = "phase-runner",
                chain_id,
                marker = label,
                provider_height = proposed.number,
                stored_height = stored.number,
                "provider reported a {label} head below the stored marker; keeping the stored \
                 marker (a lagging load-balanced node, not a chain event)"
            );
            Some(stored.clone())
        }
        (proposed, _) => proposed.cloned(),
    }
}

pub(crate) async fn load_available_heads(
    pool: &PgPool,
    chain_id: &str,
) -> RunnerResult<Option<HeadMarkers>> {
    let latest = sqlx::query_as::<_, (i64, String)>(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY block_number DESC, block_hash
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load latest available block for chain {chain_id}: {error}"
        ))
    })?
    .map(|(number, hash)| BlockMarker { number, hash });

    let Some(latest) = latest else {
        return Ok(None);
    };
    let safe = load_highest_at_least(pool, chain_id, "safe").await?;
    let finalized = load_highest_at_least(pool, chain_id, "finalized").await?;
    Ok(Some(HeadMarkers {
        latest,
        safe,
        finalized,
    }))
}

pub(crate) async fn load_marker(
    pool: &PgPool,
    chain_id: &str,
    block_number: i64,
) -> RunnerResult<Option<BlockMarker>> {
    sqlx::query_as::<_, (i64, String)>(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain_id)
    .bind(block_number)
    .fetch_optional(pool)
    .await
    .map(|marker| marker.map(|(number, hash)| BlockMarker { number, hash }))
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load block {block_number} for chain {chain_id}: {error}"
        ))
    })
}

async fn load_highest_at_least(
    pool: &PgPool,
    chain_id: &str,
    minimum_state: &str,
) -> RunnerResult<Option<BlockMarker>> {
    let states: &[&str] = match minimum_state {
        "safe" => &["safe", "finalized"],
        "finalized" => &["finalized"],
        _ => {
            return Err(RunnerError::data_integrity(format!(
                "unsupported head state {minimum_state}"
            )));
        }
    };
    sqlx::query_as::<_, (i64, String)>(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state::text = ANY($2)
        ORDER BY block_number DESC, block_hash
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .bind(states)
    .fetch_optional(pool)
    .await
    .map(|marker| marker.map(|(number, hash)| BlockMarker { number, hash }))
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load {minimum_state} block for chain {chain_id}: {error}"
        ))
    })
}

pub async fn publish_heads(pool: &PgPool, chain_id: &str, heads: &HeadMarkers) -> RunnerResult<()> {
    heads.validate()?;
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::transient(format!(
            "failed to begin head publication for chain {chain_id}: {error}"
        ))
    })?;
    let previous_boundary =
        crate::head_finality::require_monotonic(&mut transaction, chain_id, heads).await?;
    let mut path_floor =
        crate::head_finality::path_floor(&mut transaction, chain_id, previous_boundary.as_ref())
            .await?;
    if let Some(proposed_boundary) = heads.finalized.as_ref().or(heads.safe.as_ref()) {
        path_floor = path_floor.min(proposed_boundary.number);
    }
    let path = load_latest_path(
        &mut transaction,
        chain_id,
        &heads.latest,
        path_floor,
        previous_boundary.as_ref(),
    )
    .await?;
    require_marker_on_path("safe", heads.safe.as_ref(), &path)?;
    require_marker_on_path("finalized", heads.finalized.as_ref(), &path)?;
    let hashes = path
        .iter()
        .map(|(_, hash)| hash.as_str())
        .collect::<Vec<_>>();

    let (orphaned_from, previous_orphaning_epoch) = replace_readable_path(
        &mut transaction,
        chain_id,
        &hashes,
        path_floor,
        heads.latest.number,
    )
    .await?;
    let orphaned_readable_lineage = orphaned_from.is_some();
    let lineage_orphaning_epoch = previous_orphaning_epoch
        .checked_add(if orphaned_readable_lineage { 1 } else { 0 })
        .ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "lineage orphaning epoch overflow for chain {chain_id}"
            ))
        })?;
    if let Some(from) = orphaned_from {
        crate::redo_stamp::stamp_orphaned_suffix(&mut transaction, chain_id, from).await?;
    }
    promote_to_canonical(&mut transaction, chain_id, &hashes).await?;
    if let Some(safe) = &heads.safe {
        promote_to_safe(&mut transaction, chain_id, &hashes, safe.number).await?;
    }
    if let Some(finalized) = &heads.finalized {
        promote_to_finalized(&mut transaction, chain_id, finalized.number).await?;
    }
    persist::markers(
        &mut transaction,
        chain_id,
        heads,
        lineage_orphaning_epoch,
        orphaned_readable_lineage,
    )
    .await?;

    transaction.commit().await.map_err(|error| {
        RunnerError::transient(format!(
            "failed to commit head publication for chain {chain_id}: {error}"
        ))
    })
}

async fn replace_readable_path(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    hashes: &[&str],
    path_floor: i64,
    path_ceiling: i64,
) -> RunnerResult<(Option<i64>, i64)> {
    let previous_orphaning_epoch: Option<i64> = sqlx::query_scalar(
        "DELETE FROM chain_heads WHERE chain_id = $1 RETURNING lineage_orphaning_epoch",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| head_write_error("lock current head markers", chain_id, error))?;
    let conflicting_finalized: Option<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND canonicality_state = 'finalized'
          AND NOT (block_hash = ANY($3))
        ORDER BY block_number
        LIMIT 1
        ",
    )
    .bind(chain_id)
    .bind(path_floor)
    .bind(hashes)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| head_write_error("check finalized fork boundary", chain_id, error))?;
    if let Some((number, hash)) = conflicting_finalized {
        return Err(RunnerError::data_integrity(format!(
            "cannot publish head for chain {chain_id}: proposed path conflicts with finalized \
             block {hash} at height {number}"
        )));
    }
    let orphaned = sqlx::query_scalar::<_, i64>(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = $1
          AND block_number >= $2
          AND canonicality_state IN ('canonical', 'safe')
          AND NOT (block_hash = ANY($3))
        RETURNING block_number
        ",
    )
    .bind(chain_id)
    .bind(path_floor)
    .bind(hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| head_write_error("orphan displaced readable path", chain_id, error))?;
    crate::head_observed::orphan_displaced(transaction, chain_id, hashes, path_floor, path_ceiling)
        .await?;
    Ok((
        orphaned.into_iter().min(),
        previous_orphaning_epoch.unwrap_or(0),
    ))
}

async fn load_latest_path(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    latest: &BlockMarker,
    path_floor: i64,
    expected_floor: Option<&BlockMarker>,
) -> RunnerResult<Vec<(i64, String)>> {
    let path = sqlx::query_as::<_, (i64, String, Option<String>)>(
        "
        WITH RECURSIVE latest_path AS (
            SELECT block_number, block_hash, parent_hash
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
            UNION ALL
            SELECT parent.block_number, parent.block_hash, parent.parent_hash
            FROM chain_lineage AS parent
            JOIN latest_path AS child
              ON parent.chain_id = $1
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
            WHERE child.block_number > $4
        )
        SELECT block_number, block_hash, parent_hash
        FROM latest_path
        ORDER BY block_number DESC
        ",
    )
    .bind(chain_id)
    .bind(&latest.hash)
    .bind(latest.number)
    .bind(path_floor)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load latest path for chain {chain_id}: {error}"
        ))
    })?;
    if path.first().map(|(number, hash, _)| (*number, hash)) != Some((latest.number, &latest.hash))
    {
        return Err(RunnerError::data_integrity(format!(
            "latest head {} at block {} is missing from chain lineage for {chain_id}",
            latest.hash, latest.number
        )));
    }
    let reached_floor = path
        .last()
        .is_some_and(|(number, _, _)| *number == path_floor);
    let reached_expected_boundary = expected_floor.is_none_or(|expected| {
        path.iter()
            .any(|(number, hash, _)| *number == expected.number && hash == &expected.hash)
    });
    if !reached_floor || !reached_expected_boundary {
        let stopped_at = path
            .last()
            .map(|(number, hash, _)| format!("{hash} at block {number}"))
            .unwrap_or_else(|| "an empty path".to_owned());
        return Err(RunnerError::data_integrity(format!(
            "lineage gap for chain {chain_id}: latest path stopped at {stopped_at} before \
             reaching required boundary block {path_floor}"
        )));
    }
    Ok(path
        .into_iter()
        .map(|(number, hash, _)| (number, hash))
        .collect())
}

fn require_marker_on_path(
    label: &str,
    marker: Option<&BlockMarker>,
    path: &[(i64, String)],
) -> RunnerResult<()> {
    if let Some(marker) = marker
        && !path
            .iter()
            .any(|(number, hash)| *number == marker.number && hash == &marker.hash)
    {
        return Err(RunnerError::data_integrity(format!(
            "{label} head {} at block {} is not an ancestor of latest",
            marker.hash, marker.number
        )));
    }
    Ok(())
}

async fn promote_to_canonical(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    hashes: &[&str],
) -> RunnerResult<()> {
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'
        WHERE chain_id = $1
          AND block_hash = ANY($2)
          AND canonicality_state IN ('observed', 'orphaned')
        ",
    )
    .bind(chain_id)
    .bind(hashes)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("make latest path canonical", chain_id, error))?;
    Ok(())
}

async fn promote_to_safe(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    hashes: &[&str],
    through: i64,
) -> RunnerResult<()> {
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'safe'
        WHERE chain_id = $1
          AND block_hash = ANY($2)
          AND block_number <= $3
          AND canonicality_state = 'canonical'
        ",
    )
    .bind(chain_id)
    .bind(hashes)
    .bind(through)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("mark safe path", chain_id, error))?;
    Ok(())
}

async fn promote_to_finalized(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    through: i64,
) -> RunnerResult<()> {
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'finalized'
        WHERE chain_id = $1
          AND block_number <= $2
          AND canonicality_state = 'safe'
        ",
    )
    .bind(chain_id)
    .bind(through)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("mark finalized path", chain_id, error))?;
    Ok(())
}

pub(crate) fn head_write_error(action: &str, chain_id: &str, error: sqlx::Error) -> RunnerError {
    let retryable = match &error {
        sqlx::Error::Database(database) => database.code().is_some_and(|code| {
            ["08", "40", "53", "55", "57", "58"]
                .iter()
                .any(|class| code.starts_with(class))
        }),
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        _ => false,
    };
    let message = format!("failed to {action} for chain {chain_id}: {error}");
    if retryable {
        RunnerError::transient(message)
    } else {
        RunnerError::data_integrity(message)
    }
}

#[cfg(test)]
mod retain_finality_tests {
    use super::{BlockMarker, HeadMarkers, retain_higher_finality};

    fn marker(number: i64) -> BlockMarker {
        BlockMarker {
            number,
            hash: format!("0x{number:x}"),
        }
    }

    #[test]
    fn lower_provider_markers_keep_the_stored_ones() {
        let proposed = HeadMarkers {
            latest: marker(100),
            safe: Some(marker(60)),
            finalized: Some(marker(30)),
        };
        let retained =
            retain_higher_finality("chain", &proposed, Some(&marker(90)), Some(&marker(40)));
        assert_eq!(retained.latest, marker(100));
        assert_eq!(retained.safe, Some(marker(90)));
        assert_eq!(retained.finalized, Some(marker(40)));
    }

    #[test]
    fn equal_or_higher_provider_markers_are_published_as_reported() {
        let proposed = HeadMarkers {
            latest: marker(100),
            safe: Some(marker(95)),
            finalized: Some(marker(40)),
        };
        let retained =
            retain_higher_finality("chain", &proposed, Some(&marker(90)), Some(&marker(40)));
        assert_eq!(retained, proposed);
    }

    #[test]
    fn missing_markers_are_left_for_the_publication_check() {
        let proposed = HeadMarkers {
            latest: marker(100),
            safe: None,
            finalized: None,
        };
        let retained = retain_higher_finality("chain", &proposed, Some(&marker(90)), None);
        assert_eq!(retained, proposed);
        let retained = retain_higher_finality("chain", &proposed, None, None);
        assert_eq!(retained, proposed);
    }
}
