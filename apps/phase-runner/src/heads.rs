use sqlx::{PgPool, Postgres, Transaction};

use crate::error::{ErrorKind, RunnerError, RunnerResult};

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

pub(crate) async fn load_available_heads(
    pool: &PgPool,
    chain_id: &str,
) -> RunnerResult<Option<HeadMarkers>> {
    let latest = sqlx::query_as::<_, (i64, String)>(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state <> 'orphaned'
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
          AND canonicality_state <> 'orphaned'
        ORDER BY
            CASE canonicality_state
                WHEN 'finalized' THEN 4
                WHEN 'safe' THEN 3
                WHEN 'canonical' THEN 2
                ELSE 1
            END DESC,
            block_hash
        LIMIT 1
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

    replace_readable_path(&mut transaction, chain_id, &hashes, path_floor).await?;
    promote_to_canonical(&mut transaction, chain_id, &hashes).await?;
    if let Some(safe) = &heads.safe {
        promote_to_safe(&mut transaction, chain_id, &hashes, safe.number).await?;
    }
    if let Some(finalized) = &heads.finalized {
        promote_to_finalized(&mut transaction, chain_id, finalized.number).await?;
    }
    bigname_storage::invalidate_execution_outcomes_for_orphaned_blocks_in_transaction(
        &mut transaction,
    )
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to invalidate execution outcomes after head publication for chain \
             {chain_id}: {error:#}"
        ))
    })?;

    sqlx::query(
        "
        INSERT INTO chain_heads (
            chain_id,
            latest_block_hash,
            latest_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (chain_id) DO UPDATE
        SET latest_block_hash = EXCLUDED.latest_block_hash,
            latest_block_number = EXCLUDED.latest_block_number,
            safe_block_hash = EXCLUDED.safe_block_hash,
            safe_block_number = EXCLUDED.safe_block_number,
            finalized_block_hash = EXCLUDED.finalized_block_hash,
            finalized_block_number = EXCLUDED.finalized_block_number,
            updated_at = now()
        ",
    )
    .bind(chain_id)
    .bind(&heads.latest.hash)
    .bind(heads.latest.number)
    .bind(heads.safe.as_ref().map(|marker| marker.hash.as_str()))
    .bind(heads.safe.as_ref().map(|marker| marker.number))
    .bind(heads.finalized.as_ref().map(|marker| marker.hash.as_str()))
    .bind(heads.finalized.as_ref().map(|marker| marker.number))
    .execute(&mut *transaction)
    .await
    .map_err(|error| head_write_error("publish head markers", chain_id, error))?;

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
) -> RunnerResult<()> {
    sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut **transaction)
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
    sqlx::query(
        "
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = $1
          AND block_number >= $2
          AND canonicality_state IN ('canonical', 'safe')
          AND NOT (block_hash = ANY($3))
        ",
    )
    .bind(chain_id)
    .bind(path_floor)
    .bind(hashes)
    .execute(&mut **transaction)
    .await
    .map_err(|error| head_write_error("orphan displaced readable path", chain_id, error))?;
    Ok(())
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

fn head_write_error(action: &str, chain_id: &str, error: sqlx::Error) -> RunnerError {
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
