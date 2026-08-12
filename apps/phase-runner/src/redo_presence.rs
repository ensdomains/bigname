use crate::{
    config::{SourceConfig, normalized_source_kind},
    error::{RunnerError, RunnerResult},
    phase::BlockRange,
    redo_manifest_attestation::{AttestedManifestAuthority, ManifestAuthorityAttestation},
    transitions::PhaseStateRow,
};

pub(crate) fn interpret_replay_range(
    previous: &PhaseStateRow,
    requested: BlockRange,
) -> RunnerResult<BlockRange> {
    let to = previous.current_block_number.ok_or_else(|| {
        RunnerError::data_integrity("interpret redo cannot determine the recorded interpreted head")
    })?;
    BlockRange::new(requested.from, to)
}

pub(crate) async fn require_interpret_raw_data(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    chain_id: &str,
    sources: &[SourceConfig],
    range: BlockRange,
    recorded_input_hash: Option<&str>,
    supplied_manifest_authority_generation: Option<&str>,
) -> RunnerResult<Option<AttestedManifestAuthority>> {
    let supplied_generation = crate::redo_manifest_attestation::resolve_locked_generation(
        transaction,
        chain_id,
        recorded_input_hash,
        supplied_manifest_authority_generation,
    )
    .await?;
    let manifest_attestation = ManifestAuthorityAttestation::new(
        chain_id,
        recorded_input_hash,
        supplied_generation.as_deref(),
    )?
    .finish(chain_id, range)?;
    let expected_blocks = range
        .to
        .checked_sub(range.from)
        .and_then(|distance| distance.checked_add(1))
        .ok_or_else(|| RunnerError::data_integrity("interpret redo range length overflowed"))?;
    let (canonical_rows, canonical_blocks): (i64, i64) = sqlx::query_as(
        "
        SELECT count(*), count(DISTINCT block_number)
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain_id)
    .bind(range.from)
    .bind(range.to)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to check raw-data presence for interpret redo on chain {chain_id}"),
            error,
        )
    })?;
    if canonical_rows != canonical_blocks {
        return Err(RunnerError::data_integrity(format!(
            "raw-data presence check failed for interpret redo on chain {chain_id}: live lineage has multiple hashes at one height in range {}..={}",
            range.from, range.to
        )));
    }
    if canonical_blocks != expected_blocks {
        return Err(RunnerError::data_integrity(format!(
            "raw-data presence check failed for interpret redo on chain {chain_id}: range {}..={} \
             has {canonical_blocks} canonical lineage blocks, expected {expected_blocks}",
            range.from, range.to
        )));
    }
    if sources.is_empty() {
        return Err(RunnerError::data_integrity(format!(
            "raw-data presence check failed for interpret redo on chain {chain_id}: no configured \
             ingest sources can prove range {}..={}",
            range.from, range.to,
        )));
    }

    for source in sources {
        if source.start_block_number > range.to {
            continue;
        }
        let cursor: Option<(String, String, i64, i64, Option<i64>)> = sqlx::query_as(
            "
            SELECT source_kind, seed_basis, start_block_number, next_block_number,
                   target_block_number
            FROM ingest_cursors
            WHERE chain_id = $1
              AND source_key = $2
            ",
        )
        .bind(chain_id)
        .bind(&source.source_key)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to check raw-data presence for source {} on chain {chain_id}",
                    source.source_key
                ),
                error,
            )
        })?;
        let (source_kind, seed_basis, start, next, target) = cursor.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "raw-data presence check failed for interpret redo on chain {chain_id}: configured \
                 ingest source {} has no cursor covering {}..={}",
                source.source_key,
                source.start_block_number.max(range.from),
                range.to,
            ))
        })?;
        if normalized_source_kind(&source_kind) != normalized_source_kind(&source.source_kind)
            || seed_basis != source.seed_basis.as_str()
            || start != source.start_block_number
        {
            return Err(RunnerError::data_integrity(format!(
                "raw-data presence check failed for interpret redo on chain {chain_id}: ingest \
                 cursor {} does not match the configured source",
                source.source_key
            )));
        }
        let required_from = start.max(range.from);
        let required_to = target.map_or(range.to, |target| target.min(range.to));
        if required_from <= required_to && next <= required_to {
            return Err(RunnerError::data_integrity(format!(
                "raw-data presence check failed for interpret redo on chain {chain_id}: ingest \
                 cursor {} covers through {}, not required source range {required_from}..={}",
                source.source_key,
                next.saturating_sub(1),
                required_to,
            )));
        }
    }
    Ok(manifest_attestation)
}
