use sqlx::PgPool;

use crate::{IngestError, Marker, REDO_BOUNDARY_DIVERGENCE_PREFIX, Result};

pub(super) fn must_reload_completed_source_boundary(
    completing: bool,
    range_from: i64,
    range_to: i64,
    resume_current: Option<&Marker>,
    source_target_number: i64,
) -> bool {
    completing
        && source_target_number >= range_from
        && source_target_number < range_to
        && resume_current.is_some_and(|resume| resume.number >= source_target_number)
}

pub(super) fn require_loaded_boundary(
    chain_id: &str,
    loaded: &Marker,
    pre_load_target: &Marker,
) -> Result<()> {
    if loaded == pre_load_target {
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {}: loaded boundary hash {}, pre-load target hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
        pre_load_target.number, loaded.hash, pre_load_target.hash
    )))
}

pub(super) async fn reject_lineage_backed_boundary_change(
    pool: &PgPool,
    chain_id: &str,
    resume_current: Option<&Marker>,
    fresh_target: &Marker,
) -> Result<()> {
    let Some(durable) = resume_current else {
        return Ok(());
    };
    if durable.number != fresh_target.number || durable.hash == fresh_target.hash {
        return Ok(());
    }
    let has_lineage: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM chain_lineage
             WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
         )",
    )
    .bind(chain_id)
    .bind(fresh_target.number)
    .bind(&fresh_target.hash)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!(
                "failed to check boundary lineage while resuming Ingest redo for chain {chain_id}"
            ),
            error,
        )
    })?;
    if !has_lineage {
        // Cursor reconciliation independently refuses hashes absent from retained lineage.
        return Ok(());
    }
    Err(IngestError::data_integrity(format!(
        "{REDO_BOUNDARY_DIVERGENCE_PREFIX} for chain {chain_id} at block {}: durable redo hash {}, freshly observed hash {}; rerun the Ingest redo so it starts fresh and reloads this boundary under the current watch plan",
        durable.number, durable.hash, fresh_target.hash
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(number: i64) -> Marker {
        Marker {
            number,
            hash: format!("hash-{number}"),
        }
    }

    #[test]
    fn final_batch_reloads_an_in_range_source_boundary_covered_by_an_earlier_batch() {
        let resume = marker(300);

        assert!(must_reload_completed_source_boundary(
            true,
            0,
            400,
            Some(&resume),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            false,
            0,
            400,
            Some(&resume),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            true,
            200,
            400,
            Some(&resume),
            100
        ));
        assert!(must_reload_completed_source_boundary(
            true,
            0,
            200,
            Some(&marker(100)),
            100
        ));
        assert!(!must_reload_completed_source_boundary(
            true,
            0,
            100,
            Some(&marker(100)),
            100
        ));
    }
}
