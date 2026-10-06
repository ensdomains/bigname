//! Prepare the label locks before redo mutates identity anchors or raw witnesses.
use crate::{InterpretError, Result};
use bigname_adapters::schema_v2::{BatchOutput, seam::PREIMAGE_OBSERVATION_EVENT_KIND};
use sqlx::{Postgres, Transaction};

pub(super) fn failure(error: anyhow::Error) -> InterpretError {
    InterpretError::data_integrity(format!("identity search derivation failed: {error:#}"))
}

pub(super) async fn prepare_redo(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    range: (i64, i64),
    output: &BatchOutput,
) -> Result<Vec<String>> {
    // This includes the exact repair target's superset: range anchors, range observations,
    // released witnesses, and raw evidence whose witness has lost readable lineage.
    let statement = format!(
        "/* interpret:identity_search.redo_names */ SELECT logical_name_id FROM name_surfaces surface
         WHERE surface.chain_id=$1 AND (
             surface.block_number BETWEEN $2 AND $3
             OR (surface.raw_name IS NOT NULL AND (
                 surface.preimage_event_identity IS NULL OR NOT EXISTS (
                     SELECT 1 FROM normalized_events witness JOIN chain_lineage lineage
                       ON lineage.chain_id=witness.chain_id AND lineage.block_hash=witness.block_hash
                      AND lineage.block_number=witness.block_number
                     WHERE witness.chain_id=surface.chain_id
                       AND witness.event_identity=surface.preimage_event_identity
                       AND witness.canonicality_state IN ('canonical','safe','finalized')
                       AND lineage.canonicality_state IN ('canonical','safe','finalized'))))
             OR EXISTS (SELECT 1 FROM normalized_events event
                WHERE event.chain_id=$1 AND event.block_number BETWEEN $2 AND $3
                  AND event.logical_name_id=surface.logical_name_id
                  AND event.event_kind='{PREIMAGE_OBSERVATION_EVENT_KIND}'))
         ORDER BY logical_name_id",
    );
    let mut names: Vec<String> = sqlx::query_scalar(&statement)
        .bind(chain_id)
        .bind(range.0)
        .bind(range.1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to collect redo search identities", error)
        })?;
    names.extend(
        output
            .name_surfaces
            .iter()
            .map(|surface| surface.logical_name_id.clone()),
    );
    names.sort_unstable();
    names.dedup();
    let changed: Vec<String> = output
        .label_preimages
        .iter()
        .filter(|label| !label.raw_label.is_empty())
        .map(|label| label.labelhash.clone())
        .collect();
    let paths: Vec<Vec<String>> = output
        .name_surfaces
        .iter()
        .map(|surface| surface.labelhashes.clone())
        .collect();
    bigname_storage::identity_search::prepare(transaction, &changed, &paths, &names)
        .await
        .map_err(failure)?;
    Ok(names)
}
