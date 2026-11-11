//! Prepare the label locks before redo mutates identity anchors or raw witnesses.
use crate::{InterpretError, Result};
use bigname_adapters::schema_v2::{BatchOutput, seam::PREIMAGE_OBSERVATION_EVENT_KIND};
use sqlx::{Postgres, Transaction};

pub(super) fn failure(error: anyhow::Error) -> InterpretError {
    InterpretError::data_integrity(format!("identity search derivation failed: {error:#}"))
}

/// A superset of the exact repair targets: range anchors, raw evidence without a readable
/// witness, and range preimage observations. Each arm reads its own index. An event names a
/// surface of its own chain through a foreign key, so the last arm needs no surface probe.
pub(super) fn redo_names() -> String {
    format!(
        "/* interpret:identity_search.redo_names */
         SELECT anchored.logical_name_id
         FROM name_surfaces anchored
         WHERE anchored.chain_id = $1
           AND anchored.block_number BETWEEN $2 AND $3
         UNION
         {lost}
         UNION
         SELECT event.logical_name_id
         FROM normalized_events event
         WHERE event.chain_id = $1
           AND event.block_number BETWEEN $2 AND $3
           AND event.logical_name_id IS NOT NULL
           AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
         ORDER BY logical_name_id",
        lost = super::witness::lost_witnesses(),
    )
}

pub(super) async fn prepare_redo(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    range: (i64, i64),
    output: &BatchOutput,
) -> Result<Vec<String>> {
    let mut names: Vec<String> = sqlx::query_scalar(&redo_names())
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
    bigname_storage::identity_search::prepare(
        transaction,
        bigname_storage::identity_search::LabelWrite::Insert(&changed),
        &paths,
        &names,
    )
    .await
    .map_err(failure)?;
    Ok(names)
}
