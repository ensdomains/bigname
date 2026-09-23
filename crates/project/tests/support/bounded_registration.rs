//! Drift guard between the registration Project selects for a name and the bounded registration
//! set history follows.
//!
//! Name history no longer adds `name_current.declared_summary.registration.resource_id` to the
//! resources it reads: that field is current state. It follows only the bindings, `NameWrapped`
//! links and registrar grants recorded at or below the read's bound
//! (`bigname_storage::load_bounded_registration_resource_ids`). At a row's own publication the
//! selected registration must be one of them. Every Project run in the including test files ends
//! with this check, so a producer change that selects a registration the bounded readers cannot
//! reach fails here instead of silently dropping that lease's rows from history.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// For every published name with a selected registration, the bounded registration set at the
/// row's publication contains it. Returns how many rows were checked.
pub async fn assert_selected_registrations_are_bounded(pool: &PgPool) -> Result<usize> {
    let rows = sqlx::query(
        "SELECT logical_name_id, declared_summary, chain_positions
         FROM bigname_phase.name_current
         WHERE declared_summary #>> '{registration,resource_id}' IS NOT NULL
         ORDER BY logical_name_id",
    )
    .fetch_all(pool)
    .await?;
    let mut mismatches = Vec::new();
    for row in &rows {
        let logical_name_id: String = row.try_get("logical_name_id")?;
        let summary: Value = row.try_get("declared_summary")?;
        let chain_positions: Value = row.try_get("chain_positions")?;
        let selected = summary["registration"]["resource_id"]
            .as_str()
            .and_then(|id| Uuid::parse_str(id).ok())
            .with_context(|| format!("{logical_name_id} has an invalid registration id"))?;
        let bound = chain_positions
            .as_object()
            .into_iter()
            .flatten()
            .map(|(key, position)| {
                let chain_id = position["chain_id"]
                    .as_str()
                    .with_context(|| format!("{logical_name_id} position {key} has no chain_id"))?;
                let block = position["block_number"].as_i64().with_context(|| {
                    format!("{logical_name_id} position {key} has no block_number")
                })?;
                Ok((chain_id.to_owned(), block))
            })
            .collect::<Result<BTreeMap<String, i64>>>()?;
        let bounded =
            bigname_storage::load_bounded_registration_resource_ids(pool, &logical_name_id, &bound)
                .await?;
        if !bounded.contains(&selected) {
            mismatches.push(format!(
                "{logical_name_id} at {bound:?}: selected {selected}, bounded set {bounded:?}"
            ));
        }
    }
    ensure!(
        mismatches.is_empty(),
        "Project selected registrations the bounded history readers do not reach:\n{}",
        mismatches.join("\n")
    );
    Ok(rows.len())
}
