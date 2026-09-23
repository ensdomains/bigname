//! Drift guard between Project's published record attribution and the bounded history reader.
//!
//! History no longer reads `record_inventory_current.provenance.attributed_event_ids`: it
//! evaluates the same pointer, record-link and write evidence at or below the block a read is
//! bound to (`crates/storage/src/history/attribution.rs`). At a row's own publication the two must
//! agree exactly. Every Project run in the including test files ends with this check, so a change
//! to the producer's attribution that the reader does not follow fails here instead of silently
//! changing which writes history lists.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// For every published record inventory row, the bounded reader at the row's target block returns
/// exactly the row's `provenance.attributed_event_ids`.
pub async fn assert_bounded_record_attribution_matches_inventory(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query(
        "SELECT resource_id, provenance, chain_positions
         FROM bigname_phase.record_inventory_current
         ORDER BY resource_id",
    )
    .fetch_all(pool)
    .await?;
    let mut mismatches = Vec::new();
    for row in rows {
        let resource_id: Uuid = row.try_get("resource_id")?;
        let provenance: Value = row.try_get("provenance")?;
        let chain_positions: Value = row.try_get("chain_positions")?;
        let chain_id = provenance["chain_id"]
            .as_str()
            .with_context(|| format!("inventory {resource_id} has no provenance.chain_id"))?;
        let target = chain_positions["target_block_number"]
            .as_i64()
            .with_context(|| format!("inventory {resource_id} has no target block"))?;
        let published = provenance["attributed_event_ids"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|id| {
                id.as_i64()
                    .or_else(|| id.as_str().and_then(|id| id.parse().ok()))
                    .with_context(|| format!("inventory {resource_id} attributes {id}"))
            })
            .collect::<Result<BTreeSet<i64>>>()?;
        let bound = BTreeMap::from([(chain_id.to_owned(), target)]);
        let bounded =
            bigname_storage::load_bounded_record_attribution(pool, &[resource_id], Some(&bound))
                .await?
                .remove(&resource_id)
                .unwrap_or_default();
        if bounded != published {
            mismatches.push(format!(
                "{resource_id} at {chain_id}@{target}: published {published:?}, bounded reader \
                 {bounded:?}"
            ));
        }
    }
    ensure!(
        mismatches.is_empty(),
        "the bounded history attribution drifted from Project's attributed_event_ids:\n{}",
        mismatches.join("\n")
    );
    Ok(())
}
