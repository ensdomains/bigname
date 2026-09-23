//! The block at which a history read's resolver classification may next change.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// Per chain of `block_bounds`, the lowest declaration `start_block` above that chain's bound
/// across every manifest Project stages for the chain, or no entry when none starts above it.
///
/// Project recomputes resolver classification against its target block, choosing among a
/// manifest's declarations by `start_block`, so a declaration that starts above the bound can
/// change which history events a bounded read attributes once Project reaches it, with no
/// manifest change (bigname: `crates/project/src/stage.rs:122-161`,
/// `crates/project/src/builders/resolver.rs:343-387`). Every declaration of every readable
/// manifest update counts, not only resolver ones, so the horizon errs early. Project stages
/// the Basenames execution manifest of `ethereum-mainnet` for `base-mainnet` too
/// (bigname: `crates/project/src/stage.rs:92-99`).
pub async fn load_classification_horizons(
    pool: &PgPool,
    block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<String, i64>> {
    let (chain_ids, blocks): (Vec<String>, Vec<i64>) = block_bounds
        .iter()
        .map(|(chain_id, block)| (chain_id.clone(), *block))
        .unzip();
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT bound.chain_id, MIN((declaration ->> 'start_block')::bigint)
         FROM unnest($1::text[], $2::bigint[]) AS bound(chain_id, block_number)
         JOIN bigname_phase.normalized_events event
           ON event.chain_id = bound.chain_id
           OR (
               bound.chain_id = 'base-mainnet'
               AND event.namespace = 'basenames'
               AND event.source_family = 'basenames_execution'
               AND event.chain_id = 'ethereum-mainnet'
           )
         CROSS JOIN LATERAL jsonb_array_elements(
             CASE jsonb_typeof(event.after_state -> 'manifest_payload' -> 'contracts')
                 WHEN 'array' THEN event.after_state -> 'manifest_payload' -> 'contracts'
                 ELSE '[]'::jsonb
             END
         ) AS declarations(declaration)
         WHERE event.event_kind = 'SourceManifestUpdated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND jsonb_typeof(declaration -> 'start_block') = 'number'
           AND (declaration ->> 'start_block')::bigint > bound.block_number
         GROUP BY bound.chain_id",
    )
    .bind(&chain_ids)
    .bind(&blocks)
    .fetch_all(pool)
    .await
    .context("failed to load resolver classification horizons")?;
    Ok(rows.into_iter().collect())
}
