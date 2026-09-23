//! The block at which a history read's resolver classification may next change.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::PgPool;

/// Per chain of `block_bounds`, the lowest declaration `start_block` above that chain's bound
/// across the manifests Project stages for the chain, or no entry when none starts above it.
///
/// Project recomputes resolver classification against its target block, choosing among a
/// manifest's declarations by `start_block`, so a declaration that starts above the bound can
/// change which history events a bounded read attributes once Project reaches it, with no
/// manifest change (bigname: `crates/project/src/stage.rs:123-170`,
/// `crates/project/src/builders/resolver/build.sql:307-351`). The manifests are the ones Project
/// stages at the bound: the latest readable `SourceManifestUpdated` event of each manifest at or
/// below it, when it is active and carries a payload, on the chain or, for `base-mainnet`, the
/// Basenames execution manifest of `ethereum-mainnet`
/// (bigname: `crates/project/src/stage.rs:66-121`). Every declaration of a staged manifest
/// counts, not only resolver ones, so the horizon errs early.
///
/// The manifest digest a cursor binds names only finalized manifest events, while Project and
/// this read also take canonical ones. Manifest sync writes every event finalized
/// (bigname: `crates/manifests/src/schema_v2_event_history.rs:170-179`) and no other non-test
/// code writes them, so that difference has no effect today.
pub async fn load_classification_horizons(
    pool: &PgPool,
    block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<String, i64>> {
    let (chain_ids, blocks): (Vec<String>, Vec<i64>) = block_bounds
        .iter()
        .map(|(chain_id, block)| (chain_id.clone(), *block))
        .unzip();
    let rows = sqlx::query_as::<_, (String, i64)>(
        "WITH bound AS (
             SELECT * FROM unnest($1::text[], $2::bigint[]) AS bound(chain_id, block_number)
         ),
         latest AS (
             SELECT DISTINCT ON (bound.chain_id, event.source_manifest_id)
                    bound.chain_id,
                    bound.block_number,
                    event.after_state ->> 'rollout_status' AS rollout_status,
                    event.after_state -> 'manifest_payload' AS manifest_payload
             FROM bound
             JOIN bigname_phase.normalized_events event
               ON event.chain_id = bound.chain_id
               OR (
                   bound.chain_id = 'base-mainnet'
                   AND event.namespace = 'basenames'
                   AND event.source_family = 'basenames_execution'
                   AND event.chain_id = 'ethereum-mainnet'
               )
             LEFT JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE event.event_kind = 'SourceManifestUpdated'
               AND event.source_manifest_id IS NOT NULL
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (
                   event.block_hash IS NULL
                   OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )
               AND (event.block_number IS NULL OR event.block_number <= bound.block_number)
             ORDER BY bound.chain_id, event.source_manifest_id, event.normalized_event_id DESC
         )
         SELECT latest.chain_id, MIN((declaration ->> 'start_block')::bigint)
         FROM latest
         CROSS JOIN LATERAL jsonb_array_elements(
             CASE jsonb_typeof(latest.manifest_payload -> 'contracts')
                 WHEN 'array' THEN latest.manifest_payload -> 'contracts'
                 ELSE '[]'::jsonb
             END
         ) AS declarations(declaration)
         WHERE latest.rollout_status = 'active'
           AND latest.manifest_payload IS NOT NULL
           AND jsonb_typeof(declaration -> 'start_block') = 'number'
           AND (declaration ->> 'start_block')::bigint > latest.block_number
         GROUP BY latest.chain_id",
    )
    .bind(&chain_ids)
    .bind(&blocks)
    .fetch_all(pool)
    .await
    .context("failed to load resolver classification horizons")?;
    Ok(rows.into_iter().collect())
}
