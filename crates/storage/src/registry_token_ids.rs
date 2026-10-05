//! Current ENSv2 token words from the recorded registration and regeneration events.
use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;
use anyhow::{Context, Result};
use uuid::Uuid;

/// Latest recorded token per registration resource, bounded by both the requested chain
/// position and Project's publication. Token words belong to these events, not the stable
/// resource UUID or its EAC resource word. Each lateral probe uses the selective registry-token index
/// and returns at most one row; it never loads a resource's history into memory.
pub async fn load_ens_v2_token_ids(
    db: impl Into<crate::ReadDb<'_>>,
    resource_ids: &[Uuid],
    block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<Uuid, String>> {
    if resource_ids.is_empty() || block_bounds.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut conn = db.into().acquire().await?;
    let resources = resource_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let bounds = serde_json::to_value(block_bounds)?;
    let mut tokens = BTreeMap::new();
    for resources in resources.chunks(200) {
        let rows: Vec<(Uuid, Option<String>)> = sqlx::query_as(TOKEN_IDS_SQL)
            .bind(resources)
            .bind(&bounds)
            .fetch_all(&mut *conn)
            .await
            .context("failed to read published ENSv2 token IDs")?;
        for (resource, word) in rows {
            let word = word.with_context(|| {
                format!("latest ENSv2 token event has no token ID for {resource}")
            })?;
            let token = if let Some(hex) = word.strip_prefix("0x") {
                U256::from_str_radix(hex, 16)
            } else {
                U256::from_str_radix(&word, 10)
            }
            .with_context(|| format!("invalid recorded ENSv2 token ID for {resource}"))?;
            tokens.insert(resource, token.to_string());
        }
    }
    Ok(tokens)
}

const TOKEN_IDS_SQL: &str = "/* storage:normalized_events.registry_tokens */
    SELECT resource.resource_id, token.token_id
    FROM unnest($1::uuid[]) requested(resource_id)
    JOIN bigname_phase.resources resource USING (resource_id)
    JOIN jsonb_each_text($2::jsonb) bound ON bound.key = resource.chain_id
    JOIN bigname_phase.project_family_marker marker ON marker.chain_id = resource.chain_id
    JOIN LATERAL (
        SELECT CASE event.event_kind
                 WHEN 'TokenRegenerated' THEN event.after_state ->> 'new_token_id'
                 ELSE COALESCE(event.after_state ->> 'current_token_id',
                               event.after_state ->> 'token_id')
               END AS token_id
        FROM bigname_phase.normalized_events event
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
         AND lineage.block_number = event.block_number
        WHERE event.resource_id = resource.resource_id
          AND event.chain_id = resource.chain_id
          AND event.block_number <= LEAST(bound.value::bigint, marker.current_block_number)
          AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
          AND event.event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
          AND event.transaction_index IS NOT NULL AND event.log_index IS NOT NULL
          AND event.consumer_visibility = 'activated'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY event.block_number DESC, event.transaction_index DESC,
                 event.log_index DESC
        LIMIT 1
    ) token ON TRUE";

#[cfg(test)]
mod tests;
