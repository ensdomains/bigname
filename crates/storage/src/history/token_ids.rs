//! ENSv2 token words at each history row's physical event position.
use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::U256;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

use super::HistoryEvent;
use crate::CanonicalityState;

/// Direct registration/transfer words work before linkage and after release. Permission rows
/// without a recorded word use a bounded predecessor only for manifest models whose regeneration
/// marker precedes mint callbacks. Unknown and pre-audit models remain unproven and omit it.
/// The caller supplies the page's publication bounds and read snapshot, then rechecks its redo
/// fence. Results are keyed by event, since a page can hold many versions of the same resource.
pub async fn load_history_token_ids(
    db: impl Into<crate::ReadDb<'_>>,
    rows: &[HistoryEvent],
    block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<i64, String>> {
    let mut tokens = BTreeMap::new();
    let mut targets = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        if !eligible(row, block_bounds) || !seen.insert(row.normalized_event_id) {
            continue;
        }
        let word = row
            .after_state
            .get("current_token_id")
            .filter(|value| !value.is_null())
            .or_else(|| {
                row.after_state
                    .get("token_id")
                    .filter(|value| !value.is_null())
            });
        if let Some(word) = word {
            let word = word
                .as_str()
                .context("recorded history token is not a string")?;
            tokens.insert(row.normalized_event_id, decimal_token(word)?);
        } else if row.event_kind == "PermissionChanged"
            && row.resource_id.is_some()
            && row.transaction_index.is_some()
            && row.log_index.is_some()
            && row.block_hash.is_some()
        {
            targets.push(json!({
                "event_id": row.normalized_event_id,
                "chain_id": row.chain_id,
                "resource_id": row.resource_id,
                "block_number": row.block_number,
                "block_hash": row.block_hash,
                "transaction_index": row.transaction_index,
                "log_index": row.log_index,
            }));
        }
    }
    if targets.is_empty() {
        return Ok(tokens);
    }
    let mut conn = db.into().acquire().await?;
    let bounds = serde_json::to_value(block_bounds)?;
    for targets in targets.chunks(200) {
        let values: Vec<(i64, Option<Value>)> = sqlx::query_as(TOKEN_IDS_SQL)
            .bind(Value::Array(targets.to_vec()))
            .bind(&bounds)
            .fetch_all(&mut *conn)
            .await
            .context("failed to load history token evidence")?;
        for (event, word) in values {
            let word = word.context("selected history token event has no token ID")?;
            let word = word
                .as_str()
                .context("selected history token is not a string")?;
            tokens.insert(event, decimal_token(word)?);
        }
    }
    Ok(tokens)
}

fn eligible(row: &HistoryEvent, bounds: &BTreeMap<String, i64>) -> bool {
    matches!(
        row.source_family.as_str(),
        "ens_v2_registry_l1" | "ens_v2_root_l1"
    ) && matches!(
        row.event_kind.as_str(),
        "RegistrationGranted" | "LabelRegistered" | "TokenControlTransferred" | "PermissionChanged"
    ) && row.consumer_visibility == "activated"
        && matches!(
            row.canonicality_state,
            CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
        )
        && row
            .chain_id
            .as_ref()
            .and_then(|chain| bounds.get(chain))
            .zip(row.block_number)
            .is_some_and(|(bound, block)| block <= *bound)
}

fn decimal_token(word: &str) -> Result<String> {
    let token = if let Some(hex) = word.strip_prefix("0x") {
        ensure!(
            !hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid recorded history token ID"
        );
        U256::from_str_radix(hex, 16)
    } else {
        ensure!(
            !word.is_empty() && word.bytes().all(|byte| byte.is_ascii_digit()),
            "invalid recorded history token ID"
        );
        U256::from_str_radix(word, 10)
    }
    .context("invalid recorded history token ID")?;
    Ok(token.to_string())
}

// EAC emits before the regeneration callback; current and June post-audit registries emit the
// token marker before mint. Pre-audit mint callbacks precede that marker and cannot use this rule.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L280-L287 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L577-L587 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registry/PermissionedRegistry.sol:L529-L538 @ ens_v2_sepolia_20260629@ccaeb58b)
const TOKEN_IDS_SQL: &str = "/* storage:history.token_ids */
    SELECT target.normalized_event_id, token.token_id
    FROM jsonb_to_recordset($1::jsonb) requested(event_id bigint, chain_id text,
        resource_id uuid, block_number bigint, block_hash text, transaction_index bigint, log_index bigint)
    JOIN bigname_phase.normalized_events target ON target.normalized_event_id = requested.event_id
      AND target.chain_id = requested.chain_id AND target.resource_id = requested.resource_id
      AND target.block_number = requested.block_number AND target.block_hash = requested.block_hash
      AND target.transaction_index = requested.transaction_index AND target.log_index = requested.log_index
    JOIN jsonb_each_text($2::jsonb) bound ON bound.key = target.chain_id
    JOIN bigname_phase.manifest_versions manifest ON manifest.manifest_id = target.source_manifest_id
      AND manifest.chain_id = target.chain_id AND manifest.source_family = target.source_family
      AND manifest.manifest_version = target.manifest_version
      AND manifest.deployment_label IN ('ens_v2_sepolia_20261001', 'ens_v2_sepolia_post_audit')
    JOIN bigname_phase.chain_lineage target_lineage
      ON target_lineage.chain_id = target.chain_id AND target_lineage.block_hash = target.block_hash
     AND target_lineage.block_number = target.block_number
     AND target_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    JOIN LATERAL (
        SELECT CASE event.event_kind WHEN 'TokenRegenerated' THEN event.after_state -> 'new_token_id'
               ELSE COALESCE(NULLIF(event.after_state -> 'current_token_id', 'null'::jsonb), event.after_state -> 'token_id') END token_id
        FROM bigname_phase.normalized_events event
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
         AND lineage.block_number = event.block_number
        WHERE event.chain_id = target.chain_id AND event.resource_id = target.resource_id
          AND (event.block_number, event.transaction_index, event.log_index)
              <= (target.block_number, target.transaction_index, target.log_index)
          AND event.block_number <= bound.value::bigint
          AND (event.block_number < target.block_number OR event.block_hash = target.block_hash)
          AND event.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
          AND event.event_kind IN ('TokenResourceLinked', 'TokenRegenerated')
          AND event.transaction_index IS NOT NULL AND event.log_index IS NOT NULL
          AND event.consumer_visibility = 'activated'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY event.block_number DESC, event.transaction_index DESC, event.log_index DESC LIMIT 1
    ) token ON TRUE
    WHERE target.event_kind = 'PermissionChanged'
      AND target.source_family IN ('ens_v2_registry_l1', 'ens_v2_root_l1')
      AND target.block_number <= bound.value::bigint
      AND target.consumer_visibility = 'activated'
      AND target.canonicality_state IN ('canonical', 'safe', 'finalized')";

#[cfg(test)]
mod tests;
