//! Page-local evidence for an old-registry to current-registry ownership handoff.
//! No row is created and no collection membership is changed by this enrichment.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::HistoryEvent;
use crate::CanonicalityState;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryRegistryHandoff {
    pub from_registry: String,
    pub to_registry: String,
    pub old_event_id: i64,
}

/// Returns evidence only for the first original readable direct current-registry owner row of
/// a non-root node, with an earlier old owner and no earlier ownership fallback clear. Probes
/// use exact chain/node indexes and the supplied publication bounds. The caller owns the read
/// snapshot and redo fence, as for the other page context helpers.
pub async fn load_history_registry_handoffs(
    connection: &mut PgConnection,
    rows: &[HistoryEvent],
    bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<i64, HistoryRegistryHandoff>> {
    let targets: Vec<_> = rows.iter().filter_map(|row| target(row, bounds)).collect();
    let mut handoffs = BTreeMap::new();
    if targets.is_empty() {
        return Ok(handoffs);
    }
    let query = handoff_sql();
    for chunk in targets.chunks(200) {
        let evidence: Vec<(i64, i64, String, String)> = sqlx::query_as(&query)
            .bind(Value::Array(chunk.to_vec()))
            .fetch_all(&mut *connection)
            .await
            .context("failed to load registry handoff history evidence")?;
        for (event_id, old_event_id, from_registry, to_registry) in evidence {
            handoffs.insert(
                event_id,
                HistoryRegistryHandoff {
                    from_registry,
                    to_registry,
                    old_event_id,
                },
            );
        }
    }
    Ok(handoffs)
}

fn target(row: &HistoryEvent, bounds: &BTreeMap<String, i64>) -> Option<Value> {
    let body = &row.after_state;
    if row.source_family != "ens_v1_registry_l1"
        || row.consumer_visibility != "activated"
        || !matches!(
            row.canonicality_state,
            CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
        )
        || body.get("emitter_role")?.as_str()? != "registry"
        // Direct owner observations always carry both keys, including present-null values.
        // Reconciliation strips them when retargeting a row whose original role is uncertain.
        || body.get("authority_kind").is_none()
        || body.get("authority_key").is_none()
    {
        return None;
    }
    let node = match (row.event_kind.as_str(), body.get("source_event")?.as_str()?) {
        ("SubregistryChanged", "NewOwner") => body.get("child_node")?.as_str()?,
        ("AuthorityTransferred", "Transfer") => body.get("node")?.as_str()?,
        _ => return None,
    };
    if node.len() != 66
        || !node.starts_with("0x")
        || !node[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        || node[2..].bytes().all(|byte| byte == b'0')
    {
        return None;
    }
    let chain = row.chain_id.as_deref()?;
    let bound = *bounds.get(chain)?;
    if row.block_number? > bound
        || row.block_hash.is_none()
        || row.transaction_index.is_none()
        || row.log_index.is_none()
    {
        return None;
    }
    Some(json!({"event_id":row.normalized_event_id, "chain":chain,
        "name_key":format!("{}:{}", row.namespace, node.to_ascii_lowercase()), "bound":bound}))
}

fn owner_probe(role: &str, earlier: bool) -> String {
    // Retargeted rows remain observations here: rejecting them as enrichment targets must
    // not let a later ordinary transfer inherit the missing original handoff's marker.
    let mut arms = Vec::new();
    for (kind, source, field) in [
        ("SubregistryChanged", "NewOwner", "child_node"),
        ("AuthorityTransferred", "Transfer", "node"),
    ] {
        arms.push(format!(r#"SELECT observed.* FROM bigname_phase.normalized_events observed
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = observed.chain_id AND lineage.block_hash = observed.block_hash
            WHERE observed.chain_id = target.chain
              AND (observed.namespace || ':' || lower(observed.after_state ->> '{field}')) = target.name_key
              AND observed.event_kind = '{kind}' AND observed.source_family = 'ens_v1_registry_l1'
              AND observed.after_state ->> 'source_event' = '{source}'
              AND observed.after_state ->> 'emitter_role' = '{role}'
              AND observed.consumer_visibility = 'activated'
              AND observed.canonicality_state IN ('canonical','safe','finalized')
              AND lineage.canonicality_state IN ('canonical','safe','finalized')
              AND observed.block_number <= target.bound
              {earlier}
            "#, earlier = if earlier {
                "AND observed.transaction_index IS NOT NULL AND observed.log_index IS NOT NULL
                 AND ROW(observed.block_number, observed.transaction_index, observed.log_index)
                     < ROW(current_owner.block_number, current_owner.transaction_index, current_owner.log_index)"
            } else { "" }));
    }
    arms.join(" UNION ALL ")
}

pub(super) fn handoff_sql() -> String {
    format!(
        r#"SELECT target.event_id, old_owner.normalized_event_id,
            lower(old_owner.raw_fact_ref ->> 'emitting_address'),
            lower(current_owner.raw_fact_ref ->> 'emitting_address')
        FROM jsonb_to_recordset($1::jsonb) AS target(event_id bigint, chain text, name_key text, bound bigint)
        CROSS JOIN LATERAL (
            SELECT * FROM ({current}) observations
            ORDER BY block_number, transaction_index NULLS FIRST, log_index NULLS FIRST, event_identity
            LIMIT 1
        ) current_owner
        CROSS JOIN LATERAL (
            SELECT * FROM ({old}) observations
            ORDER BY block_number DESC, transaction_index DESC, log_index DESC, event_identity DESC
            LIMIT 1
        ) old_owner
        WHERE current_owner.normalized_event_id = target.event_id
          AND current_owner.transaction_index IS NOT NULL AND current_owner.log_index IS NOT NULL
          AND current_owner.raw_fact_ref ->> 'emitting_address' ~ '^0x[0-9a-fA-F]{{40}}$'
          AND old_owner.raw_fact_ref ->> 'emitting_address' ~ '^0x[0-9a-fA-F]{{40}}$'
          AND lower(current_owner.raw_fact_ref ->> 'emitting_address') <> lower(old_owner.raw_fact_ref ->> 'emitting_address')
          AND NOT EXISTS (
            SELECT 1 FROM bigname_phase.normalized_events cleared
            JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = cleared.chain_id AND lineage.block_hash = cleared.block_hash
            WHERE cleared.chain_id = target.chain
              AND COALESCE(cleared.namespace || ':' || lower(COALESCE(
                    cleared.after_state ->> 'child_node', cleared.after_state ->> 'namehash',
                    cleared.after_state ->> 'node', cleared.after_state #>> '{{grant_source,node}}',
                    cleared.after_state #>> '{{revocation_source,node}}')), cleared.logical_name_id) = target.name_key
              AND cleared.source_family LIKE 'ens\_v1\_%'
              AND cleared.source_family = 'ens_v1_registry_l1'
              AND cleared.event_kind = 'ResolverChanged'
              AND cleared.after_state -> 'registry_fallback_handoff' = 'true'::jsonb
              AND cleared.after_state ->> 'source_event' IN ('NewOwner','Transfer')
              AND cleared.after_state ->> 'emitter_role' = 'registry'
              AND lower(cleared.raw_fact_ref ->> 'emitting_address') = lower(current_owner.raw_fact_ref ->> 'emitting_address')
              AND cleared.consumer_visibility = 'activated'
              AND cleared.canonicality_state IN ('canonical','safe','finalized')
              AND lineage.canonicality_state IN ('canonical','safe','finalized')
              AND cleared.block_number <= target.bound
              AND cleared.transaction_index IS NOT NULL AND cleared.log_index IS NOT NULL
              AND ROW(cleared.block_number, cleared.transaction_index, cleared.log_index)
                  < ROW(current_owner.block_number, current_owner.transaction_index, current_owner.log_index)
          )
    "#,
        current = owner_probe("registry", false),
        old = owner_probe("registry_old", true)
    )
}

#[cfg(test)]
#[path = "registry_handoffs_tests.rs"]
mod tests;
