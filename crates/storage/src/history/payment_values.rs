//! Unique same-transaction payment observations for already selected history rows.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::HistoryEvent;

/// Returns only small payment objects keyed by the selected history event. No stored row is
/// changed and no supporting observation becomes a product history row. Each lateral branch
/// retains at most two candidates, so ambiguous evidence omits rather than choosing a charge.
pub async fn load_history_payment_values(
    db: impl Into<crate::ReadDb<'_>>,
    rows: &[HistoryEvent],
    block_bounds: &BTreeMap<String, i64>,
) -> Result<BTreeMap<i64, Value>> {
    let mut seen = BTreeSet::new();
    let targets = rows.iter().filter(|row| {
        let registration = matches!(row.event_kind.as_str(), "RegistrationGranted" | "LabelRegistered")
            && matches!(row.source_family.as_str(), "ens_v2_registry_l1" | "ens_v2_root_l1")
            && row.after_state["source_event"] == "LabelRegistered"
            && !row.event_identity.contains(":RegistrationGranted:topology:");
        let renewal = row.event_kind == "RegistrationRenewed"
            && row.source_family == "ens_v1_registrar_l1"
            && row.chain_id.as_deref() == Some("ethereum-sepolia")
            && row.after_state["source_event"] == "NameRenewed"
            && row.after_state.get("cost").is_none_or(Value::is_null);
        (registration || renewal) && row.transaction_hash.is_some()
            && row.transaction_index.is_some() && row.log_index.is_some()
            && seen.insert(row.normalized_event_id)
    }).map(|row| json!({
        "event_id":row.normalized_event_id,"event_identity":row.event_identity,
        "chain_id":row.chain_id,"block_number":row.block_number,"block_hash":row.block_hash,
        "transaction_hash":row.transaction_hash,"transaction_index":row.transaction_index,"log_index":row.log_index,
    })).collect::<Vec<_>>();
    if targets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut conn = db.into().acquire().await?;
    let bounds = serde_json::to_value(block_bounds)?;
    let mut values = BTreeMap::new();
    for targets in targets.chunks(200) {
        let results: Vec<(i64, Value)> = sqlx::query_as(PAYMENT_VALUES_SQL)
            .bind(Value::Array(targets.to_vec()))
            .bind(&bounds)
            .fetch_all(&mut *conn)
            .await
            .context("failed to load history payment observations")?;
        values.extend(results);
    }
    Ok(values)
}

const PAYMENT_VALUES_SQL: &str = "/* storage:history.payment_values */
WITH page AS MATERIALIZED (
    SELECT target.*
    FROM jsonb_to_recordset($1::jsonb) requested(event_id bigint, event_identity text, chain_id text,
        block_number bigint, block_hash text, transaction_hash text, transaction_index bigint, log_index bigint)
    JOIN bigname_phase.normalized_events target ON target.normalized_event_id=requested.event_id
      AND target.event_identity=requested.event_identity AND target.chain_id=requested.chain_id
      AND target.block_number=requested.block_number AND target.block_hash=requested.block_hash
      AND target.transaction_hash=requested.transaction_hash AND target.transaction_index=requested.transaction_index
      AND target.log_index=requested.log_index
    JOIN jsonb_each_text($2::jsonb) bound ON bound.key=target.chain_id
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id=target.chain_id AND lineage.block_hash=target.block_hash
     AND lineage.block_number=target.block_number
     AND lineage.canonicality_state IN ('canonical','safe','finalized')
    WHERE target.block_number<=bound.value::bigint AND target.consumer_visibility='activated'
      AND target.canonicality_state IN ('canonical','safe','finalized')
)
SELECT target.normalized_event_id, payment.value
FROM page target
JOIN LATERAL (
    SELECT jsonb_agg(candidate.value)->0 AS value
    FROM (
        SELECT jsonb_strip_nulls(jsonb_build_object(
            'base',observed.after_state->'base','premium',observed.after_state->'premium',
            'payment_token',observed.after_state->'payment_token','referrer',observed.after_state->'referrer')) AS value
        FROM bigname_phase.normalized_events observed
        JOIN LATERAL (
            SELECT count(*) AS matches, bool_and(link.resource_id=observed.resource_id) AS agrees
            FROM (
                SELECT witness.resource_id FROM bigname_phase.normalized_events witness
                WHERE witness.chain_id=target.chain_id AND witness.block_hash=target.block_hash
                  AND witness.block_number=target.block_number AND witness.transaction_index=target.transaction_index
                  AND witness.transaction_hash=target.transaction_hash AND witness.namespace=target.namespace
                  AND witness.source_manifest_id=target.source_manifest_id
                  AND witness.source_family=target.source_family AND witness.event_kind='TokenResourceLinked'
                  AND witness.log_index>=target.log_index AND witness.log_index<observed.log_index
                  AND lower(witness.raw_fact_ref->>'emitting_address')=lower(target.raw_fact_ref->>'emitting_address')
                  AND lower(COALESCE(witness.after_state->>'current_token_id',witness.after_state->>'token_id'))
                      =lower(COALESCE(target.after_state->>'current_token_id',target.after_state->>'token_id'))
                  AND witness.consumer_visibility='activated'
                  AND witness.canonicality_state IN ('canonical','safe','finalized')
                LIMIT 2
            ) link
        ) linkage ON linkage.matches=1 AND linkage.agrees
        WHERE observed.chain_id=target.chain_id AND observed.block_hash=target.block_hash
          AND observed.block_number=target.block_number AND observed.transaction_index=target.transaction_index
          AND observed.transaction_hash=target.transaction_hash AND observed.namespace=target.namespace
          AND observed.source_family='ens_v2_registrar_l1' AND observed.event_kind='RegistrarNameRegistered'
          AND observed.after_state->>'source_event'='NameRegistered' AND observed.log_index>target.log_index
          AND lower(observed.after_state->>'token_id')
              =lower(COALESCE(target.after_state->>'current_token_id',target.after_state->>'token_id'))
          AND COALESCE(observed.logical_name_id,observed.namespace||':'||(observed.after_state->>'namehash'))
              =COALESCE(target.logical_name_id,target.namespace||':'||(target.after_state->>'namehash'))
          AND lower(target.after_state->>'sender')=lower(observed.raw_fact_ref->>'emitting_address')
          AND observed.resource_id IS NOT NULL
          AND (target.resource_id IS NULL OR target.resource_id=observed.resource_id)
          AND observed.consumer_visibility='activated'
          AND observed.canonicality_state IN ('canonical','safe','finalized')
        LIMIT 2
    ) candidate HAVING count(*)=1
) payment ON TRUE
WHERE target.event_kind IN ('RegistrationGranted','LabelRegistered')
  AND target.source_family IN ('ens_v2_registry_l1','ens_v2_root_l1')
  AND target.after_state->>'source_event'='LabelRegistered'
  AND target.event_identity NOT LIKE '%:RegistrationGranted:topology:%'
  AND COALESCE(target.after_state->>'state_derived','false')<>'true'
UNION ALL
SELECT target.normalized_event_id, payment.value
FROM page target
JOIN LATERAL (
    SELECT jsonb_agg(candidate.value)->0 AS value
    FROM (
        SELECT jsonb_build_object('cost',observed.after_state->'cost') AS value
        FROM bigname_phase.normalized_events observed
        WHERE observed.chain_id=target.chain_id AND observed.block_hash=target.block_hash
          AND observed.block_number=target.block_number AND observed.transaction_index=target.transaction_index
          AND observed.transaction_hash=target.transaction_hash AND observed.namespace=target.namespace
          AND observed.source_family=target.source_family AND observed.source_manifest_id=target.source_manifest_id
          AND observed.manifest_version=target.manifest_version
          AND observed.event_kind='PreimageObserved' AND observed.after_state->>'source_event'='NameRenewed'
          AND observed.log_index>target.log_index AND observed.after_state->>'cost' IS NOT NULL
          AND observed.after_state->>'namehash'=target.after_state->>'namehash'
          AND observed.after_state->>'labelhash'=target.after_state->>'labelhash'
          AND observed.after_state->>'expiry'=target.after_state->>'expiry'
          AND observed.consumer_visibility='activated'
          AND observed.canonicality_state IN ('canonical','safe','finalized')
          AND EXISTS (
              SELECT 1 FROM bigname_phase.manifest_contract_instances declared
              WHERE declared.manifest_id=observed.source_manifest_id AND declared.chain_id=observed.chain_id
                AND declared.declaration_kind='contract'
                AND declared.role IN ('wrapped_registrar_controller','wrapped_registrar_controller_4477cac')
                AND lower(declared.declared_address)=lower(observed.raw_fact_ref->>'emitting_address')
                AND COALESCE(declared.start_block_number,0)<=observed.block_number
          )
          AND NOT EXISTS (
              SELECT 1 FROM bigname_phase.normalized_events intervening
              WHERE intervening.chain_id=target.chain_id AND intervening.block_hash=target.block_hash
                AND intervening.block_number=target.block_number AND intervening.transaction_index=target.transaction_index
                AND intervening.transaction_hash=target.transaction_hash
                AND intervening.source_manifest_id=target.source_manifest_id AND intervening.source_family=target.source_family
                AND intervening.event_kind='RegistrationRenewed' AND intervening.after_state->>'source_event'='NameRenewed'
                AND intervening.after_state->>'labelhash'=target.after_state->>'labelhash'
                AND lower(intervening.raw_fact_ref->>'emitting_address')=lower(target.raw_fact_ref->>'emitting_address')
                AND intervening.log_index>target.log_index AND intervening.log_index<observed.log_index
                AND intervening.consumer_visibility='activated'
                AND intervening.canonicality_state IN ('canonical','safe','finalized')
          )
        LIMIT 2
    ) candidate HAVING count(*)=1
) payment ON TRUE
WHERE target.event_kind='RegistrationRenewed' AND target.chain_id='ethereum-sepolia'
  AND target.source_family='ens_v1_registrar_l1' AND target.after_state->>'source_event'='NameRenewed'
  AND target.after_state->>'cost' IS NULL
  AND EXISTS (
      SELECT 1 FROM bigname_phase.manifest_contract_instances declared
      WHERE declared.manifest_id=target.source_manifest_id AND declared.chain_id=target.chain_id
        AND declared.declaration_kind='contract' AND declared.role='registrar'
        AND lower(declared.declared_address)=lower(target.raw_fact_ref->>'emitting_address')
        AND COALESCE(declared.start_block_number,0)<=target.block_number
  )";

#[cfg(test)]
mod tests;
