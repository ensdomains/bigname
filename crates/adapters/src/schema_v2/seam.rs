//! Hash-covered vocabulary and formulas consumed by the schema-v2 persistence transport.

use std::collections::BTreeMap;

use anyhow::{Context, bail};
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};

pub const PREIMAGE_OBSERVATION_EVENT_KIND: &str = "PreimageObserved";
pub const SURFACE_BOUND_EVENT_KIND: &str = "SurfaceBound";
pub const MIGRATION_APPLIED_EVENT_KIND: &str = "MigrationApplied";
pub const SURFACE_UNBOUND_EVENT_KIND: &str = "SurfaceUnbound";
pub const SURFACE_BINDING_ID_KEY: &str = "surface_binding_id";
pub const TOKEN_LINEAGE_ID_KEY: &str = "token_lineage_id";
pub const INTERPRETER_STATE_KEY: &str = "interpreter_state_key";
pub const STATE_SCOPE_KEY: &str = "state_scope";
pub const OBSERVATION_KEY: &str = "observation_key";
pub const TRANSACTION_INDEX_KEY: &str = "transaction_index";
pub const LOG_INDEX_KEY: &str = "log_index";
pub const PROVENANCE_KIND_KEY: &str = "kind";
pub const RAW_BLOCK_PROVENANCE_KIND: &str = "raw_block";
pub const MIGRATION_REGISTRY_ASSOCIATION_KIND: &str = "migration_registry_creation";
pub const REGISTRY_ANNOUNCEMENT_EDGE_KIND: &str = "registry_announcement";

pub const ADMISSION_DISCOVERY_EDGE_KINDS: &[&str] = &["resolver", REGISTRY_ANNOUNCEMENT_EDGE_KIND];

pub const EVENT_CLOSE_TIME_SQL: &str = "lineage.block_timestamp + make_interval(\
    secs => COALESCE(event.log_index, 0)::double precision / 1000000.0\
)";
pub const BINDING_CLOSE_CLAMP_SQL: &str = "GREATEST($2, active_from + interval '1 microsecond')";
pub const REDO_BINDING_CLOSE_CLAMP_SQL: &str =
    "GREATEST(event.closed_at, binding.active_from + interval '1 microsecond')";
pub const REDO_RESOLVER_EVIDENCE_SELECT_SQL: &str = r#"
    SELECT event.chain_id, event.event_identity, event.block_number, event.event_kind,
           event.source_family, event.resource_id,
           CASE
               WHEN event.event_kind = 'ResolverChanged' THEN
                   NULLIF(lower(event.before_state ->> 'resolver'), '')
               WHEN event.event_kind = 'AliasChanged' THEN
                   NULLIF(lower(COALESCE(
                       event.before_state ->> 'resolver',
                       event.raw_fact_ref ->> 'emitting_address'
                   )), '')
               WHEN event.before_state #>> '{scope,kind}' = 'resolver' THEN
                   NULLIF(lower(event.before_state #>> '{scope,resolver_address}'), '')
           END,
           CASE
               WHEN event.event_kind = 'ResolverChanged' THEN
                   NULLIF(lower(event.after_state ->> 'resolver'), '')
               WHEN event.event_kind = 'AliasChanged' THEN
                   NULLIF(lower(COALESCE(
                       event.after_state ->> 'resolver',
                       event.raw_fact_ref ->> 'emitting_address'
                   )), '')
               WHEN event.after_state #>> '{scope,kind}' = 'resolver' THEN
                   NULLIF(lower(event.after_state #>> '{scope,resolver_address}'), '')
           END
    FROM normalized_events event
    WHERE event.chain_id = $1
      AND event.block_number BETWEEN $2 AND $3
      AND event.consumer_visibility = 'activated'
      AND event.event_kind IN ('PermissionChanged', 'ResolverChanged', 'AliasChanged')
      AND (
          event.before_state ->> 'resolver' IS NOT NULL
          OR event.after_state ->> 'resolver' IS NOT NULL
          OR (
              event.event_kind = 'AliasChanged'
              AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
          )
          OR (
              event.before_state #>> '{scope,kind}' = 'resolver'
              AND event.before_state #>> '{scope,resolver_address}' IS NOT NULL
          )
          OR (
              event.after_state #>> '{scope,kind}' = 'resolver'
              AND event.after_state #>> '{scope,resolver_address}' IS NOT NULL
          )
      )
"#;

pub fn event_time(block_timestamp: OffsetDateTime, log_index: i64) -> OffsetDateTime {
    block_timestamp + Duration::microseconds(log_index.max(0))
}

pub fn binding_open_time(
    candidate: OffsetDateTime,
    predecessor: Option<OffsetDateTime>,
) -> OffsetDateTime {
    predecessor
        .filter(|predecessor| candidate <= *predecessor)
        .map(|predecessor| predecessor + Duration::microseconds(1))
        .unwrap_or(candidate)
}

pub fn raw_block_provenance() -> Value {
    json!({ (PROVENANCE_KIND_KEY): RAW_BLOCK_PROVENANCE_KIND })
}

pub fn is_raw_block_provenance(provenance: &Value) -> bool {
    provenance.get(PROVENANCE_KIND_KEY).and_then(Value::as_str) == Some(RAW_BLOCK_PROVENANCE_KIND)
}

pub fn fold_prior_events(
    prior: Vec<super::PriorEventInput>,
    events: &[super::NormalizedEvent],
    blocks: &[super::RawBlockInput],
) -> anyhow::Result<Vec<super::PriorEventInput>> {
    let block_times = blocks
        .iter()
        .map(|block| {
            (
                (block.block_number, block.block_hash.as_str()),
                block.block_timestamp,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut sequence = 0_u64;
    let mut compacted = BTreeMap::<String, (u64, super::PriorEventInput)>::new();
    for event in prior {
        sequence = sequence.saturating_add(1);
        compacted.insert(event.retained_state_key.clone(), (sequence, event));
    }
    for event in events {
        let state_scope = event
            .raw_fact_ref
            .get(STATE_SCOPE_KEY)
            .and_then(Value::as_str)
            .context("normalized event is missing its adapter state scope")?
            .to_owned();
        let block_timestamp = match (event.block_number, event.block_hash.as_deref()) {
            (Some(number), Some(hash)) => {
                Some(*block_times.get(&(number, hash)).with_context(|| {
                    format!("normalized event block {number} {hash} is absent from its batch")
                })?)
            }
            (None, None) => None,
            _ => bail!("normalized event has an incomplete block position"),
        };
        let prior = super::PriorEventInput {
            retained_state_key: retained_prior_state_key(
                event
                    .raw_fact_ref
                    .get(INTERPRETER_STATE_KEY)
                    .and_then(Value::as_str),
                &event.event_identity,
            ),
            chain_id: event.chain_id.clone(),
            namespace: event.namespace.clone(),
            logical_name_id: event.logical_name_id.clone(),
            resource_id: event.resource_id,
            event_kind: event.event_kind.clone(),
            source_family: event.source_family.clone(),
            manifest_version: event.manifest_version,
            source_manifest_id: event.source_manifest_id,
            state_scope: Some(state_scope),
            block_timestamp,
            after_state: event.after_state.clone(),
        };
        sequence = sequence.saturating_add(1);
        compacted.insert(prior.retained_state_key.clone(), (sequence, prior));
    }
    let mut compacted = compacted.into_values().collect::<Vec<_>>();
    compacted.sort_by_key(|(sequence, _)| *sequence);
    Ok(compacted.into_iter().map(|(_, event)| event).collect())
}

pub fn retained_prior_state_key(
    interpreter_state_key: Option<&str>,
    event_identity: &str,
) -> String {
    match interpreter_state_key {
        Some(key) => format!("state:{key}"),
        None => format!("legacy:{event_identity}"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn fold_prior_events_preserves_distinct_legacy_event_identities() -> anyhow::Result<()> {
        let prior = [1, 2]
            .into_iter()
            .map(|value| super::super::PriorEventInput {
                retained_state_key: retained_prior_state_key(None, &format!("legacy-{value}")),
                chain_id: "1".to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: None,
                resource_id: None,
                event_kind: "LegacyEvent".to_owned(),
                source_family: "legacy".to_owned(),
                manifest_version: 1,
                source_manifest_id: None,
                state_scope: None,
                block_timestamp: None,
                after_state: json!({"value": value}),
            })
            .collect::<Vec<_>>();

        let folded = fold_prior_events(prior, &[], &[])?;

        assert_eq!(folded.len(), 2);
        assert_eq!(folded[0].after_state["value"], 1);
        assert_eq!(folded[1].after_state["value"], 2);
        Ok(())
    }
}
