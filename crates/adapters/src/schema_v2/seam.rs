//! Hash-covered vocabulary and formulas consumed by the schema-v2 persistence transport.

use std::collections::BTreeMap;

use anyhow::{Context, bail};
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};

pub const PREIMAGE_OBSERVATION_EVENT_KIND: &str = "PreimageObserved";
pub const SURFACE_BOUND_EVENT_KIND: &str = "SurfaceBound";
pub const MIGRATION_APPLIED_EVENT_KIND: &str = "MigrationApplied";
pub const SURFACE_UNBOUND_EVENT_KIND: &str = "SurfaceUnbound";
pub const TOKEN_CONTROL_TRANSFERRED_EVENT_KIND: &str = "TokenControlTransferred";
pub const SURFACE_BINDING_ID_KEY: &str = "surface_binding_id";
pub const ARM_WIDE_BINDING_CLOSE_KEY: &str = "arm_wide_binding_close";
pub const CLOSED_AUTHORITY_ARM_KEY: &str = "closed_authority_arm";
pub const TOKEN_LINEAGE_ID_KEY: &str = "token_lineage_id";
pub const INTERPRETER_STATE_KEY: &str = "interpreter_state_key";
pub const SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY: &str = "subregistry_invalidated_token_ids";
pub const STATE_SCOPE_KEY: &str = "state_scope";
pub const OBSERVATION_KEY: &str = "observation_key";
pub const TRANSACTION_INDEX_KEY: &str = "transaction_index";
pub const LOG_INDEX_KEY: &str = "log_index";
pub const PROVENANCE_KIND_KEY: &str = "kind";
pub const RAW_BLOCK_PROVENANCE_KIND: &str = "raw_block";
pub const MIGRATION_REGISTRY_ASSOCIATION_KIND: &str = "migration_registry_creation";
pub const REGISTRY_ANNOUNCEMENT_EDGE_KIND: &str = "registry_announcement";

pub const ADMISSION_DISCOVERY_EDGE_KINDS: &[&str] = &["resolver", REGISTRY_ANNOUNCEMENT_EDGE_KIND];

/// The only normalized event kinds a child migration boundary's recorded ENSv1 cleanup can be: the
/// wrapper token parked in the graveyard
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58),
/// or the node unwrapped into it
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178 @ ens_v2@ccaeb58).
/// Which upstream branch a cleanup came from is adapter knowledge, so the transport matches this
/// set rather than naming kinds of its own.
pub const CHILD_CLEANUP_EVENT_KINDS: &[&str] = &[
    TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
    SURFACE_UNBOUND_EVENT_KIND,
];

/// The instant an event closed bindings at, which is where a redo reopen looks for the close to
/// undo. Ordinarily that is the event's own log position. A cleanup-relative migration boundary
/// instead closes its ENSv1 predecessor at the cleanup it records, earlier in its own transaction,
/// so keying a reopen on the boundary's own log would find nothing to undo. This covers direct
/// children and both unlocked second-level paths; for the `locked_child` shape, whose cleanup
/// closes nothing by itself, the transition write is the only close there is.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@ccaeb58)
pub const EVENT_CLOSE_TIME_SQL: &str = "lineage.block_timestamp + make_interval(\
    secs => COALESCE(\
        CASE WHEN event.after_state #>> '{predecessor_binding,selection}' \
                  = 'active_immediately_before_predecessor_cleanup' \
             THEN (\
                 event.after_state #>> '{predecessor_binding,predecessor_cleanup,log_index}'\
             )::bigint \
        END, \
        event.log_index, 0\
    )::double precision / 1000000.0\
)";
pub const BINDING_CLOSE_CLAMP_SQL: &str = "GREATEST($2, active_from + interval '1 microsecond')";
pub const REDO_BINDING_CLOSE_CLAMP_SQL: &str =
    "GREATEST(event.closed_at, binding.active_from + interval '1 microsecond')";
/// Exact persisted evidence that a non-lifecycle observation accompanies an arm-wide binding
/// close and names the replacement binding exempted from that close.
pub const REDO_ARM_WIDE_CLOSE_SQL: &str = "event.event_kind = 'PreimageObserved'
                           AND event.after_state ->> 'arm_wide_binding_close' = 'true'
                           AND event.after_state ->> 'surface_binding_id' IS NOT NULL
                           AND event.after_state ->> 'closed_authority_arm' IS NOT NULL
                           AND EXISTS (
                               SELECT 1
                               FROM surface_bindings replacement
                               WHERE replacement.surface_binding_id::text
                                   = event.after_state ->> 'surface_binding_id'
                                 AND replacement.chain_id = event.chain_id
                                 AND replacement.logical_name_id = event.logical_name_id
                                 AND replacement.authority_arm
                                   = event.after_state ->> 'closed_authority_arm'
                                 AND replacement.block_number = event.block_number
                                 AND replacement.block_hash = event.block_hash
                                 AND COALESCE(
                                     (replacement.provenance ->> 'transaction_index')::bigint,
                                     -1
                                 ) = COALESCE(event.transaction_index, -1)
                                 AND COALESCE(
                                     (replacement.provenance ->> 'log_index')::bigint,
                                     -1
                                 ) = COALESCE(event.log_index, -1)
                           )";
/// The authority arm a closing event's own evidence names, which is the arm a redo reopen may
/// undo a close on. An ordinary open or unbind closes only its own arm, so that arm is the one the
/// event identifies: the binding it opened, or failing that its resource. A migration boundary is
/// deliberately cross-arm and records the predecessor arm it closes, and its successor binding is
/// on the other arm entirely — so a boundary that fails to record a predecessor arm resolves to
/// NULL and reopens nothing, rather than falling through to the arm it opened. Without exact
/// evidence the event reopens nothing rather than guessing an arm from position or source family.
pub const REDO_CLOSED_ARM_SQL: &str = "CASE
                       WHEN event.event_kind = 'MigrationApplied'
                           THEN event.after_state #>> '{predecessor_binding,authority_epoch}'
                       WHEN event.event_kind = 'PreimageObserved'
                            AND event.after_state ->> 'arm_wide_binding_close' = 'true'
                           THEN event.after_state ->> 'closed_authority_arm'
                       ELSE (
                           SELECT CASE
                               WHEN count(DISTINCT opened.authority_arm) = 1
                                   THEN min(opened.authority_arm)
                           END
                           FROM surface_bindings opened
                           WHERE opened.chain_id = event.chain_id
                             AND opened.logical_name_id = event.logical_name_id
                             AND CASE
                                 WHEN event.after_state ->> 'surface_binding_id' IS NOT NULL
                                     THEN opened.surface_binding_id::text
                                          = event.after_state ->> 'surface_binding_id'
                                 ELSE opened.resource_id = event.resource_id
                             END
                       )
                   END";
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
        let retained_state_key = retained_event_state_key(
            retained_prior_state_key(
                event
                    .raw_fact_ref
                    .get(INTERPRETER_STATE_KEY)
                    .and_then(Value::as_str),
                &event.event_identity,
            ),
            &event.after_state,
        );
        let prior = super::PriorEventInput {
            retained_state_key,
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

pub fn retained_event_state_key(mut key: String, state: &Value) -> String {
    if state.get(SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY).is_some() {
        key.push_str(":subregistry-zero-clear");
    }
    key
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
