use bigname_adapters::schema_v2::{BatchOutput, MigrationAuthorityTransition, seam};
use sqlx::{Postgres, Transaction, types::Uuid};

use crate::{InterpretError, Result};

const CHILD_ANCHOR_KIND: &str = "wrapper_backed_child_control";
/// Both wrapper anchors resolve against the same ENSv1 NameWrapper evidence shape.
const WRAPPER_EVIDENCE_ANCHOR_KIND: &str = "wrapper_backed_control";
const REGISTRAR_ANCHOR_KIND: &str = "registrar_backed_registration";

pub(super) fn validate_boundaries(output: &BatchOutput) -> Result<()> {
    for transition in &output.migration_authority_transitions {
        let matching = output
            .normalized_events
            .iter()
            .filter(|event| exact_boundary(transition, event))
            .count();
        if matching != 1 {
            return Err(InterpretError::data_integrity(format!(
                "migration authority transition {} has {matching} exact activated MigrationApplied boundaries; expected one",
                transition.boundary_event_identity
            )));
        }
    }
    for event in output.normalized_events.iter().filter(|event| {
        event.event_kind == seam::MIGRATION_APPLIED_EVENT_KIND
            && event.consumer_visibility == "activated"
            && matches!(
                event.canonicality_state.as_str(),
                "canonical" | "safe" | "finalized"
            )
    }) {
        let matching = output
            .migration_authority_transitions
            .iter()
            .filter(|transition| exact_boundary(transition, event))
            .count();
        if matching != 1 {
            return Err(InterpretError::data_integrity(format!(
                "activated MigrationApplied boundary {} has {matching} exact authority transitions; expected one",
                event.event_identity,
            )));
        }
    }
    Ok(())
}

fn exact_boundary(
    transition: &MigrationAuthorityTransition,
    event: &bigname_adapters::schema_v2::NormalizedEvent,
) -> bool {
    event.event_identity == transition.boundary_event_identity
        && event.event_kind == seam::MIGRATION_APPLIED_EVENT_KIND
        && event.consumer_visibility == "activated"
        && event.migration_correlation_ids == [transition.migration_correlation_id.clone()]
        && event.logical_name_id.as_deref() == Some(transition.logical_name_id.as_str())
        && event.chain_id == transition.chain_id
        && event.block_number == Some(transition.block_number)
        && event.transaction_index == Some(transition.transaction_index)
        && event.log_index == Some(transition.log_index)
        && matches!(
            event.canonicality_state.as_str(),
            "canonical" | "safe" | "finalized"
        )
        && event.after_state["predecessor_binding"] == transition.predecessor_selector
        && event.after_state["successor_binding"]["binding_id"]
            == transition.successor_surface_binding_id.to_string()
        && event.after_state["successor_binding"]["resource_id"]
            == transition.successor_resource_id.to_string()
        && event.after_state["successor_binding"]["authority_epoch"] == transition.successor_arm
}

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    transitions: &[MigrationAuthorityTransition],
) -> Result<()> {
    for transition in transitions {
        let selector = validate(transition)?;
        let boundary_time: Option<time::OffsetDateTime> = sqlx::query_scalar(&format!(
            "SELECT lineage.block_timestamp + $8 * interval '1 microsecond'
             FROM surface_bindings binding
             JOIN chain_lineage lineage
               ON lineage.chain_id = binding.chain_id
              AND lineage.block_hash = binding.block_hash
              AND lineage.block_number = binding.block_number
             WHERE binding.surface_binding_id = $1
               AND binding.logical_name_id = $2
               AND binding.resource_id = $3
               AND binding.authority_arm = $4
               AND binding.chain_id = $5
               AND binding.block_number = $6
               AND COALESCE((binding.provenance ->> '{}')::bigint, -1) = $7
               AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             FOR UPDATE OF binding",
            seam::TRANSACTION_INDEX_KEY,
        ))
        .bind(transition.successor_surface_binding_id)
        .bind(&transition.logical_name_id)
        .bind(transition.successor_resource_id)
        .bind(&transition.successor_arm)
        .bind(&transition.chain_id)
        .bind(transition.block_number)
        .bind(transition.transaction_index)
        .bind(transition.log_index)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock migration successor binding", error)
        })?;
        let boundary_time = boundary_time.ok_or_else(|| {
            InterpretError::data_integrity(format!(
                "activated migration boundary {} has no exact ENSv2 successor binding",
                transition.boundary_event_identity
            ))
        })?;
        // A child or unlocked second-level predecessor resolves and closes at its recorded ENSv1
        // cleanup. A locked-wrapped second-level predecessor resolves and closes at the boundary.
        let (predecessor_at, predecessor_time) = match &selector.cleanup {
            Some(cleanup) => (
                (
                    cleanup.block_number,
                    cleanup.transaction_index,
                    cleanup.log_index,
                ),
                resolve_cleanup_time(transaction, transition, &selector, cleanup).await?,
            ),
            None => (
                (
                    transition.block_number,
                    transition.transaction_index,
                    transition.log_index,
                ),
                boundary_time,
            ),
        };
        // A fallback registrar binding is effective from NameUnwrapped, but its confirming
        // evidence is the cleanup transfer. Only registrar evidence may equal the cleanup.
        let allow_cleanup_evidence =
            selector.cleanup.is_some() && selector.anchor_kind == REGISTRAR_ANCHOR_KIND;
        let predecessors: Vec<Uuid> = sqlx::query_scalar(&format!(
            "SELECT surface_binding_id
             FROM surface_bindings
             WHERE chain_id = $1
               AND logical_name_id = $2
               AND authority_arm = $3
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (
                   (
                       block_number,
                       COALESCE((provenance ->> '{}')::bigint, -1),
                       COALESCE((provenance ->> '{}')::bigint, -1)
                   ) < ($4, $5, $6)
                   OR ($12 AND (
                       block_number,
                       COALESCE((provenance ->> '{}')::bigint, -1),
                       COALESCE((provenance ->> '{}')::bigint, -1)
                   ) = ($4, $5, $6))
               )
               AND active_from < $7
               AND (active_to IS NULL OR active_to >= $7)
               AND EXISTS (
                   SELECT 1
                   FROM normalized_events evidence
                   WHERE evidence.chain_id = surface_bindings.chain_id
                     AND evidence.logical_name_id = surface_bindings.logical_name_id
                     AND evidence.resource_id = surface_bindings.resource_id
                     AND evidence.consumer_visibility = 'activated'
                     AND evidence.canonicality_state IN ('canonical', 'safe', 'finalized')
                     AND (
                         (
                             evidence.block_number,
                             COALESCE(evidence.transaction_index, -1),
                             COALESCE(evidence.log_index, -1)
                         ) < ($4, $5, $6)
                         OR ($12
                             AND (
                                 surface_bindings.block_number,
                                 COALESCE((surface_bindings.provenance ->> '{}')::bigint, -1),
                                 COALESCE((surface_bindings.provenance ->> '{}')::bigint, -1)
                             ) = ($4, $5, $6)
                             AND (evidence.block_number,
                                  COALESCE(evidence.transaction_index, -1),
                                  COALESCE(evidence.log_index, -1)) = ($4, $5, $6))
                     )
                     AND EXISTS (
                         SELECT 1
                         FROM chain_lineage evidence_lineage
                         WHERE evidence_lineage.chain_id = evidence.chain_id
                           AND evidence_lineage.block_hash = evidence.block_hash
                           AND evidence_lineage.block_number = evidence.block_number
                           AND evidence_lineage.canonicality_state IN (
                               'canonical', 'safe', 'finalized'
                           )
                     )
                     AND (
                         (
                             $8 = 'registrar_backed_registration'
                             AND evidence.after_state ->> 'token_id' = $9
                             AND EXISTS (
                                 SELECT 1
                                 FROM contract_instance_addresses address
                                 WHERE address.chain_id = evidence.chain_id
                                   AND address.contract_instance_id = $10
                                   AND lower(address.address) = lower(
                                       evidence.raw_fact_ref ->> 'emitting_address'
                                   )
                                   AND (
                                       address.active_from_block_number IS NULL
                                       OR address.active_from_block_number <= evidence.block_number
                                   )
                                   AND (
                                       address.active_to_block_number IS NULL
                                       OR address.active_to_block_number >= evidence.block_number
                                   )
                             )
                         )
                         OR (
                             $8 = 'wrapper_backed_control'
                             AND evidence.after_state ->> 'authority_kind' = 'wrapper'
                             AND evidence.after_state ->> 'node' = $9
                             AND lower(evidence.raw_fact_ref ->> 'emitting_address') = lower($11)
                         )
                     )
               )
             ORDER BY surface_binding_id
             FOR UPDATE",
            seam::TRANSACTION_INDEX_KEY,
            seam::LOG_INDEX_KEY,
            seam::TRANSACTION_INDEX_KEY,
            seam::LOG_INDEX_KEY,
            seam::TRANSACTION_INDEX_KEY,
            seam::LOG_INDEX_KEY,
        ))
        .bind(&transition.chain_id)
        .bind(&transition.logical_name_id)
        .bind(&transition.expected_predecessor_arm)
        .bind(predecessor_at.0)
        .bind(predecessor_at.1)
        .bind(predecessor_at.2)
        .bind(predecessor_time)
        .bind(&selector.anchor_kind)
        .bind(&selector.identity)
        .bind(selector.contract_instance_id)
        .bind(&selector.contract_address)
        .bind(allow_cleanup_evidence)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock migration predecessor binding", error)
        })?;
        if predecessors.len() != 1 {
            return Err(InterpretError::data_integrity(format!(
                "activated migration boundary {} has {} active ENSv1 predecessors matching its resource selector; expected exactly one",
                transition.boundary_event_identity,
                predecessors.len()
            )));
        }
        sqlx::query(
            "UPDATE surface_bindings
             SET active_to = $2, observed_at = now()
             WHERE surface_binding_id = $1",
        )
        .bind(predecessors[0])
        .bind(predecessor_time)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to apply migration authority transition", error)
        })?;
    }
    Ok(())
}

/// Resolves the recorded ENSv1 cleanup to the instant its authority ended.
///
/// A child boundary names its wrapper cleanup. Both unlocked `.eth` second-level paths name the
/// BaseRegistrar transfer that precedes the ENSv2 registration. The recorded event must exist
/// exactly as described — same identity, name, position, source event, and admitted emitter — on
/// readable canonical lineage. "Some earlier cleanup" is not equivalent evidence.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@ccaeb58)
async fn resolve_cleanup_time(
    transaction: &mut Transaction<'_, Postgres>,
    transition: &MigrationAuthorityTransition,
    selector: &PredecessorSelector,
    cleanup: &PredecessorCleanup,
) -> Result<time::OffsetDateTime> {
    // The cleanup kind must match its anchor, so an unrelated same-position event cannot stand in
    // for the recorded transfer.
    let resolved: Option<time::OffsetDateTime> = sqlx::query_scalar(
        "SELECT lineage.block_timestamp + $6 * interval '1 microsecond'
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.event_identity = $2
           AND event.logical_name_id = $3
           AND event.block_number = $4
           AND COALESCE(event.transaction_index, -1) = $5
           AND COALESCE(event.log_index, -1) = $6
           AND event.after_state ->> 'source_event' = $7
           AND (
               (
                   $9 = 'registrar_backed_registration'
                   AND event.event_kind = $10
                   AND EXISTS (
                       SELECT 1
                       FROM contract_instance_addresses address
                       WHERE address.chain_id = event.chain_id
                         AND address.contract_instance_id = $11
                         AND lower(address.address) = lower(
                             event.raw_fact_ref ->> 'emitting_address'
                         )
                         AND (
                             address.active_from_block_number IS NULL
                             OR address.active_from_block_number <= event.block_number
                         )
                         AND (
                             address.active_to_block_number IS NULL
                             OR address.active_to_block_number >= event.block_number
                         )
                   )
               )
               OR (
                   $9 = 'wrapper_backed_control'
                   AND event.event_kind = ANY($12)
                   AND lower(event.raw_fact_ref ->> 'emitting_address') = lower($8)
               )
           )
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(&transition.chain_id)
    .bind(&cleanup.event_identity)
    .bind(&transition.logical_name_id)
    .bind(cleanup.block_number)
    .bind(cleanup.transaction_index)
    .bind(cleanup.log_index)
    .bind(&cleanup.source_event)
    .bind(&selector.contract_address)
    .bind(&selector.anchor_kind)
    .bind(seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
    .bind(selector.contract_instance_id)
    .bind(
        seam::CHILD_CLEANUP_EVENT_KINDS
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect::<Vec<_>>(),
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to resolve migration predecessor cleanup", error)
    })?;
    resolved.ok_or_else(|| {
        InterpretError::data_integrity(format!(
            "activated migration boundary {} has no exact ENSv1 predecessor cleanup",
            transition.boundary_event_identity
        ))
    })
}

struct PredecessorSelector {
    /// The evidence shape the predecessor query matches. A child anchor keeps its own
    /// `anchor_kind` in the selector but resolves through the same ENSv1 NameWrapper evidence,
    /// because every migratable child is held in that wrapper immediately before its boundary
    /// ([child migration boundary](../../../../../docs/glossary.md#child-migration-boundary)).
    anchor_kind: String,
    identity: String,
    contract_instance_id: Option<Uuid>,
    contract_address: Option<String>,
    /// Present for a child boundary or either unlocked second-level boundary, whose predecessor is
    /// resolved against its ENSv1 cleanup rather than the later ENSv2 registration.
    cleanup: Option<PredecessorCleanup>,
}

/// The exact ENSv1 cleanup a boundary records, which is the position its authority ended at.
struct PredecessorCleanup {
    event_identity: String,
    source_event: String,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
}

fn validate(transition: &MigrationAuthorityTransition) -> Result<PredecessorSelector> {
    let selection = transition
        .predecessor_selector
        .get("selection")
        .and_then(serde_json::Value::as_str);
    let selector_name = transition
        .predecessor_selector
        .get("logical_name_id")
        .and_then(serde_json::Value::as_str);
    let selector_arm = transition
        .predecessor_selector
        .get("authority_epoch")
        .and_then(serde_json::Value::as_str);
    let resource = transition.predecessor_selector.get("resource");
    let resource_selection = resource
        .and_then(|value| value.get("selection"))
        .and_then(serde_json::Value::as_str);
    let anchor_kind = resource
        .and_then(|value| value.get("anchor_kind"))
        .and_then(serde_json::Value::as_str);
    // Each selection admits only its stated anchor family. Child and unlocked registrar
    // boundaries resolve at their recorded cleanup; locked-wrapped second-level boundaries
    // resolve at the migration boundary itself.
    let cleanup_relative = match (selection, anchor_kind) {
        (Some("active_immediately_before_boundary"), Some(WRAPPER_EVIDENCE_ANCHOR_KIND)) => false,
        (Some("active_immediately_before_predecessor_cleanup"), Some(CHILD_ANCHOR_KIND)) => true,
        (Some("active_immediately_before_predecessor_cleanup"), Some(REGISTRAR_ANCHOR_KIND)) => {
            true
        }
        _ => {
            return Err(InterpretError::data_integrity(format!(
                "activated migration boundary {} has an invalid authority selector or position",
                transition.boundary_event_identity
            )));
        }
    };
    if transition.expected_predecessor_arm != "ens_v1"
        || transition.successor_arm != "ens_v2"
        || selector_arm != Some(transition.expected_predecessor_arm.as_str())
        || selector_name != Some(transition.logical_name_id.as_str())
        || transition.block_number < 0
        || transition.transaction_index < 0
        || transition.log_index < 0
    {
        return Err(InterpretError::data_integrity(format!(
            "activated migration boundary {} has an invalid authority selector or position",
            transition.boundary_event_identity
        )));
    }
    if cleanup_relative && anchor_kind == Some(CHILD_ANCHOR_KIND) {
        return child_selector(transition, resource, resource_selection);
    }
    match anchor_kind {
        Some(REGISTRAR_ANCHOR_KIND) => {
            let token_id = resource
                .and_then(|value| value.get("token_id"))
                .and_then(serde_json::Value::as_str);
            let labelhash = resource
                .and_then(|value| value.get("labelhash"))
                .and_then(serde_json::Value::as_str);
            let contract_instance_id = resource
                .and_then(|value| value.get("contract_instance_id"))
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse().ok());
            let expected_selection = if cleanup_relative {
                "current_registrar_resource_immediately_before_predecessor_cleanup"
            } else {
                "current_registrar_resource_immediately_before_boundary"
            };
            if resource_selection != Some(expected_selection)
                || token_id.is_none()
                || token_id != labelhash
                || contract_instance_id.is_none()
            {
                return Err(invalid_selector(transition));
            }
            Ok(PredecessorSelector {
                anchor_kind: "registrar_backed_registration".to_owned(),
                identity: token_id.unwrap().to_owned(),
                contract_instance_id,
                contract_address: None,
                cleanup: cleanup_relative
                    .then(|| predecessor_cleanup(transition))
                    .transpose()?,
            })
        }
        Some("wrapper_backed_control") => {
            let namehash = resource
                .and_then(|value| value.get("namehash"))
                .and_then(serde_json::Value::as_str);
            let wrapper_token_id = resource
                .and_then(|value| value.get("wrapper_token_id"))
                .and_then(serde_json::Value::as_str);
            let contract_address = resource
                .and_then(|value| value.get("contract_address"))
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty());
            if resource_selection != Some("current_wrapper_resource_immediately_before_boundary")
                || namehash.is_none()
                || namehash != wrapper_token_id
                || contract_address.is_none()
            {
                return Err(invalid_selector(transition));
            }
            Ok(PredecessorSelector {
                anchor_kind: WRAPPER_EVIDENCE_ANCHOR_KIND.to_owned(),
                identity: namehash.unwrap().to_owned(),
                contract_instance_id: None,
                contract_address: contract_address.map(str::to_owned),
                cleanup: None,
            })
        }
        _ => Err(invalid_selector(transition)),
    }
}

/// The child anchor names the child's own position in the ENSv1 NameWrapper and carries the parent
/// evidence the correlation derived it from, so every field it records has to be present and agree
/// with the transition before the cleanup is resolved.
fn child_selector(
    transition: &MigrationAuthorityTransition,
    resource: Option<&serde_json::Value>,
    resource_selection: Option<&str>,
) -> Result<PredecessorSelector> {
    let field = |key: &str| {
        resource
            .and_then(|value| value.get(key))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    let namehash = field("namehash");
    let contract_address = field("contract_address");
    if resource_selection != Some("current_wrapper_resource_immediately_before_predecessor_cleanup")
        || namehash.is_none()
        || namehash != field("wrapper_token_id")
        || contract_address.is_none()
        || field("parent_namehash").is_none()
        || field("labelhash").is_none()
        || field("parent_migration_correlation_id").is_none()
    {
        return Err(invalid_selector(transition));
    }
    let cleanup = predecessor_cleanup(transition)?;
    Ok(PredecessorSelector {
        anchor_kind: WRAPPER_EVIDENCE_ANCHOR_KIND.to_owned(),
        identity: namehash.unwrap().to_owned(),
        contract_instance_id: None,
        contract_address: contract_address.map(str::to_owned),
        cleanup: Some(cleanup),
    })
}

fn predecessor_cleanup(transition: &MigrationAuthorityTransition) -> Result<PredecessorCleanup> {
    let recorded = transition
        .predecessor_selector
        .get("predecessor_cleanup")
        .ok_or_else(|| invalid_selector(transition))?;
    let text = |key: &str| {
        recorded
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    let number = |key: &str| recorded.get(key).and_then(serde_json::Value::as_i64);
    let (Some(event_identity), Some(source_event), Some(block_number), Some(transaction_index)) = (
        text("event_identity"),
        text("source_event"),
        number("block_number"),
        number(seam::TRANSACTION_INDEX_KEY),
    ) else {
        return Err(invalid_selector(transition));
    };
    // Every cleanup-relative path retires the ENSv1 side before registering the successor. The
    // direct-unwrapped controller transfers the registrar token first
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@ccaeb58,
    // upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@ccaeb58),
    // and its unlocked-wrapped loop unwraps before injecting
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@ccaeb58),
    // while the child receiver performs its cleanup first
    // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2@ccaeb58,
    // upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2@ccaeb58,
    // upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L178 @ ens_v2@ccaeb58,
    // upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L186 @ ens_v2@ccaeb58),
    // and its receiver hook runs the whole migration synchronously
    // (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L119 @ ens_v2@ccaeb58),
    // so this boundary's cleanup is in the same transaction and strictly earlier in it. A batch may
    // interleave other names, hence the match is per boundary, never per transaction.
    let log_index = number(seam::LOG_INDEX_KEY).unwrap_or(-1);
    if block_number != transition.block_number
        || transaction_index != transition.transaction_index
        || log_index < 0
        || log_index >= transition.log_index
    {
        return Err(invalid_selector(transition));
    }
    Ok(PredecessorCleanup {
        event_identity: event_identity.to_owned(),
        source_event: source_event.to_owned(),
        block_number,
        transaction_index,
        log_index,
    })
}

fn invalid_selector(transition: &MigrationAuthorityTransition) -> InterpretError {
    InterpretError::data_integrity(format!(
        "activated migration boundary {} has an invalid predecessor resource selector",
        transition.boundary_event_identity
    ))
}
