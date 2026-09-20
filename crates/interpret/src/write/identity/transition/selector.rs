use bigname_adapters::schema_v2::{MigrationAuthorityTransition, seam};
use sqlx::types::Uuid;

use crate::{InterpretError, Result};

const CHILD_ANCHOR_KIND: &str = "wrapper_backed_child_control";
/// Both wrapper anchors resolve against the same ENSv1 NameWrapper evidence shape.
const WRAPPER_EVIDENCE_ANCHOR_KIND: &str = "wrapper_backed_control";
pub(super) const REGISTRAR_ANCHOR_KIND: &str = "registrar_backed_registration";

pub(super) struct PredecessorSelector {
    /// The evidence shape the predecessor query matches. A child anchor keeps its own
    /// `anchor_kind` in the selector but resolves through the same ENSv1 NameWrapper evidence,
    /// because every migratable child is held in that wrapper immediately before its boundary
    /// ([child migration boundary](../../../../../docs/glossary.md#child-migration-boundary)).
    pub(super) anchor_kind: String,
    pub(super) identity: String,
    pub(super) contract_instance_id: Option<Uuid>,
    pub(super) contract_address: Option<String>,
    /// Present for a child boundary or either unlocked second-level boundary, whose predecessor is
    /// resolved against its ENSv1 cleanup rather than the later ENSv2 registration.
    pub(super) cleanup: Option<PredecessorCleanup>,
}

/// The exact ENSv1 cleanup a boundary records, which is the position its authority ended at.
pub(super) struct PredecessorCleanup {
    pub(super) event_identity: String,
    pub(super) source_event: String,
    pub(super) block_number: i64,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
}

pub(super) fn validate(transition: &MigrationAuthorityTransition) -> Result<PredecessorSelector> {
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
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@a971bd64),
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L118 @ ens_v2@a971bd64),
    // and its unlocked-wrapped loop unwraps before injecting
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L146-L148 @ ens_v2@a971bd64),
    // while the child receiver performs its cleanup first
    // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58),
    // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58),
    // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64),
    // (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64),
    // and its receiver hook runs the whole migration synchronously
    // (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L119 @ ens_v2@a971bd64),
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
