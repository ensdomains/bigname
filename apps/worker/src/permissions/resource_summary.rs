use std::collections::BTreeSet;

use bigname_storage::{PermissionsCurrentResourceSummary, ens_v2_registry_resource_id};
use serde_json::{Value, json};
use uuid::Uuid;

use super::canonicality::{build_canonicality_summary, build_chain_positions, format_timestamp};
use super::types::{RelevantEvent, ResourceProjectionContext};

const ENSV1_WRAPPER_SOURCE_FAMILY: &str = "ens_v1_wrapper_l1";
const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const ENSV2_ROOT_UPSTREAM_RESOURCE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
const WRAPPER_UNSUPPORTED_REASON: &str = "ensv1_wrapper_holder_permissions_not_projected";
const UNKNOWN_AUTHORITY_REASON: &str = "resource_permission_authority_not_projected";

pub(super) fn build_resource_summary(
    context: &ResourceProjectionContext,
    events: &[RelevantEvent],
) -> PermissionsCurrentResourceSummary {
    let authority_kind = projected_authority_kind(context, events);
    let root_resource_id = ensv2_root_resource_id(context);
    let event_refs = events.iter().collect::<Vec<_>>();
    let mut chain_positions = build_chain_positions(&event_refs);
    if chain_positions
        .as_object()
        .is_none_or(serde_json::Map::is_empty)
        && let Some(timestamp) = context.block_timestamp
    {
        let mut positions = serde_json::Map::new();
        positions.insert(
            context.chain_id.clone(),
            json!({
                "chain_id": context.chain_id,
                "block_number": context.block_number,
                "block_hash": context.block_hash,
                "timestamp": format_timestamp(timestamp),
            }),
        );
        chain_positions = Value::Object(positions);
    }
    let canonicality_summary = if event_refs.is_empty() {
        let mut chains = serde_json::Map::new();
        chains.insert(
            context.chain_id.clone(),
            Value::String(context.canonicality_state.as_str().to_owned()),
        );
        json!({
            "status": context.canonicality_state.as_str(),
            "chains": chains,
        })
    } else {
        build_canonicality_summary(&event_refs)
    };
    let coverage = match authority_kind.as_deref() {
        Some("wrapper") => json!({
            "status": "unsupported",
            "exhaustiveness": "not_applicable",
            "source_classes_considered": ["permissions_current", ENSV1_WRAPPER_SOURCE_FAMILY],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": WRAPPER_UNSUPPORTED_REASON,
        }),
        Some(
            "registrar" | "registry" | "registry_only" | "registry_owner" | "registrant"
            | "resolver" | "ens_v2_registry",
        ) => json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["permissions_current"],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": Value::Null,
        }),
        Some(_) | None => json!({
            "status": "partial",
            "exhaustiveness": "best_effort",
            "source_classes_considered": ["permissions_current"],
            "enumeration_basis": "resource_permissions",
            "unsupported_reason": UNKNOWN_AUTHORITY_REASON,
        }),
    };
    let source_families = events
        .iter()
        .map(|event| event.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    PermissionsCurrentResourceSummary {
        resource_id: context.resource_id,
        authority_kind,
        root_resource_id,
        coverage,
        provenance: json!({
            "derivation_kind": "permissions_current_resource_summary_rebuild",
            "source_families": source_families,
        }),
        chain_positions,
        canonicality_summary,
        manifest_version: events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .or(context.block_timestamp)
            .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH),
    }
}

fn projected_authority_kind(
    context: &ResourceProjectionContext,
    events: &[RelevantEvent],
) -> Option<String> {
    let event_authority = events.iter().rev().find_map(|event| {
        event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
    });
    let resource_permission_authority = events
        .iter()
        .rev()
        .find_map(resource_permission_authority_kind);
    let resource_authority = context
        .provenance
        .get("authority_kind")
        .and_then(Value::as_str);
    let source_family = context
        .provenance
        .get("source_family")
        .and_then(Value::as_str);

    event_authority
        .or(resource_permission_authority)
        .or(resource_authority)
        .map(normalize_authority_kind)
        .or_else(|| {
            matches!(
                source_family,
                Some(ENSV2_ROOT_SOURCE_FAMILY | ENSV2_REGISTRY_SOURCE_FAMILY)
            )
            .then(|| "ens_v2_registry".to_owned())
        })
}

fn resource_permission_authority_kind(event: &RelevantEvent) -> Option<&str> {
    if event
        .after_state
        .pointer("/scope/kind")
        .and_then(Value::as_str)
        != Some("resource")
    {
        return None;
    }

    event
        .after_state
        .pointer("/grant_source/authority_kind")
        .or_else(|| {
            event
                .after_state
                .pointer("/revocation_source/authority_kind")
        })
        .and_then(Value::as_str)
}

fn normalize_authority_kind(value: &str) -> String {
    match value {
        "name_wrapper" | "wrapper" => "wrapper".to_owned(),
        value => value.to_owned(),
    }
}

fn ensv2_root_resource_id(context: &ResourceProjectionContext) -> Option<Uuid> {
    let provenance = context.provenance.as_object()?;
    let source_family = provenance.get("source_family")?.as_str()?;
    if !matches!(
        source_family,
        ENSV2_ROOT_SOURCE_FAMILY | ENSV2_REGISTRY_SOURCE_FAMILY
    ) {
        return None;
    }
    provenance.get("upstream_resource")?.as_str()?;
    let chain_id = provenance
        .get("chain_id")
        .and_then(Value::as_str)
        .unwrap_or(&context.chain_id);
    let registry_contract_instance_id = provenance
        .get("registry_contract_instance_id")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())?;
    Some(ens_v2_registry_resource_id(
        chain_id,
        registry_contract_instance_id,
        ENSV2_ROOT_UPSTREAM_RESOURCE,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::time::OffsetDateTime;

    fn context(authority_kind: &str) -> ResourceProjectionContext {
        ResourceProjectionContext {
            resource_id: Uuid::nil(),
            chain_id: "ethereum-mainnet".to_owned(),
            block_number: 1,
            block_hash: "0xcontext".to_owned(),
            provenance: json!({"authority_kind": authority_kind}),
            canonicality_state: bigname_storage::CanonicalityState::Finalized,
            block_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    #[test]
    fn unknown_nonempty_authority_kind_stays_partial() {
        let summary = build_resource_summary(&context("future_authority"), &[]);

        assert_eq!(summary.authority_kind.as_deref(), Some("future_authority"));
        assert_eq!(summary.coverage["status"], "partial");
        assert_eq!(summary.coverage["exhaustiveness"], "best_effort");
        assert_eq!(
            summary.coverage["unsupported_reason"],
            UNKNOWN_AUTHORITY_REASON
        );
    }

    #[test]
    fn known_and_wrapper_authorities_publish_explicit_support() {
        let supported = build_resource_summary(&context("registrar"), &[]);
        assert_eq!(supported.coverage["status"], "full");
        assert_eq!(supported.coverage["exhaustiveness"], "authoritative");

        let wrapper = build_resource_summary(&context("wrapper"), &[]);
        assert_eq!(wrapper.coverage["status"], "unsupported");
        assert_eq!(
            wrapper.coverage["unsupported_reason"],
            WRAPPER_UNSUPPORTED_REASON
        );
    }

    #[test]
    fn resource_permission_source_sets_authority_ahead_of_later_resolver_scope() {
        let permission_event = |normalized_event_id, after_state| RelevantEvent {
            normalized_event_id,
            event_kind: "PermissionChanged".to_owned(),
            source_family: "basenames_base_registry".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: "base-mainnet".to_owned(),
            block_number: normalized_event_id,
            block_hash: format!("0x{normalized_event_id}"),
            log_index: Some(0),
            block_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
            raw_fact_ref: json!({}),
            canonicality_state: bigname_storage::CanonicalityState::Finalized,
            after_state,
        };
        let events = [
            permission_event(
                1,
                json!({
                    "scope": {"kind": "resource"},
                    "grant_source": {"authority_kind": "registrant"},
                }),
            ),
            permission_event(
                2,
                json!({
                    "scope": {"kind": "resolver"},
                    "grant_source": {"authority_kind": "resolver"},
                }),
            ),
        ];

        let summary = build_resource_summary(&context(""), &events);

        assert_eq!(summary.authority_kind.as_deref(), Some("registrant"));
        assert_eq!(summary.coverage["status"], "full");
        assert_eq!(summary.coverage["exhaustiveness"], "authoritative");
    }

    #[test]
    fn ensv2_registry_context_publishes_exact_root_anchor_and_root_self_anchor() {
        let registry_contract_instance_id =
            Uuid::parse_str("00000000-0000-0000-0000-00000000e201").unwrap();
        let expected_root_resource_id =
            Uuid::parse_str("882f36f0-b76f-5dd2-9eae-e4d2fe4bb714").unwrap();
        let context_for = |resource_id, upstream_resource: &str| ResourceProjectionContext {
            resource_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_number: 1,
            block_hash: "0xcontext".to_owned(),
            provenance: json!({
                "source_family": ENSV2_REGISTRY_SOURCE_FAMILY,
                "upstream_resource": upstream_resource,
                "registry_contract_instance_id": registry_contract_instance_id,
            }),
            canonicality_state: bigname_storage::CanonicalityState::Finalized,
            block_timestamp: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let resource_summary = build_resource_summary(
            &context_for(
                Uuid::from_u128(0xe202),
                "0x00000000000000000000000000000000000000000000000000000000000073c0",
            ),
            &[],
        );
        assert_eq!(
            resource_summary.root_resource_id,
            Some(expected_root_resource_id)
        );

        let root_summary = build_resource_summary(
            &context_for(expected_root_resource_id, ENSV2_ROOT_UPSTREAM_RESOURCE),
            &[],
        );
        assert_eq!(root_summary.resource_id, expected_root_resource_id);
        assert_eq!(
            root_summary.root_resource_id,
            Some(expected_root_resource_id),
            "the projected root summary must be filterable as its own root stream"
        );
        assert_eq!(
            root_summary.chain_positions["ethereum-mainnet"]["block_hash"],
            "0xcontext"
        );
        assert_eq!(root_summary.canonicality_summary["status"], "finalized");
    }
}
