use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    PermissionsCurrentResourceSummary, ResourcePermissionCoverage, ens_v2_registry_resource_id,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::canonicality::{build_canonicality_summary, build_chain_positions, format_timestamp};
use super::types::{RelevantEvent, ResourceProjectionContext};

const ENSV2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
const ENSV2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
const ENSV2_ROOT_UPSTREAM_RESOURCE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

pub(super) fn build_resource_summary(
    context: &ResourceProjectionContext,
    events: &[RelevantEvent],
) -> Result<PermissionsCurrentResourceSummary> {
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
        Some("wrapper") => {
            ResourcePermissionCoverage::ensv1_wrapper_holder_permissions_not_projected()
        }
        Some(
            "registrar" | "registry" | "registry_only" | "registry_owner" | "registrant"
            | "resolver" | "ens_v2_registry",
        ) => ResourcePermissionCoverage::authoritative(["permissions_current"]),
        Some(_) | None => ResourcePermissionCoverage::resource_authority_not_projected(),
    };
    let source_families = events
        .iter()
        .map(|event| event.source_family.clone())
        .chain(resource_source_families(&context.provenance).map(str::to_owned))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ensure!(
        !source_families.is_empty(),
        "permissions_current resource summary {} lacks authoritative source-family evidence",
        context.resource_id
    );
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .chain(resource_manifest_versions(&context.provenance))
        .max()
        .filter(|manifest_version| *manifest_version > 0)
        .with_context(|| {
            format!(
                "permissions_current resource summary {} lacks authoritative manifest-version evidence",
                context.resource_id
            )
        })?;
    let last_recomputed_at = events
        .iter()
        .filter_map(|event| event.block_timestamp)
        .chain(context.block_timestamp)
        .max()
        .filter(|timestamp| *timestamp > sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        .with_context(|| {
            format!(
                "permissions_current resource summary {} lacks a source block timestamp",
                context.resource_id
            )
        })?;

    Ok(PermissionsCurrentResourceSummary {
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
        manifest_version,
        last_recomputed_at,
    })
}

fn resource_source_families(provenance: &Value) -> impl Iterator<Item = &str> {
    ["source_family", "binding_source_family"]
        .into_iter()
        .filter_map(|key| provenance.get(key).and_then(Value::as_str))
}

fn resource_manifest_versions(provenance: &Value) -> impl Iterator<Item = i64> + '_ {
    ["manifest_version", "binding_manifest_version"]
        .into_iter()
        .filter_map(|key| provenance.get(key).and_then(Value::as_i64))
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
    let source_family = resource_source_families(&context.provenance).next();

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
            provenance: json!({
                "authority_kind": authority_kind,
                "source_family": "test_permission_authority",
                "manifest_version": 1,
            }),
            canonicality_state: bigname_storage::CanonicalityState::Finalized,
            block_timestamp: Some(OffsetDateTime::from_unix_timestamp(1).unwrap()),
        }
    }

    #[test]
    fn unknown_nonempty_authority_kind_stays_partial() {
        let summary = build_resource_summary(&context("future_authority"), &[]).unwrap();

        assert_eq!(summary.authority_kind.as_deref(), Some("future_authority"));
        assert_eq!(
            summary.coverage,
            ResourcePermissionCoverage::resource_authority_not_projected()
        );
    }

    #[test]
    fn known_and_wrapper_authorities_publish_explicit_support() {
        let supported = build_resource_summary(&context("registrar"), &[]).unwrap();
        assert_eq!(
            supported.coverage,
            ResourcePermissionCoverage::authoritative(["permissions_current"])
        );

        let wrapper = build_resource_summary(&context("wrapper"), &[]).unwrap();
        assert_eq!(
            wrapper.coverage,
            ResourcePermissionCoverage::ensv1_wrapper_holder_permissions_not_projected()
        );
    }

    #[test]
    fn zero_event_ensv1_authority_uses_binding_provenance_fallback() {
        let mut context = context("registrar");
        context.provenance = json!({
            "authority_kind": "registrar",
            "binding_source_family": "ens_v1_registrar_l1",
            "binding_manifest_version": 7,
        });

        let summary = build_resource_summary(&context, &[]).unwrap();

        assert_eq!(summary.manifest_version, 7);
        assert_eq!(
            summary.provenance["source_families"],
            json!(["ens_v1_registrar_l1"])
        );
    }

    #[test]
    fn summary_without_authoritative_manifest_evidence_fails_closed() {
        let mut context = context("registrar");
        context
            .provenance
            .as_object_mut()
            .unwrap()
            .remove("manifest_version");

        let error = build_resource_summary(&context, &[]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("lacks authoritative manifest-version evidence")
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

        let summary = build_resource_summary(&context(""), &events).unwrap();

        assert_eq!(summary.authority_kind.as_deref(), Some("registrant"));
        assert_eq!(
            summary.coverage,
            ResourcePermissionCoverage::authoritative(["permissions_current"])
        );
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
                "manifest_version": 1,
                "upstream_resource": upstream_resource,
                "registry_contract_instance_id": registry_contract_instance_id,
            }),
            canonicality_state: bigname_storage::CanonicalityState::Finalized,
            block_timestamp: Some(OffsetDateTime::from_unix_timestamp(1).unwrap()),
        };

        let resource_summary = build_resource_summary(
            &context_for(
                Uuid::from_u128(0xe202),
                "0x00000000000000000000000000000000000000000000000000000000000073c0",
            ),
            &[],
        )
        .unwrap();
        assert_eq!(
            resource_summary.root_resource_id,
            Some(expected_root_resource_id)
        );

        let root_summary = build_resource_summary(
            &context_for(expected_root_resource_id, ENSV2_ROOT_UPSTREAM_RESOURCE),
            &[],
        )
        .unwrap();
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
