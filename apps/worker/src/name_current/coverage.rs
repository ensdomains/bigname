use serde_json::{Value, json};

use super::types::RelevantEvent;
use super::{
    BASENAMES_NAMESPACE, CAPABILITY_STATUS_SUPPORTED, ENS_NAMESPACE, ETHEREUM_SEPOLIA_CHAIN_ID,
    MANIFEST_ROLLOUT_STATUS_ACTIVE, SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
};

pub(super) fn build_exact_name_coverage(namespace: &str, events: &[RelevantEvent]) -> Value {
    if namespace == ENS_NAMESPACE {
        let has_ens_v2 = events.iter().any(|event| {
            matches!(
                event.source_family.as_str(),
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1 | SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
            )
        });
        let has_ens_v1 = events
            .iter()
            .any(|event| event.source_family.starts_with("ens_v1_"));
        if has_ens_v2 && has_ens_v1 {
            return json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": ens_v2_exact_name_coverage_source_classes(),
                "unsupported_reason": "mixed_ensv1_ensv2_exact_name_corpus",
                "enumeration_basis": "exact_name_profile",
            });
        }
        if has_ens_v2 && ens_v2_sepolia_post_audit_exact_name_supported(events) {
            return json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ens_v2_exact_name_coverage_source_classes(),
                "unsupported_reason": Value::Null,
                "enumeration_basis": "exact_name_profile",
            });
        }
        if has_ens_v2 {
            return json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": ["ensv2_registry_resource_surface"],
                "unsupported_reason": "ensv2_exact_name_profile_shadow",
                "enumeration_basis": "exact_name",
            });
        }
    }

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": exact_name_coverage_source_classes(namespace),
        "unsupported_reason": Value::Null,
        "enumeration_basis": "exact_name",
    })
}

fn ens_v2_sepolia_post_audit_exact_name_supported(events: &[RelevantEvent]) -> bool {
    let mut has_registry = false;
    let mut has_supported_registrar = false;

    for event in events
        .iter()
        .filter(|event| event.source_family.starts_with("ens_v2_"))
        .filter(|event| ens_v2_event_uses_active_selected_exact_name_manifest(event))
    {
        match event.source_family.as_str() {
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
                has_registry = true;
            }
            SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
                if event.exact_name_profile_status.as_deref()
                    == Some(CAPABILITY_STATUS_SUPPORTED) =>
            {
                has_supported_registrar = true;
            }
            _ => {}
        }
    }

    has_registry && has_supported_registrar
}

fn ens_v2_event_uses_active_selected_exact_name_manifest(event: &RelevantEvent) -> bool {
    event.source_manifest_id.is_some()
        && event.chain_id.as_deref() == Some(ETHEREUM_SEPOLIA_CHAIN_ID)
        && event.source_manifest_version == Some(event.manifest_version)
        && event.source_manifest_namespace.as_deref() == Some(ENS_NAMESPACE)
        && event.source_manifest_source_family.as_deref() == Some(event.source_family.as_str())
        && event.source_manifest_chain.as_deref() == Some(ETHEREUM_SEPOLIA_CHAIN_ID)
        && event.source_manifest_deployment_epoch.as_deref()
            == Some(SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH)
        && event.source_manifest_rollout_status.as_deref() == Some(MANIFEST_ROLLOUT_STATUS_ACTIVE)
}

fn ens_v2_exact_name_coverage_source_classes() -> &'static [&'static str] {
    &[
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
    ]
}

fn exact_name_coverage_source_classes(namespace: &str) -> &'static [&'static str] {
    match namespace {
        ENS_NAMESPACE | BASENAMES_NAMESPACE => &["ensv1_registry_path"],
        _ => &[],
    }
}
