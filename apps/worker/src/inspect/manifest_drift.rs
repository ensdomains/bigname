use anyhow::Result;
use bigname_storage::{ManifestDriftAlertInspection, ManifestDriftAlertObservation};
use serde_json::{Value, json};

use super::InspectManifestDriftArgs;
use super::formatting::{canonicality_state_label, format_timestamp};

pub(in crate::inspect) async fn inspect_manifest_drift(
    args: InspectManifestDriftArgs,
) -> Result<()> {
    let _emit_json = args.json;
    let pool = bigname_storage::connect(&args.database).await?;
    let inspection = bigname_storage::list_manifest_drift_alert_observations(&pool).await?;

    println!("{}", render_manifest_drift_inspection(&inspection));
    Ok(())
}

pub(crate) fn render_manifest_drift_alert_observations(
    command: &str,
    read_only: bool,
    inspection: &ManifestDriftAlertInspection,
) -> Value {
    json!({
        "command": command,
        "read_only": read_only,
        "counts": {
            "manifest_code_hash_drift": inspection.code_hash_drift_alerts.len(),
            "manifest_proxy_implementation": inspection.proxy_implementation_alerts.len(),
            "total": inspection.total_alert_count(),
        },
        "manifest_code_hash_drift_alerts": inspection
            .code_hash_drift_alerts
            .iter()
            .map(render_manifest_code_hash_drift_alert)
            .collect::<Vec<_>>(),
        "proxy_implementation_alerts": inspection
            .proxy_implementation_alerts
            .iter()
            .map(render_manifest_proxy_implementation_alert)
            .collect::<Vec<_>>(),
    })
}

pub(in crate::inspect) fn render_manifest_drift_inspection(
    inspection: &ManifestDriftAlertInspection,
) -> Value {
    render_manifest_drift_alert_observations("inspect manifest-drift", true, inspection)
}

fn render_manifest_code_hash_drift_alert(alert: &ManifestDriftAlertObservation) -> Value {
    json!({
        "normalized_event_id": alert.normalized_event_id,
        "event_identity": alert.event_identity.as_str(),
        "event_kind": alert.alert_kind.event_kind(),
        "alert_type": alert.alert_kind.alert_type(),
        "namespace": alert.namespace.as_str(),
        "source_family": alert.source_family.as_str(),
        "manifest_version": alert.manifest_version,
        "source_manifest_id": alert_source_manifest_id(alert),
        "chain": alert_chain(alert),
        "chain_id": alert.chain_id.as_deref(),
        "canonicality_state": canonicality_state_label(alert.canonicality_state),
        "lifecycle": render_manifest_alert_lifecycle(alert),
        "declaration": {
            "kind": alert_state_string(alert, "declaration_kind"),
            "name": alert_state_string(alert, "declaration_name"),
        },
        "contract": {
            "contract_instance_id": alert_state_string(alert, "contract_instance_id"),
            "address": alert_state_string(alert, "address"),
        },
        "code_hash": {
            "expected": alert_state_string(alert, "expected_code_hash"),
            "observed": alert_state_string(alert, "observed_code_hash"),
            "observed_byte_length": alert_state_i64(alert, "observed_code_byte_length"),
        },
        "observed_block": {
            "number": alert.block_number.or_else(|| alert_state_i64(alert, "observed_block_number")),
            "hash": alert.block_hash.as_deref().or_else(|| alert_state_string(alert, "observed_block_hash")),
            "canonicality_state": alert_state_string(alert, "observed_canonicality_state"),
        },
        "watched_target": {
            "source": alert_state_string(alert, "watched_source"),
            "raw_fact_ref": alert.raw_fact_ref.clone(),
        },
        "timestamps": {
            "observed_at": format_timestamp(alert.observed_at),
        },
        "remediation": alert_remediation(alert),
    })
}

fn render_manifest_proxy_implementation_alert(alert: &ManifestDriftAlertObservation) -> Value {
    json!({
        "normalized_event_id": alert.normalized_event_id,
        "event_identity": alert.event_identity.as_str(),
        "event_kind": alert.alert_kind.event_kind(),
        "alert_type": alert.alert_kind.alert_type(),
        "namespace": alert.namespace.as_str(),
        "source_family": alert.source_family.as_str(),
        "manifest_version": alert.manifest_version,
        "source_manifest_id": alert_source_manifest_id(alert),
        "chain": alert_chain(alert),
        "chain_id": alert.chain_id.as_deref(),
        "canonicality_state": canonicality_state_label(alert.canonicality_state),
        "lifecycle": render_manifest_alert_lifecycle(alert),
        "declaration": {
            "name": alert_state_string(alert, "declaration_name"),
            "role": alert_state_string(alert, "role"),
            "proxy_kind": alert_state_string(alert, "proxy_kind"),
        },
        "proxy": {
            "contract_instance_id": alert_state_string(alert, "proxy_contract_instance_id"),
            "address": alert_state_string(alert, "proxy_address"),
        },
        "implementation": {
            "contract_instance_id": alert_state_string(alert, "implementation_contract_instance_id"),
            "address": alert_state_string(alert, "implementation_address"),
        },
        "implementation_edge": {
            "admission": alert_state_string(alert, "admission"),
            "active_from_block_number": alert_state_i64(alert, "active_from_block_number"),
            "active_to_block_number": alert_state_i64(alert, "active_to_block_number"),
            "provenance": alert.alert_state.get("provenance").cloned().unwrap_or(Value::Null),
        },
        "timestamps": {
            "observed_at": format_timestamp(alert.observed_at),
        },
        "remediation": alert_remediation(alert),
    })
}

fn render_manifest_alert_lifecycle(alert: &ManifestDriftAlertObservation) -> Value {
    let status = alert_state_string(alert, "alert_status").unwrap_or("unknown");
    json!({
        "status": status,
        "active": status == "active",
        "remediated": status == "remediated",
    })
}

fn alert_source_manifest_id(alert: &ManifestDriftAlertObservation) -> Option<i64> {
    alert
        .source_manifest_id
        .or_else(|| alert_state_i64(alert, "source_manifest_id"))
        .or_else(|| {
            alert
                .raw_fact_ref
                .get("manifest_id")
                .and_then(Value::as_i64)
        })
}

fn alert_chain(alert: &ManifestDriftAlertObservation) -> Option<&str> {
    alert_state_string(alert, "chain").or(alert.chain_id.as_deref())
}

fn alert_state_string<'a>(
    alert: &'a ManifestDriftAlertObservation,
    field: &str,
) -> Option<&'a str> {
    alert.alert_state.get(field).and_then(Value::as_str)
}

fn alert_state_i64(alert: &ManifestDriftAlertObservation, field: &str) -> Option<i64> {
    alert.alert_state.get(field).and_then(Value::as_i64)
}

fn alert_remediation(alert: &ManifestDriftAlertObservation) -> Value {
    ["remediation", "remediation_metadata"]
        .iter()
        .find_map(|field| alert.alert_state.get(*field).cloned())
        .unwrap_or(Value::Null)
}
