use axum::Json;
use bigname_storage::{NameCurrentRow, SelectedSnapshot};
use serde_json::Value as JsonValue;

use crate::{AppState, load_name_current_for_selected_snapshot, normalize_inferred_route_name};

use super::super::{
    Envelope, Meta, QueryParams, V2Error, V2Result, api_error_to_v2, as_of_meta,
    resolve_v2_snapshot, v2_exact_name_snapshot_scope,
};

mod authority;
mod binding;
mod coverage;

pub(crate) use authority::get_name_authority_diagnostic;
pub(crate) use binding::get_name_binding_diagnostic;
pub(crate) use coverage::get_name_coverage_diagnostic;

async fn resolve_diagnostic_name(
    state: &AppState,
    params: &QueryParams,
) -> V2Result<(NameCurrentRow, SelectedSnapshot)> {
    let input_name = params
        .name
        .as_deref()
        .ok_or_else(|| V2Error::internal_error("diagnostic name path parameter was not bound"))?;
    let normalized = normalize_inferred_route_name(input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    let scope = v2_exact_name_snapshot_scope(state, &namespace).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(api_error_to_v2)?;

    Ok((row, selected_snapshot))
}

fn bind_path_name(input_name: String, mut params: QueryParams) -> QueryParams {
    params.name = Some(input_name);
    params
}

fn diagnostic_envelope(
    data: JsonValue,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<Json<Envelope<JsonValue>>> {
    Ok(Json(Envelope {
        data,
        page: None,
        meta: Meta {
            as_of: Some(as_of_meta(selected_snapshot)?),
            ..Meta::default()
        },
    }))
}

fn empty_object() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn provenance_field<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    value.as_object().and_then(|object| object.get(key))
}

fn string_field(value: Option<&JsonValue>) -> Option<String> {
    match value {
        Some(JsonValue::String(value)) => Some(value.clone()),
        Some(JsonValue::Number(value)) => Some(value.to_string()),
        Some(JsonValue::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn array_or_empty(value: Option<&JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Array(values)) => JsonValue::Array(values.clone()),
        _ => JsonValue::Array(Vec::new()),
    }
}

fn summary_is_unsupported(section: Option<&JsonValue>) -> bool {
    matches!(
        string_field(section.and_then(|value| provenance_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && string_field(section.and_then(|value| provenance_field(value, "unsupported_reason")))
        .is_some()
}

fn unsupported_section(unsupported_reason: &str) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "status", "unsupported".to_owned());
    insert_string_field(
        &mut value,
        "unsupported_reason",
        unsupported_reason.to_owned(),
    );
    value
}

fn insert_string_field(object: &mut JsonValue, key: &str, value: String) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), JsonValue::String(value));
}

fn insert_optional_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(
            key.to_owned(),
            value.map(JsonValue::String).unwrap_or(JsonValue::Null),
        );
}

fn insert_nullable_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    insert_optional_string_field(object, key, value);
}

fn insert_value_field(object: &mut JsonValue, key: &str, value: JsonValue) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), value);
}

fn declared_summary_section(summary: &JsonValue, key: &str, unsupported_reason: &str) -> JsonValue {
    provenance_field(summary, key)
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn declared_authority_section(row: &NameCurrentRow) -> JsonValue {
    if let Some(section) =
        provenance_field(&row.declared_summary, "authority").filter(|value| value.is_object())
    {
        return section.clone();
    }

    let has_binding_summary =
        row.resource_id.is_some() || row.token_lineage_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared authority summary is not yet projected");
    }

    let mut authority = empty_object();
    insert_optional_string_field(
        &mut authority,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    authority
}

fn declared_name_control_section(summary: &JsonValue) -> JsonValue {
    let Some(section) = provenance_field(summary, "control").filter(|value| value.is_object())
    else {
        return unsupported_section("declared control summary is not yet projected");
    };

    if summary_is_unsupported(Some(section)) {
        return section.clone();
    }

    let mut control = empty_object();
    insert_value_field(
        &mut control,
        "registrant",
        provenance_field(section, "registrant")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "registry_owner",
        provenance_field(section, "registry_owner")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "latest_event_kind",
        provenance_field(section, "latest_event_kind")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    control
}

#[cfg(test)]
fn test_name_row() -> NameCurrentRow {
    use serde_json::json;
    use sqlx::types::Uuid;

    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(Uuid::from_u128(0x3300)),
        resource_id: Some(Uuid::from_u128(0x2200)),
        token_lineage_id: Some(Uuid::from_u128(0x1100)),
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "control": {
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "NameTransferred"
            },
            "history": {
                "latest_event_kind": "NameTransferred"
            }
        }),
        provenance: json!({}),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ens_v1_registry_l1"],
            "enumeration_basis": "exact_name",
            "unsupported_reason": null
        }),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: bigname_storage::parse_rfc3339_utc_timestamp("2026-04-17T00:00:03Z")
            .expect("test timestamp must parse"),
    }
}
