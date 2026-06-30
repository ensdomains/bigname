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
