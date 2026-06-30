use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::NameCurrentRow;
use serde_json::Value as JsonValue;

use crate::AppState;

use super::{
    Envelope, QueryParams, V2Result, bind_path_name, declared_summary_section, diagnostic_envelope,
    empty_object, insert_optional_string_field, insert_value_field, resolve_diagnostic_name,
    unsupported_section,
};

pub(crate) async fn get_name_binding_diagnostic(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_surface_binding_explain_declared_state(&row);

    diagnostic_envelope(data, &selected_snapshot)
}

fn build_name_surface_binding_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "surface_binding",
        build_name_surface_binding_explain_summary(row),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

fn build_name_surface_binding_explain_summary(row: &NameCurrentRow) -> JsonValue {
    let has_binding_summary = row.surface_binding_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared surface binding summary is not yet projected");
    }

    let mut surface_binding = empty_object();
    insert_optional_string_field(
        &mut surface_binding,
        "surface_binding_id",
        row.surface_binding_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut surface_binding,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    surface_binding
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn binding_builder_projects_surface_binding_and_history() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_surface_binding_explain_declared_state(&row),
            json!({
                "surface_binding": {
                    "surface_binding_id": "00000000-0000-0000-0000-000000003300",
                    "binding_kind": "declared_registry_path"
                },
                "history": {
                    "latest_event_kind": "NameTransferred"
                }
            })
        );
    }

    #[test]
    fn binding_builder_reports_unsupported_without_binding_summary() {
        let mut row = super::super::test_name_row();
        row.surface_binding_id = None;
        row.binding_kind = None;

        assert_eq!(
            build_name_surface_binding_explain_declared_state(&row)["surface_binding"],
            json!({
                "status": "unsupported",
                "unsupported_reason": "declared surface binding summary is not yet projected"
            })
        );
    }
}
