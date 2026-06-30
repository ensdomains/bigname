use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value as JsonValue;

use crate::{AppState, responses::build_name_surface_binding_explain_declared_state};

use super::{
    Envelope, QueryParams, V2Result, bind_path_name, diagnostic_envelope, resolve_diagnostic_name,
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
