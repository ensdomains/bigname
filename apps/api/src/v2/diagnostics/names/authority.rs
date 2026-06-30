use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value as JsonValue;

use crate::{AppState, responses::build_name_authority_control_explain_declared_state};

use super::{
    Envelope, QueryParams, V2Result, bind_path_name, diagnostic_envelope, resolve_diagnostic_name,
};

pub(crate) async fn get_name_authority_diagnostic(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_authority_control_explain_declared_state(&row);

    diagnostic_envelope(data, &selected_snapshot)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn authority_builder_projects_authority_fallback_and_control() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_authority_control_explain_declared_state(&row),
            json!({
                "authority": {
                    "resource_id": "00000000-0000-0000-0000-000000002200",
                    "token_lineage_id": "00000000-0000-0000-0000-000000001100",
                    "binding_kind": "declared_registry_path"
                },
                "control": {
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "registry_owner": "0x00000000000000000000000000000000000000bb",
                    "latest_event_kind": "NameTransferred"
                }
            })
        );
    }

    #[test]
    fn authority_builder_keeps_projected_authority_section() {
        let mut row = super::super::test_name_row();
        row.declared_summary["authority"] = json!({
            "resource_id": "projected-resource",
            "permission_lineage": ["registry-owner"]
        });

        assert_eq!(
            build_name_authority_control_explain_declared_state(&row)["authority"],
            json!({
                "resource_id": "projected-resource",
                "permission_lineage": ["registry-owner"]
            })
        );
    }
}
