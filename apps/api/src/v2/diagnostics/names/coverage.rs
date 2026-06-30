use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value as JsonValue;

use crate::AppState;

use super::{
    Envelope, QueryParams, V2Result, array_or_empty, bind_path_name, diagnostic_envelope,
    empty_object, insert_nullable_string_field, insert_string_field, insert_value_field,
    provenance_field, resolve_diagnostic_name, string_field,
};

pub(crate) async fn get_name_coverage_diagnostic(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let params = bind_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let data = build_name_coverage_declared_state(&row.coverage);

    diagnostic_envelope(data, &selected_snapshot)
}

fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_string_field(
        &mut normalized,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut normalized,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut normalized,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut normalized,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut normalized,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    normalized
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn coverage_builder_normalizes_declared_coverage() {
        let row = super::super::test_name_row();

        assert_eq!(
            build_name_coverage_declared_state(&row.coverage),
            json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["ens_v1_registry_l1"],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            })
        );
    }

    #[test]
    fn coverage_builder_defaults_missing_fields_to_unsupported_shape() {
        assert_eq!(
            build_name_coverage_declared_state(&json!({})),
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_applicable",
                "source_classes_considered": [],
                "enumeration_basis": "exact_name",
                "unsupported_reason": null
            })
        );
    }
}
