use serde_json::json;
use sqlx::types::JsonValue;

use crate::{MAX_PAGE_SIZE, PUBLIC_NAMESPACES};

fn path_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "description": description.into(),
        "schema": schema,
    })
}

pub(super) fn query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "query",
        "required": false,
        "description": description.into(),
        "schema": schema,
    })
}

fn required_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = query_parameter(name, description, schema);
    parameter
        .as_object_mut()
        .expect("required query parameter helper must create an object")
        .insert("required".to_owned(), JsonValue::Bool(true));
    parameter
}

pub(super) fn csv_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = query_parameter(name, description, schema);
    let object = parameter
        .as_object_mut()
        .expect("query parameter helper must create an object");
    object.insert("style".to_owned(), JsonValue::String("form".to_owned()));
    object.insert("explode".to_owned(), JsonValue::Bool(false));
    parameter
}

pub(super) fn required_csv_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = csv_query_parameter(name, description, schema);
    parameter
        .as_object_mut()
        .expect("required CSV query parameter helper must create an object")
        .insert("required".to_owned(), JsonValue::Bool(true));
    parameter
}

fn string_schema() -> JsonValue {
    json!({ "type": "string" })
}

fn boolean_schema() -> JsonValue {
    json!({ "type": "boolean" })
}

fn uuid_string_schema() -> JsonValue {
    json!({ "type": "string", "format": "uuid" })
}

fn string_pattern_schema(pattern: &'static str) -> JsonValue {
    json!({ "type": "string", "pattern": pattern })
}

fn string_enum_schema(values: &[&str]) -> JsonValue {
    json!({ "type": "string", "enum": values })
}

fn string_enum_default_schema(values: &[&str], default: &'static str) -> JsonValue {
    json!({ "type": "string", "enum": values, "default": default })
}

fn integer_min_schema(minimum: i64) -> JsonValue {
    json!({ "type": "integer", "minimum": minimum })
}

fn integer_range_schema(minimum: i64, maximum: u64) -> JsonValue {
    json!({ "type": "integer", "minimum": minimum, "maximum": maximum })
}

pub(super) fn namespace_path_parameter() -> JsonValue {
    path_parameter(
        "namespace",
        "Supported namespace identifier.",
        string_enum_schema(PUBLIC_NAMESPACES),
    )
}

pub(super) fn name_path_parameter() -> JsonValue {
    path_parameter(
        "name",
        "Normalized name within the namespace.",
        string_schema(),
    )
}

pub(super) fn exact_name_snapshot_parameters(at_description: &'static str) -> Vec<JsonValue> {
    vec![
        namespace_path_parameter(),
        name_path_parameter(),
        at_query_parameter(at_description),
        chain_positions_query_parameter(),
        consistency_query_parameter(),
    ]
}

pub(super) fn resolution_current_parameters() -> Vec<JsonValue> {
    let mut parameters = exact_name_snapshot_parameters(
        "Point-in-time selector for the exact-name snapshot used by resolution joins. Mutually exclusive with `chain_positions`.",
    );
    parameters.push(resolution_mode_query_parameter());
    parameters.push(csv_query_parameter(
        "records",
        "Comma-separated record selectors. Required when `mode` is `verified` or `both`.",
        string_schema(),
    ));
    parameters
}

fn at_query_parameter(description: &'static str) -> JsonValue {
    query_parameter(
        "at",
        description,
        string_schema(),
    )
}

fn chain_positions_query_parameter() -> JsonValue {
    query_parameter(
        "chain_positions",
        "Explicit exact-name snapshot selector encoded as one JSON object using ChainPositions position objects. Mutually exclusive with `at`.",
        string_schema(),
    )
}

fn consistency_query_parameter() -> JsonValue {
    query_parameter(
        "consistency",
        "Snapshot consistency floor. Defaults to `head`.",
        string_enum_default_schema(&["head", "safe", "finalized"], "head"),
    )
}

pub(super) fn address_path_parameter() -> JsonValue {
    path_parameter(
        "address",
        "Address anchor for the collection or history read. Addresses are normalized to lowercase.",
        string_schema(),
    )
}

pub(super) fn resource_id_path_parameter() -> JsonValue {
    path_parameter(
        "resource_id",
        "Resource identifier anchor.",
        uuid_string_schema(),
    )
}

pub(super) fn chain_id_path_parameter() -> JsonValue {
    path_parameter(
        "chain_id",
        "Resolver chain identifier.",
        string_schema(),
    )
}

pub(super) fn resolver_address_path_parameter() -> JsonValue {
    path_parameter(
        "resolver_address",
        "Resolver address anchor. Addresses are normalized to lowercase.",
        string_schema(),
    )
}

pub(super) fn namespace_query_parameter() -> JsonValue {
    query_parameter(
        "namespace",
        "Optional namespace filter.",
        string_enum_schema(PUBLIC_NAMESPACES),
    )
}

pub(super) fn required_namespace_query_parameter() -> JsonValue {
    required_query_parameter(
        "namespace",
        "Required namespace identifier for the requested primary-name tuple.",
        string_enum_schema(PUBLIC_NAMESPACES),
    )
}

pub(super) fn relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional relation facet filter.",
        string_enum_schema(&["registrant", "token_holder", "effective_controller"]),
    )
}

pub(super) fn dedupe_by_query_parameter() -> JsonValue {
    query_parameter(
        "dedupe_by",
        "Current collection dedupe basis.",
        string_enum_default_schema(&["surface", "resource"], "surface"),
    )
}

pub(super) fn history_scope_query_parameter() -> JsonValue {
    query_parameter(
        "scope",
        "History scope selector.",
        string_enum_default_schema(&["surface", "resource", "both"], "both"),
    )
}

pub(super) fn history_view_query_parameter() -> JsonValue {
    view_query_parameter("full")
}

pub(super) fn history_meta_query_parameter() -> JsonValue {
    meta_query_parameter("summary")
}

pub(super) fn resolution_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Resolution read mode.",
        string_enum_default_schema(&["declared", "verified", "both"], "declared"),
    )
}

pub(super) fn primary_name_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Primary-name read mode.",
        string_enum_default_schema(&["declared", "verified", "both"], "declared"),
    )
}

include!("app_facing_parameters.rs");

pub(super) fn required_coin_type_query_parameter() -> JsonValue {
    required_query_parameter(
        "coin_type",
        "Required `coin_type` selector for the requested primary-name tuple.",
        string_pattern_schema("^[0-9]+$"),
    )
}

pub(super) fn view_query_parameter(default: &'static str) -> JsonValue {
    query_parameter(
        "view",
        "Response view selector.",
        string_enum_default_schema(&["compact", "full"], default),
    )
}

pub(super) fn compact_view_query_parameter() -> JsonValue {
    query_parameter(
        "view",
        "Compact response view selector. `view=full` remains a compatibility-reserved value and returns `400 invalid_input`.",
        string_enum_default_schema(&["compact"], "compact"),
    )
}

pub(super) fn meta_query_parameter(default: &'static str) -> JsonValue {
    query_parameter(
        "meta",
        "Compact response metadata selector.",
        string_enum_default_schema(&["none", "summary", "full"], default),
    )
}

pub(super) fn cursor_query_parameter() -> JsonValue {
    query_parameter(
        "cursor",
        "Replay-stable pagination cursor.",
        string_schema(),
    )
}

pub(super) fn page_size_query_parameter() -> JsonValue {
    query_parameter(
        "page_size",
        format!("Optional page size. When supplied it must be between 1 and {MAX_PAGE_SIZE}."),
        integer_range_schema(1, MAX_PAGE_SIZE),
    )
}
