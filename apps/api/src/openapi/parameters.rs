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

pub(super) fn namespace_path_parameter() -> JsonValue {
    path_parameter(
        "namespace",
        "Supported namespace identifier.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

pub(super) fn name_path_parameter() -> JsonValue {
    path_parameter(
        "name",
        "Normalized name within the namespace.",
        json!({
            "type": "string",
        }),
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
        json!({
            "type": "string",
        }),
    ));
    parameters
}

fn at_query_parameter(description: &'static str) -> JsonValue {
    query_parameter(
        "at",
        description,
        json!({
            "type": "string",
        }),
    )
}

fn chain_positions_query_parameter() -> JsonValue {
    query_parameter(
        "chain_positions",
        "Explicit exact-name snapshot selector encoded as one JSON object using ChainPositions position objects. Mutually exclusive with `at`.",
        json!({
            "type": "string",
        }),
    )
}

fn consistency_query_parameter() -> JsonValue {
    query_parameter(
        "consistency",
        "Snapshot consistency floor. Defaults to `head`.",
        json!({
            "type": "string",
            "enum": ["head", "safe", "finalized"],
            "default": "head",
        }),
    )
}

pub(super) fn address_path_parameter() -> JsonValue {
    path_parameter(
        "address",
        "Address anchor for the collection or history read. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

pub(super) fn resource_id_path_parameter() -> JsonValue {
    path_parameter(
        "resource_id",
        "Resource identifier anchor.",
        json!({
            "type": "string",
            "format": "uuid",
        }),
    )
}

pub(super) fn chain_id_path_parameter() -> JsonValue {
    path_parameter(
        "chain_id",
        "Resolver chain identifier.",
        json!({
            "type": "string",
        }),
    )
}

pub(super) fn resolver_address_path_parameter() -> JsonValue {
    path_parameter(
        "resolver_address",
        "Resolver address anchor. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

pub(super) fn namespace_query_parameter() -> JsonValue {
    query_parameter(
        "namespace",
        "Optional namespace filter.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

pub(super) fn required_namespace_query_parameter() -> JsonValue {
    required_query_parameter(
        "namespace",
        "Required namespace identifier for the requested primary-name tuple.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

pub(super) fn relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional relation facet filter.",
        json!({
            "type": "string",
            "enum": ["registrant", "token_holder", "effective_controller"],
        }),
    )
}

pub(super) fn dedupe_by_query_parameter() -> JsonValue {
    query_parameter(
        "dedupe_by",
        "Current collection dedupe basis.",
        json!({
            "type": "string",
            "enum": ["surface", "resource"],
            "default": "surface",
        }),
    )
}

pub(super) fn history_scope_query_parameter() -> JsonValue {
    query_parameter(
        "scope",
        "History scope selector.",
        json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }),
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
        json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }),
    )
}

pub(super) fn primary_name_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Primary-name read mode.",
        json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }),
    )
}

include!("app_facing_parameters.rs");

pub(super) fn required_coin_type_query_parameter() -> JsonValue {
    required_query_parameter(
        "coin_type",
        "Required `coin_type` selector for the requested primary-name tuple.",
        json!({
            "type": "string",
            "pattern": "^[0-9]+$",
        }),
    )
}

pub(super) fn view_query_parameter(default: &'static str) -> JsonValue {
    query_parameter(
        "view",
        "Response view selector.",
        json!({
            "type": "string",
            "enum": ["compact", "full"],
            "default": default,
        }),
    )
}

pub(super) fn meta_query_parameter(default: &'static str) -> JsonValue {
    query_parameter(
        "meta",
        "Compact response metadata selector.",
        json!({
            "type": "string",
            "enum": ["none", "summary", "full"],
            "default": default,
        }),
    )
}

pub(super) fn cursor_query_parameter() -> JsonValue {
    query_parameter(
        "cursor",
        "Replay-stable pagination cursor.",
        json!({
            "type": "string",
        }),
    )
}

pub(super) fn page_size_query_parameter() -> JsonValue {
    query_parameter(
        "page_size",
        format!("Optional page size. When supplied it must be between 1 and {MAX_PAGE_SIZE}."),
        json!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_PAGE_SIZE,
        }),
    )
}
