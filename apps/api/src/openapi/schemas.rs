use serde_json::json;
use sqlx::types::JsonValue;

use crate::PUBLIC_NAMESPACES;

use super::responses::{
    declared_response_schema, mixed_response_schema, paginated_declared_response_schema,
    primary_name_response_schema,
};

pub(super) fn openapi_components() -> JsonValue {
    json!({
        "schemas": {
            "JsonObject": json_object_schema(),
            "NullValue": json!({ "type": "null" }),
            "Consistency": json!({
                "type": "string",
                "enum": ["head", "safe", "finalized"],
            }),
            "Provenance": json!({
                "type": "object",
                "required": [
                    "normalized_event_ids",
                    "raw_fact_refs",
                    "manifest_versions",
                    "execution_trace_id",
                    "derivation_kind",
                ],
                "properties": {
                    "normalized_event_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "raw_fact_refs": {
                        "type": "array",
                        "items": {},
                    },
                    "manifest_versions": {
                        "type": "array",
                        "items": {},
                    },
                    "execution_trace_id": {
                        "type": ["string", "null"],
                    },
                    "derivation_kind": {
                        "type": "string",
                    },
                },
            }),
            "CoverageResponse": json!({
                "type": "object",
                "required": [
                    "status",
                    "exhaustiveness",
                    "source_classes_considered",
                    "enumeration_basis",
                    "unsupported_reason",
                ],
                "properties": {
                    "status": { "type": "string" },
                    "exhaustiveness": { "type": "string" },
                    "source_classes_considered": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "enumeration_basis": { "type": "string" },
                    "unsupported_reason": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ChainPositionResponse": json!({
                "type": "object",
                "required": ["chain_id", "block_number", "block_hash", "timestamp"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "block_number": { "type": "integer" },
                    "block_hash": { "type": "string" },
                    "timestamp": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
            }),
            "ChainPositions": json!({
                "type": "object",
                "additionalProperties": schema_ref("ChainPositionResponse"),
            }),
            "HistoryPageResponse": json!({
                "type": "object",
                "required": ["cursor", "next_cursor", "page_size", "sort"],
                "properties": {
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "page_size": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "sort": { "type": "string" },
                },
            }),
            "ExactNameData": json!({
                "type": "object",
                "required": [
                    "logical_name_id",
                    "namespace",
                    "normalized_name",
                    "canonical_display_name",
                    "namehash",
                    "resource_id",
                    "token_lineage_id",
                    "binding_kind",
                ],
                "properties": {
                    "logical_name_id": { "type": "string" },
                    "namespace": { "type": "string" },
                    "normalized_name": { "type": "string" },
                    "canonical_display_name": { "type": "string" },
                    "namehash": { "type": "string" },
                    "resource_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "token_lineage_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "binding_kind": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ResolverData": json!({
                "type": "object",
                "required": ["chain_id", "resolver_address"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "resolver_address": { "type": "string" },
                },
            }),
            "PrimaryNameData": json!({
                "type": "object",
                "required": ["address", "namespace", "coin_type"],
                "properties": {
                    "address": { "type": "string" },
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                    "coin_type": { "type": "string" },
                },
            }),
            "PrimaryNameClaimedResult": primary_name_claimed_result_schema(),
            "PrimaryNameDeclaredState": json!({
                "type": "object",
                "required": ["claimed_primary_name"],
                "properties": {
                    "claimed_primary_name": schema_ref("PrimaryNameClaimedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedState": json!({
                "type": "object",
                "required": ["verified_primary_name"],
                "properties": {
                    "verified_primary_name": schema_ref("PrimaryNameVerifiedResult"),
                },
                "additionalProperties": false,
            }),
            "PrimaryNameVerifiedResult": primary_name_verified_result_schema(),
            "PrimaryNameVerifiedResultProvenance": primary_name_verified_result_provenance_schema(),
            "ExactNameResponse": declared_response_schema(
                schema_ref("ExactNameData"),
                schema_ref("JsonObject"),
            ),
            "ResolverResponse": declared_response_schema(
                schema_ref("ResolverData"),
                schema_ref("JsonObject"),
            ),
            "ResolutionResponse": mixed_response_schema(schema_ref("ExactNameData")),
            "PrimaryNameResponse": primary_name_response_schema(),
            "CollectionResponse": paginated_declared_response_schema(
                json!({
                    "type": "array",
                    "items": schema_ref("JsonObject"),
                }),
                schema_ref("JsonObject"),
            ),
            "NamespaceData": json!({
                "type": "object",
                "required": ["namespace"],
                "properties": {
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                },
            }),
            "NamespaceMetadataDeclaredState": json!({
                "type": "object",
                "required": [
                    "active_manifest_count",
                    "active_source_families",
                    "chains",
                    "normalizer_versions",
                ],
                "properties": {
                    "active_manifest_count": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "active_source_families": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "chains": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "normalizer_versions": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                },
            }),
            "NamespaceMetadataResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceMetadataDeclaredState"),
            ),
            "CapabilityFlag": json!({
                "type": "object",
                "required": ["status", "notes"],
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["unsupported", "shadow", "supported"],
                    },
                    "notes": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "NamespaceManifestEntry": json!({
                "type": "object",
                "required": [
                    "manifest_version",
                    "source_family",
                    "chain",
                    "deployment_epoch",
                    "normalizer_version",
                    "capability_flags",
                ],
                "properties": {
                    "manifest_version": {
                        "type": "integer",
                        "minimum": 1,
                    },
                    "source_family": { "type": "string" },
                    "chain": { "type": "string" },
                    "deployment_epoch": { "type": "string" },
                    "normalizer_version": { "type": "string" },
                    "capability_flags": {
                        "type": "object",
                        "additionalProperties": schema_ref("CapabilityFlag"),
                    },
                },
            }),
            "NamespaceManifestsDeclaredState": json!({
                "type": "object",
                "required": ["manifests"],
                "properties": {
                    "manifests": {
                        "type": "array",
                        "items": schema_ref("NamespaceManifestEntry"),
                    },
                },
            }),
            "NamespaceManifestsResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceManifestsDeclaredState"),
            ),
            "HealthResponse": json!({
                "type": "object",
                "required": ["service", "phase", "status"],
                "properties": {
                    "service": { "type": "string" },
                    "phase": { "type": "string" },
                    "status": { "type": "string" },
                },
            }),
            "ErrorBody": json!({
                "type": "object",
                "required": ["code", "message", "details"],
                "properties": {
                    "code": { "type": "string" },
                    "message": { "type": "string" },
                    "details": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                    },
                },
            }),
            "ErrorResponse": json!({
                "type": "object",
                "required": ["error"],
                "properties": {
                    "error": schema_ref("ErrorBody"),
                },
            }),
        },
    })
}
fn primary_name_claimed_result_schema() -> JsonValue {
    json!({
        "oneOf": [
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "success",
                    },
                    "name": {
                        "type": "string",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "not_found",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "unsupported_reason": {
                        "type": "string",
                    },
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "unsupported",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
            json!({
                "type": "object",
                "required": ["status", "raw_claim_name", "provenance"],
                "properties": {
                    "status": {
                        "type": "string",
                        "const": "invalid_name",
                    },
                    "raw_claim_name": {
                        "type": "string",
                    },
                    "provenance": schema_ref("JsonObject"),
                },
                "additionalProperties": false,
            }),
        ],
    })
}

fn primary_name_verified_result_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": {
                "type": "string",
            },
            "provenance": schema_ref("PrimaryNameVerifiedResultProvenance"),
        },
        "additionalProperties": true,
    })
}

fn primary_name_verified_result_provenance_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["manifest_versions", "execution_trace_id"],
        "properties": {
            "manifest_versions": {
                "type": "array",
                "items": {},
            },
            "execution_trace_id": {
                "type": "string",
            },
        },
        "additionalProperties": false,
    })
}
pub(super) fn schema_ref(schema_name: &str) -> JsonValue {
    json!({
        "$ref": format!("#/components/schemas/{schema_name}"),
    })
}

pub(super) fn nullable_ref_schema(schema_name: &str) -> JsonValue {
    json!({
        "anyOf": [
            schema_ref(schema_name),
            schema_ref("NullValue"),
        ],
    })
}

fn json_object_schema() -> JsonValue {
    json!({
        "type": "object",
        "additionalProperties": true,
    })
}
