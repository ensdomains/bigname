use serde_json::json;
use sqlx::types::JsonValue;

use super::schema_ref;

pub(super) fn health_projection_publication_versions_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["permissions_current"],
        "properties": {
            "permissions_current": { "type": "integer", "minimum": 1 },
        },
    })
}

pub(super) fn health_identity_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "version",
            "build_sha",
            "schema_migration_version",
            "projection_replay_version",
            "projection_publication_versions",
        ],
        "properties": {
            "version": { "type": "string" },
            "build_sha": { "type": "string" },
            "schema_migration_version": { "type": "integer", "minimum": 0 },
            "projection_replay_version": { "type": "integer", "minimum": 1 },
            "projection_publication_versions": schema_ref("HealthProjectionPublicationVersions"),
        },
    })
}

pub(super) fn health_process_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status"],
        "properties": {
            "status": { "type": "string" },
        },
    })
}

pub(super) fn health_database_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["status", "reachable", "check", "error"],
        "properties": {
            "status": { "type": "string" },
            "reachable": { "type": "boolean" },
            "check": { "type": "string" },
            "error": { "type": ["string", "null"] },
        },
    })
}

pub(super) fn health_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["service", "identity", "status", "process", "database"],
        "properties": {
            "service": { "type": "string" },
            "identity": schema_ref("HealthIdentity"),
            "status": { "type": "string" },
            "process": schema_ref("HealthProcess"),
            "database": schema_ref("HealthDatabase"),
        },
    })
}
