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
            "version": {
                "type": "string",
                "description": "Cargo package version compiled into this binary; not live database state.",
            },
            "build_sha": {
                "type": "string",
                "description": "Source commit identifier compiled into this binary, or \"unknown\" when unavailable; not live database state.",
            },
            "schema_migration_version": {
                "type": "integer",
                "minimum": 0,
                "description": "Latest migration version compiled into this binary; not the database's applied state.",
            },
            "projection_replay_version": {
                "type": "integer",
                "minimum": 1,
                "description": "Projection replay compatibility version compiled into this binary; not the database's applied replay state.",
            },
            "projection_publication_versions": {
                "$ref": "#/components/schemas/HealthProjectionPublicationVersions",
                "description": "Projection publication compatibility versions compiled into this binary; not the database's applied publication state.",
            },
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

pub(super) fn health_loop_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "status",
            "started_at",
            "heartbeat_at",
            "heartbeat_age_seconds",
            "max_age_seconds",
        ],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["running", "stale", "not_started", "unavailable"],
            },
            "started_at": { "type": ["string", "null"], "format": "date-time" },
            "heartbeat_at": { "type": ["string", "null"], "format": "date-time" },
            "heartbeat_age_seconds": { "type": ["integer", "null"], "minimum": 0 },
            "max_age_seconds": { "type": "integer", "minimum": 1 },
        },
    })
}

pub(super) fn health_loops_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["indexer", "worker"],
        "properties": {
            "indexer": schema_ref("HealthLoop"),
            "worker": schema_ref("HealthLoop"),
        },
    })
}

pub(super) fn health_response_schema() -> JsonValue {
    json!({
        "type": "object",
        "required": ["service", "identity", "status", "process", "database", "loops"],
        "properties": {
            "service": { "type": "string" },
            "identity": schema_ref("HealthIdentity"),
            "status": { "type": "string" },
            "process": schema_ref("HealthProcess"),
            "database": schema_ref("HealthDatabase"),
            "loops": schema_ref("HealthLoops"),
        },
    })
}
