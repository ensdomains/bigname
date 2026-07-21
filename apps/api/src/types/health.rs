use serde::Serialize;

use crate::{BUILD_SHA, SOFTWARE_VERSION};

#[derive(Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) service: &'static str,
    pub(crate) identity: HealthIdentityResponse,
    pub(crate) status: &'static str,
    pub(crate) process: HealthProcessResponse,
    pub(crate) database: HealthDatabaseResponse,
    pub(crate) loops: HealthLoopsResponse,
}

#[derive(Serialize)]
pub(crate) struct HealthIdentityResponse {
    pub(crate) version: &'static str,
    pub(crate) build_sha: &'static str,
    pub(crate) schema_migration_version: i64,
    pub(crate) projection_replay_version: i32,
    pub(crate) projection_publication_versions: HealthProjectionPublicationVersions,
}

impl HealthIdentityResponse {
    pub(crate) fn current() -> Self {
        Self {
            version: SOFTWARE_VERSION,
            build_sha: BUILD_SHA,
            schema_migration_version: bigname_storage::latest_migration_version(),
            projection_replay_version: bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
            projection_publication_versions: HealthProjectionPublicationVersions {
                permissions_current: bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION,
            },
        }
    }
}

#[derive(Serialize)]
pub(crate) struct HealthProjectionPublicationVersions {
    pub(crate) permissions_current: i32,
}

#[derive(Serialize)]
pub(crate) struct HealthProcessResponse {
    pub(crate) status: &'static str,
}

#[derive(Serialize)]
pub(crate) struct HealthDatabaseResponse {
    pub(crate) status: &'static str,
    pub(crate) reachable: bool,
    pub(crate) check: &'static str,
    pub(crate) error: Option<&'static str>,
}

#[derive(Serialize)]
pub(crate) struct HealthLoopsResponse {
    pub(crate) indexer: HealthLoopResponse,
    pub(crate) worker: HealthLoopResponse,
}

#[derive(Serialize)]
pub(crate) struct HealthLoopResponse {
    pub(crate) status: &'static str,
    pub(crate) started_at: Option<String>,
    pub(crate) heartbeat_at: Option<String>,
    pub(crate) heartbeat_age_seconds: Option<i64>,
    pub(crate) max_age_seconds: i64,
}
