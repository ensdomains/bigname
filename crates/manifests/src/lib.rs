//! Repository manifest loading, schema-v2 persistence, and runtime views.

#[path = "lib/attribution.rs"]
mod attribution;
#[path = "lib/model.rs"]
mod model;
#[path = "lib/repository.rs"]
mod repository;
#[path = "lib/role_insensitivity.rs"]
mod role_insensitivity;
mod schema_v2;
#[cfg(test)]
#[path = "lib/tests.rs"]
mod tests;
#[path = "lib/views.rs"]
mod views;
#[path = "lib/watch_policy.rs"]
mod watch_policy;

const REACHABLE_FROM_ROOT_ADMISSION: &str = "reachable_from_root";

pub use model::*;
pub use repository::load_repository;
pub use role_insensitivity::{
    ROLE_INSENSITIVE_EVENTS, RoleInsensitiveEvent, event_allows_empty_emitter_roles,
    role_insensitivity_justification,
};
pub use schema_v2::{SchemaV2ManifestSyncSummary, sync_schema_v2_repository};
pub use views::*;
pub use watch_policy::*;

pub(crate) use repository::normalize_address;
