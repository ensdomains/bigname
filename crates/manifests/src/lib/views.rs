#[path = "views/abi.rs"]
mod abi;
#[path = "views/bootstrap.rs"]
mod bootstrap;
#[path = "views/drift.rs"]
mod drift;
#[path = "views/execution_owner.rs"]
mod execution_owner;
#[path = "views/resolver_profiles.rs"]
mod resolver_profiles;
#[path = "views/snapshot.rs"]
mod snapshot;
#[path = "views/types.rs"]
mod types;
#[path = "views/watched.rs"]
mod watched;

pub use abi::*;
pub use bootstrap::*;
pub use drift::*;
pub use execution_owner::*;
pub use resolver_profiles::*;
pub use snapshot::*;
pub use types::*;
pub use watched::*;
// Keep the legacy generation-bound fact scans available to this crate's tests
// without exporting a production path that bypasses the storage-owned rollup.
#[allow(dead_code, hidden_glob_reexports, unused_imports)]
pub(crate) use watched::{
    find_uncovered_required_watched_tuples_for_retention_generation,
    find_uncovered_required_watched_tuples_for_retention_generation_in_transaction,
};
