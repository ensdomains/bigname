//! Schema-v2 block-by-block current-state projection and atomic family publication.

mod error;
pub mod families;

pub use error::{ErrorKind, ProjectError, Result};
pub use families::EXCLUDED_CHILD_REGISTRATION_PARENTS;

/// A block number and hash on one chain's readable lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}
