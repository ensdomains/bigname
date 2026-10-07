//! Stable lookup facts composed at Project's publication. The API joins identity spelling and
//! publication positions when reading these facts; provider responses and primary claims are
//! never stored here.
mod inventory;
pub(crate) mod read;
mod read_inventory;
pub(crate) mod reverse;
pub(crate) use read::load_on;
pub use read::{ensure_publications, load_lookup_records};
mod types;
pub use super::name::compose_lookup_names_at;
pub use super::records::{compose_lookup_inventories_at, compose_lookup_record_keys_at};
pub use inventory::LookupInventoryMetadata;
pub use types::*;
