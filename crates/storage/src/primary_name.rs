mod full_rebuild;
mod hooks;
mod lock;
mod reads;
mod rows;
mod types;
mod upserts;
mod validation;

pub use full_rebuild::{
    publish_primary_names_current_full_rebuild,
    publish_primary_names_current_full_rebuild_in_transaction,
};
pub use hooks::verified_primary_name_claim_hooks;
pub use lock::{
    lock_primary_name_tuple_in_transaction, lock_primary_names_current_replacement_in_transaction,
};
pub use reads::{
    clear_primary_names_current, delete_primary_name_current,
    delete_primary_name_current_in_transaction, load_primary_name_current,
    load_primary_name_current_snapshot,
    load_primary_name_current_snapshot_for_update_in_transaction,
};
pub use types::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, VerifiedPrimaryNameClaimHooks,
    VerifiedPrimaryNameInvalidationHook, VerifiedPrimaryNameLookupHook,
};
pub use upserts::{
    upsert_primary_name_current_rows, upsert_primary_name_current_snapshots,
    upsert_primary_name_current_snapshots_in_transaction,
};

#[cfg(test)]
mod tests;
