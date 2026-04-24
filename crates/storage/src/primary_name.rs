mod hooks;
mod reads;
mod rows;
mod types;
mod upserts;
mod validation;

pub use hooks::verified_primary_name_claim_hooks;
pub use reads::{
    clear_primary_names_current, delete_primary_name_current, load_primary_name_current,
    load_primary_name_current_snapshot,
};
pub use types::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, VerifiedPrimaryNameClaimHooks,
    VerifiedPrimaryNameInvalidationHook, VerifiedPrimaryNameLookupHook,
};
pub use upserts::{upsert_primary_name_current_rows, upsert_primary_name_current_snapshots};

#[cfg(test)]
mod tests;
