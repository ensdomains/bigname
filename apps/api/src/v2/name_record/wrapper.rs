use super::super::{
    V2Error, V2Result,
    vocab::{WrapperFuses, WrapperState},
};
pub(crate) use bigname_storage::public_name_fields::{
    served_manager, wrapper_expiry, wrapper_lifecycle_matches_fuses,
};
use serde_json::Value;

pub(crate) fn wrapper_metadata(summary: &Value) -> V2Result<Option<(WrapperState, WrapperFuses)>> {
    bigname_storage::public_name_fields::wrapper_metadata(summary)
        .map_err(|error| V2Error::internal_error(error.to_string()))
}
