use super::{V2Error, V2Result};

pub(super) fn normalize_name_prefix(prefix: &str) -> V2Result<String> {
    crate::name_filter::normalize_name_prefix(prefix).map_err(|error| {
        V2Error::invalid_input(format!(
            "q must be a valid ENSIP-15 name prefix: {}",
            error.message()
        ))
    })
}

pub(super) fn normalize_name_contains(fragment: &str) -> V2Result<String> {
    crate::name_filter::normalize_name_contains(fragment).map_err(|error| {
        V2Error::invalid_input(format!(
            "q must be a valid ENSIP-15 name substring: {}",
            error.message()
        ))
    })
}
