use serde::{Deserialize, Serialize};
use sqlx::types::time::OffsetDateTime;

/// An expiry with registration context: a finite exact second, or explicit null with a reason.
/// The enclosing optional field still omits expiry when there is no registration context.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum ExpiryTimestamp {
    Seconds(String),
    NoExpiry,
}

pub(super) fn clock_input(value: &str) -> Option<OffsetDateTime> {
    value
        .parse::<bigname_storage::UnixSeconds>()
        .ok()?
        .to_datetime()
}
