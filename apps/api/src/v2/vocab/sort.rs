use serde::{Deserialize, Serialize};

/// The collection `sort` values. `created_at`, the name's first observation, is an
/// address-name sort only; the subnames route refuses it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesSort {
    Name,
    ExpiresAt,
    RegisteredAt,
    CreatedAt,
}

impl AddressNamesSort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiresAt => "expires_at",
            Self::RegisteredAt => "registered_at",
            Self::CreatedAt => "created_at",
        }
    }
}
