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

/// How a name-list `q` matches the stored normalized name: `match=prefix` (the default) or
/// `match=contains`, the same vocabulary `GET /v1/search` accepts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum NameMatch {
    #[default]
    Prefix,
    Contains,
}

impl NameMatch {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Prefix => "prefix",
            Self::Contains => "contains",
        }
    }

    /// `prefix` or `contains`; an absent or blank value is `prefix`.
    pub(crate) fn parse(value: Option<&str>) -> V2Result<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            None | Some("prefix") => Ok(Self::Prefix),
            Some("contains") => Ok(Self::Contains),
            Some(_) => Err(V2Error::invalid_input("match is invalid")),
        }
    }

    /// Normalizes `q` as this mode's fragment: an ENSIP-15 name prefix, or for `contains` a
    /// name substring that may also keep one leading label boundary.
    pub(crate) fn normalize(self, q: &str) -> V2Result<String> {
        match self {
            Self::Prefix => normalize_name_prefix(q),
            Self::Contains => normalize_name_contains(q),
        }
    }

    pub(crate) const fn to_storage(self, text: &str) -> bigname_storage::NameQuery<'_> {
        match self {
            Self::Prefix => bigname_storage::NameQuery::prefix(text),
            Self::Contains => bigname_storage::NameQuery::contains(text),
        }
    }
}
