use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    Ok,
    NotFound,
    InvalidName,
    Mismatch,
    Unsupported,
    Stale,
    Failed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Completeness {
    Full,
    Partial,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Source {
    Indexed,
    Verified,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Finality {
    Latest,
    Safe,
    Finalized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryScope {
    Name,
    Registration,
    Both,
}

impl HistoryScope {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryEventType {
    Registration,
    Renewal,
    Release,
    Expiry,
    Transfer,
    Authority,
    Resolver,
    Record,
    PrimaryName,
    Permission,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegistrationStatus {
    Active,
    Wrapped,
    Registered,
    Released,
    Unregistered,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Relation {
    Owner,
    Manager,
    Registrant,
}

impl Relation {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Manager => "manager",
            Self::Registrant => "registrant",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesDedupe {
    Name,
    Registration,
}

impl AddressNamesDedupe {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Registration => "registration",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AddressNamesSort {
    Name,
    ExpiresAt,
    RegisteredAt,
}

impl AddressNamesSort {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::ExpiresAt => "expires_at",
            Self::RegisteredAt => "registered_at",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Resolver {
    pub(crate) chain_id: u64,
    pub(crate) address: String,
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::*;

    fn assert_wire<T: Serialize>(value: T, expected: &str) {
        let serialized = serde_json::to_value(value).expect("value must serialize");
        assert_eq!(serialized, serde_json::Value::String(expected.to_owned()));
    }

    #[test]
    fn status_variants_use_exact_wire_spelling() {
        assert_wire(Status::Ok, "ok");
        assert_wire(Status::NotFound, "not_found");
        assert_wire(Status::InvalidName, "invalid_name");
        assert_wire(Status::Mismatch, "mismatch");
        assert_wire(Status::Unsupported, "unsupported");
        assert_wire(Status::Stale, "stale");
        assert_wire(Status::Failed, "failed");
    }

    #[test]
    fn completeness_variants_use_exact_wire_spelling() {
        assert_wire(Completeness::Full, "full");
        assert_wire(Completeness::Partial, "partial");
        assert_wire(Completeness::Unsupported, "unsupported");
    }

    #[test]
    fn source_variants_use_exact_wire_spelling() {
        assert_wire(Source::Indexed, "indexed");
        assert_wire(Source::Verified, "verified");
    }

    #[test]
    fn finality_variants_use_exact_wire_spelling() {
        assert_wire(Finality::Latest, "latest");
        assert_wire(Finality::Safe, "safe");
        assert_wire(Finality::Finalized, "finalized");
    }

    #[test]
    fn history_scope_variants_use_exact_wire_spelling() {
        assert_wire(HistoryScope::Name, "name");
        assert_wire(HistoryScope::Registration, "registration");
        assert_wire(HistoryScope::Both, "both");
    }

    #[test]
    fn history_event_type_variants_use_exact_wire_spelling() {
        assert_wire(HistoryEventType::Registration, "registration");
        assert_wire(HistoryEventType::Renewal, "renewal");
        assert_wire(HistoryEventType::Release, "release");
        assert_wire(HistoryEventType::Expiry, "expiry");
        assert_wire(HistoryEventType::Transfer, "transfer");
        assert_wire(HistoryEventType::Authority, "authority");
        assert_wire(HistoryEventType::Resolver, "resolver");
        assert_wire(HistoryEventType::Record, "record");
        assert_wire(HistoryEventType::PrimaryName, "primary_name");
        assert_wire(HistoryEventType::Permission, "permission");
    }

    #[test]
    fn registration_status_variants_use_exact_wire_spelling() {
        assert_wire(RegistrationStatus::Active, "active");
        assert_wire(RegistrationStatus::Wrapped, "wrapped");
        assert_wire(RegistrationStatus::Registered, "registered");
        assert_wire(RegistrationStatus::Released, "released");
        assert_wire(RegistrationStatus::Unregistered, "unregistered");
    }

    #[test]
    fn relation_variants_use_exact_wire_spelling() {
        assert_wire(Relation::Owner, "owner");
        assert_wire(Relation::Manager, "manager");
        assert_wire(Relation::Registrant, "registrant");
    }

    #[test]
    fn address_names_dedupe_variants_use_exact_wire_spelling() {
        assert_wire(AddressNamesDedupe::Name, "name");
        assert_wire(AddressNamesDedupe::Registration, "registration");
    }

    #[test]
    fn address_names_sort_variants_use_exact_wire_spelling() {
        assert_wire(AddressNamesSort::Name, "name");
        assert_wire(AddressNamesSort::ExpiresAt, "expires_at");
        assert_wire(AddressNamesSort::RegisteredAt, "registered_at");
    }
}
