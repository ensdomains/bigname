//! The friendly history event type vocabulary and its canonical `type` filter sets.

use serde::{Deserialize, Serialize};

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
    Subregistry,
    /// A confirmed ENSv1→ENSv2 migration of the name: the activated `MigrationApplied` of a
    /// completed migration correlation group, never a native ENSv2 registration.
    Migration,
    Reservation,
    Token,
    Contract,
}

impl HistoryEventType {
    pub(crate) const ALL: [Self; 15] = [
        Self::Registration,
        Self::Renewal,
        Self::Release,
        Self::Expiry,
        Self::Transfer,
        Self::Authority,
        Self::Resolver,
        Self::Record,
        Self::PrimaryName,
        Self::Permission,
        Self::Subregistry,
        Self::Migration,
        Self::Reservation,
        Self::Token,
        Self::Contract,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Registration => "registration",
            Self::Renewal => "renewal",
            Self::Release => "release",
            Self::Expiry => "expiry",
            Self::Transfer => "transfer",
            Self::Authority => "authority",
            Self::Resolver => "resolver",
            Self::Record => "record",
            Self::PrimaryName => "primary_name",
            Self::Permission => "permission",
            Self::Subregistry => "subregistry",
            Self::Migration => "migration",
            Self::Reservation => "reservation",
            Self::Token => "token",
            Self::Contract => "contract",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|event_type| event_type.as_str() == value)
    }

    pub(crate) const fn storage_event_kinds(self) -> &'static [&'static str] {
        match self {
            Self::Registration => &["RegistrationGranted", "LabelRegistered"],
            Self::Renewal => &["RegistrationRenewed"],
            Self::Release => &["RegistrationReleased"],
            Self::Expiry => &["ExpiryChanged"],
            Self::Transfer => &["TokenControlTransferred"],
            Self::Authority => &["AuthorityTransferred", "AuthorityEpochChanged"],
            Self::Resolver => &["ResolverChanged", "ResolverRecordLinked"],
            Self::Record => &["RecordChanged", "RecordVersionChanged"],
            Self::PrimaryName => &["ReverseChanged"],
            Self::Permission => &[
                "PermissionChanged",
                "RootPermissionChanged",
                "PermissionScopeChanged",
                "RolesChanged",
                "EACRolesChanged",
                "AccountPermissionChanged",
            ],
            Self::Subregistry => &["SubregistryChanged"],
            Self::Migration => &["MigrationApplied"],
            Self::Reservation => &["RegistrationReserved"],
            Self::Token => &["TokenRegenerated"],
            Self::Contract => &["RegistryCreated", "ParentChanged", "Upgraded"],
        }
    }
}

/// Non-empty set of product event types in canonical (`HistoryEventType::ALL`)
/// order with duplicates removed, so equal sets always share one wire value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoryEventTypeSet {
    event_types: Vec<HistoryEventType>,
}

impl HistoryEventTypeSet {
    pub(crate) fn from_event_types(
        event_types: impl IntoIterator<Item = HistoryEventType>,
    ) -> Option<Self> {
        let requested = event_types.into_iter().collect::<Vec<_>>();
        let normalized = HistoryEventType::ALL
            .iter()
            .copied()
            .filter(|candidate| requested.contains(candidate))
            .collect::<Vec<_>>();
        (!normalized.is_empty()).then_some(Self {
            event_types: normalized,
        })
    }

    #[cfg(test)]
    pub(crate) fn as_slice(&self) -> &[HistoryEventType] {
        &self.event_types
    }

    pub(crate) fn canonical_value(&self) -> String {
        self.event_types
            .iter()
            .map(|event_type| event_type.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn storage_event_kinds(&self) -> Vec<String> {
        self.event_types
            .iter()
            .flat_map(|event_type| event_type.storage_event_kinds())
            .map(|kind| (*kind).to_owned())
            .collect()
    }
}

impl From<HistoryEventType> for HistoryEventTypeSet {
    fn from(value: HistoryEventType) -> Self {
        Self {
            event_types: vec![value],
        }
    }
}
