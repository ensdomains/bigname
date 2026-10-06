//! Closed product actions classify retained rows, never infer transaction intent.
use super::{HistoryEventType, HistoryRowContext};
use bigname_storage::HistoryEvent;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryAction {
    RegistrationGranted,
    RegistrationRenewed,
    RegistrationReleased,
    ExpiryChanged,
    TokenTransferred,
    AuthorityChanged,
    ResolverChanged,
    RecordChanged,
    PrimaryNameRecorded,
    PermissionChanged,
    SubregistryChanged,
    MigrationApplied,
    NameWrapped,
    NameUnwrapped,
    OperatorApprovalChanged,
    RegistrationReserved,
    ReservationBecameReachable,
    ResolverRecordLinked,
    TokenRegenerated,
    RegistryHandoff,
    ReverseClaimed,
    RecordVersionChanged,
    RegistryCreated,
    RegistryParentChanged,
    ContractUpgraded,
}

impl HistoryAction {
    pub(super) fn for_row(
        row: &HistoryEvent,
        category: HistoryEventType,
        context: &HistoryRowContext,
    ) -> Self {
        if context.registry_handoff(row).is_some() {
            return Self::RegistryHandoff;
        }
        match (
            row.event_kind.as_str(),
            row.after_state["source_event"].as_str(),
        ) {
            ("AuthorityEpochChanged", Some("NameWrapped")) => return Self::NameWrapped,
            ("AuthorityEpochChanged", Some("NameUnwrapped")) => return Self::NameUnwrapped,
            ("AccountPermissionChanged", _) => return Self::OperatorApprovalChanged,
            ("RegistrationReserved", _)
                if row
                    .event_identity
                    .contains(":RegistrationReserved:topology:") =>
            {
                return Self::ReservationBecameReachable;
            }
            ("ResolverRecordLinked", _) => return Self::ResolverRecordLinked,
            ("ReverseChanged", Some("ReverseClaimed")) => return Self::ReverseClaimed,
            ("RecordVersionChanged", _) => return Self::RecordVersionChanged,
            ("ParentChanged", _) => return Self::RegistryParentChanged,
            ("Upgraded", _) => return Self::ContractUpgraded,
            _ => {}
        }
        match category {
            HistoryEventType::Registration => Self::RegistrationGranted,
            HistoryEventType::Renewal => Self::RegistrationRenewed,
            HistoryEventType::Release => Self::RegistrationReleased,
            HistoryEventType::Expiry => Self::ExpiryChanged,
            HistoryEventType::Transfer => Self::TokenTransferred,
            HistoryEventType::Authority => Self::AuthorityChanged,
            HistoryEventType::Resolver => Self::ResolverChanged,
            HistoryEventType::Record => Self::RecordChanged,
            HistoryEventType::PrimaryName => Self::PrimaryNameRecorded,
            HistoryEventType::Permission => Self::PermissionChanged,
            HistoryEventType::Subregistry => Self::SubregistryChanged,
            HistoryEventType::Migration => Self::MigrationApplied,
            HistoryEventType::Reservation => Self::RegistrationReserved,
            HistoryEventType::Token => Self::TokenRegenerated,
            HistoryEventType::Contract => Self::RegistryCreated,
        }
    }
}
