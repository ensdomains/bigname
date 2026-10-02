mod relation_tests;

use serde::Serialize;

use super::*;

#[test]
fn authority_context_serializes_as_the_documented_wire_values() {
    assert_wire(AuthorityContext::CurrentForName, "current_for_name");
    assert_wire(AuthorityContext::ResourceAudit, "resource_audit");
}

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
fn ops_status_variants_use_exact_wire_spelling() {
    assert_wire(OpsStatus::Ready, "ready");
    assert_wire(OpsStatus::Degraded, "degraded");
    assert_wire(OpsStatus::Stale, "stale");
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
    assert_wire(HistoryEventType::Subregistry, "subregistry");
}

#[test]
fn history_event_type_storage_kinds_round_trip_to_product_types() {
    for event_type in [
        HistoryEventType::Registration,
        HistoryEventType::Renewal,
        HistoryEventType::Release,
        HistoryEventType::Expiry,
        HistoryEventType::Transfer,
        HistoryEventType::Authority,
        HistoryEventType::Resolver,
        HistoryEventType::Record,
        HistoryEventType::PrimaryName,
        HistoryEventType::Permission,
        HistoryEventType::Subregistry,
    ] {
        for storage_kind in event_type.storage_event_kinds() {
            assert_eq!(
                crate::v2::history_event_type(storage_kind),
                Some(event_type)
            );
        }
    }
}

#[test]
fn history_event_type_sets_canonicalize_order_and_duplicates() {
    let set = HistoryEventTypeSet::from_event_types([
        HistoryEventType::Renewal,
        HistoryEventType::Registration,
        HistoryEventType::Renewal,
    ])
    .expect("non-empty set must build");
    assert_eq!(
        set.as_slice(),
        &[HistoryEventType::Registration, HistoryEventType::Renewal]
    );
    assert_eq!(set.canonical_value(), "registration,renewal");
    assert_eq!(
        set.storage_event_kinds(),
        vec![
            "RegistrationGranted".to_owned(),
            "LabelRegistered".to_owned(),
            "RegistrationRenewed".to_owned(),
        ]
    );
    assert!(HistoryEventTypeSet::from_event_types([]).is_none());
    assert_eq!(
        HistoryEventType::from_wire("primary_name"),
        Some(HistoryEventType::PrimaryName)
    );
    assert_eq!(HistoryEventType::from_wire("registered"), None);
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
fn wrapper_state_variants_use_exact_wire_spelling() {
    assert_wire(WrapperState::Wrapped, "wrapped");
    assert_wire(WrapperState::Emancipated, "emancipated");
    assert_wire(WrapperState::Locked, "locked");
}

#[test]
fn relation_variants_use_exact_wire_spelling() {
    assert_wire(Relation::Owner, "owner");
    assert_wire(Relation::Manager, "manager");
    assert_wire(Relation::RoleHolder, "role_holder");
    assert_wire(Relation::ResolvesTo, "resolves_to");
    assert_wire(Relation::FormerOwner, "former_owner");
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
    assert_wire(AddressNamesSort::CreatedAt, "created_at");
}

#[test]
fn boundary_vocabulary_matching_uses_underscore_boundaries_and_plural_suffixes() {
    const TERMS: &[&str] = &["coverage", "raw_fact", "normalized_events"];

    assert_eq!(
        matched_boundary_vocabulary_terms("insufficient_coverage", TERMS),
        vec!["coverage"]
    );
    assert!(contains_boundary_vocabulary("coverage_gap", TERMS));
    assert!(contains_boundary_vocabulary("coverages", TERMS));
    assert!(contains_boundary_vocabulary("raw facts", TERMS));
    assert!(contains_boundary_vocabulary("normalized_event", TERMS));
    assert!(contains_boundary_vocabulary(
        "identity_sidecar_missing",
        PRODUCT_PIPELINE_TERMS
    ));
    assert!(!contains_boundary_vocabulary("discoverage", TERMS));
    assert!(!contains_boundary_vocabulary("rawfactory", TERMS));
}
