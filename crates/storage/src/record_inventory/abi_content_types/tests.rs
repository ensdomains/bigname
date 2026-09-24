use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_json::{Value, json};

use super::{
    AbiContentTypes, AbiContentTypesInput, AbiContentTypesUnavailable as Unavailable, EventScope,
    Evidence, Plan, admits_abi_observations, content_types_from_evidence, plan,
    single_bit_content_type,
};

fn scope() -> EventScope {
    EventScope {
        chain_id: "ethereum-mainnet".to_owned(),
        max_block_number: Some(100),
    }
}

fn evidence(items: &[(i64, Option<&str>)]) -> BTreeMap<(i64, EventScope), Evidence> {
    items
        .iter()
        .map(|(id, selector)| {
            let evidence = match selector {
                Some(selector) => Evidence::Abi(json!(selector)),
                None => Evidence::Other,
            };
            ((*id, scope()), evidence)
        })
        .collect()
}

fn observed(ids: &[i64], links: &[i64], items: &[(i64, Option<&str>)]) -> AbiContentTypes {
    content_types_from_evidence(
        ids,
        &links.iter().copied().collect::<BTreeSet<_>>(),
        &scope(),
        &evidence(items),
    )
}

#[test]
fn single_bit_content_types_decode_from_the_selector_key_only() {
    let wide = format!("{}", alloy_primitives::U256::from(1_u8) << 200);
    // 2^255, the highest single-bit uint256.
    let top_bit = "57896044618658097711785492504343953926634992332820282019728792003956564819968";
    assert_eq!(
        single_bit_content_type(&json!(top_bit)),
        Some(alloy_primitives::U256::from(1_u8) << 255)
    );
    for accepted in [
        "1",
        "2",
        "8",
        "9223372036854775808",
        "18446744073709551616",
        &wide,
        top_bit,
    ] {
        assert_eq!(
            single_bit_content_type(&json!(accepted)).map(|value| value.to_string()),
            Some(accepted.to_owned()),
            "{accepted}"
        );
    }
    let overflow = format!("1{}", "0".repeat(80));
    // 2^256, one past the uint256 range.
    let past_top = "115792089237316195423570985008687907853269984665640564039457584007913129639936";
    for rejected in [
        json!(past_top),
        json!("0"),
        json!("3"),
        json!("6"),
        json!("01"),
        json!("+1"),
        json!(" 1"),
        json!(""),
        json!("0x1"),
        json!(1),
        Value::Null,
        json!(overflow),
    ] {
        assert_eq!(single_bit_content_type(&rejected), None, "{rejected}");
    }
}

#[test]
fn content_types_are_deduplicated_and_numerically_ordered() {
    let answer = observed(
        &[1, 2, 3, 4, 5],
        &[],
        &[
            (1, Some("16")),
            (2, Some("2")),
            (3, None),
            (4, Some("16")),
            (5, Some("18446744073709551616")),
        ],
    );
    assert_eq!(
        answer,
        AbiContentTypes::Observed(vec![
            "2".to_owned(),
            "16".to_owned(),
            "18446744073709551616".to_owned()
        ])
    );
}

#[test]
fn an_eligible_path_without_abi_writes_is_an_empty_list() {
    assert_eq!(
        observed(&[1, 2], &[2], &[(1, None), (2, None)]),
        AbiContentTypes::Observed(Vec::new())
    );
    assert_eq!(
        observed(&[], &[], &[]),
        AbiContentTypes::Observed(Vec::new())
    );
}

#[test]
fn missing_selected_evidence_withholds_the_list() {
    // Event 2 is referenced but not retained as canonical, activated evidence.
    assert_eq!(
        observed(&[1, 2], &[], &[(1, Some("1"))]),
        AbiContentTypes::Unavailable(Unavailable::ObservationsStale)
    );
    // A retracted link event is selected evidence too.
    assert_eq!(
        observed(&[1, 9], &[9], &[(1, Some("1"))]),
        AbiContentTypes::Unavailable(Unavailable::ObservationsStale)
    );
}

#[test]
fn a_non_single_bit_write_is_flagged_not_expanded() {
    for selector in ["3", "0"] {
        assert_eq!(
            observed(&[1, 2], &[], &[(1, Some("1")), (2, Some(selector))]),
            AbiContentTypes::Unavailable(Unavailable::ContentTypeNotSingleBit),
            "{selector}"
        );
    }
}

#[test]
fn link_events_never_contribute_content_types() {
    assert_eq!(
        observed(&[1, 2], &[2], &[(1, Some("4")), (2, Some("8"))]),
        AbiContentTypes::Observed(vec!["4".to_owned()])
    );
}

#[test]
fn observation_paths_follow_the_selected_storage_classification() {
    assert!(admits_abi_observations(
        "ens_v1_resolver_l1",
        Some("public_resolver")
    ));
    assert!(admits_abi_observations("ens_v1_resolver_l1", None));
    assert!(admits_abi_observations(
        "ens_v2_resolver_l1",
        Some("permissioned_resolver")
    ));
    assert!(admits_abi_observations("ens_v2_resolver_l1", None));
    assert!(!admits_abi_observations(
        "ens_v2_resolver_l1",
        Some("public_resolver_v2")
    ));
    assert!(!admits_abi_observations(
        "ens_v2_resolver_l1",
        Some("ensv1_mirror_resolver")
    ));
    assert!(!admits_abi_observations("basenames_base_resolver", None));
    assert!(!admits_abi_observations("ens_v1_registry_l1", None));
}

fn planned(authoritative: bool, provenance: Value) -> Plan {
    plan(&AbiContentTypesInput {
        authoritative,
        resource_id: uuid::Uuid::nil(),
        record_version_boundary_key: "",
        provenance: &provenance,
        chain_positions: &json!({"target_block_number": 100}),
        last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
    })
}

#[test]
fn rows_without_a_usable_basis_are_answered_before_any_read() {
    let complete = json!({
        "chain_id": "ethereum-mainnet",
        "resolver_address": "0xABC",
        "record_event_ids": [1, 2],
        "record_link_event_ids": [2]
    });
    assert!(matches!(
        planned(false, complete.clone()),
        Plan::Done(Unavailable::InventoryNotAuthoritative)
    ));
    match planned(true, complete) {
        Plan::Classify {
            resolver_address,
            event_ids,
            link_event_ids,
            max_block_number,
            ..
        } => {
            assert_eq!(resolver_address.as_deref(), Some("0xabc"));
            assert_eq!(event_ids, vec![1, 2]);
            assert_eq!(link_event_ids, BTreeSet::from([2]));
            assert_eq!(max_block_number, Some(100));
        }
        Plan::Done(reason) => panic!("unexpected {reason:?}"),
    }
    assert!(matches!(
        planned(true, json!({"record_event_ids": []})),
        Plan::Done(Unavailable::ObservationsNotSupported)
    ));
    for (record_event_ids, record_link_event_ids) in [
        (Value::Null, json!([])),
        (json!(["1"]), json!([])),
        (json!(1), json!([])),
        (json!([1]), json!("not a list")),
    ] {
        let mut provenance = json!({
            "chain_id": "ethereum-mainnet",
            "resolver_address": "0xabc",
            "record_link_event_ids": record_link_event_ids
        });
        if !record_event_ids.is_null() {
            provenance["record_event_ids"] = record_event_ids;
        }
        assert!(
            matches!(
                planned(true, provenance.clone()),
                Plan::Done(Unavailable::ObservationsStale)
            ),
            "{provenance}"
        );
    }
}

#[test]
fn unavailable_reasons_are_product_vocabulary() {
    let reasons = [
        Unavailable::InventoryNotAvailable,
        Unavailable::InventoryNotAuthoritative,
        Unavailable::ObservationsNotSupported,
        Unavailable::ObservationsStale,
        Unavailable::ContentTypeNotSingleBit,
    ]
    .map(Unavailable::as_str);
    assert_eq!(
        reasons,
        [
            "inventory_not_available",
            "inventory_not_authoritative",
            "abi_observations_not_supported",
            "abi_observations_stale",
            "abi_content_type_not_single_bit",
        ]
    );
    let routes = include_str!("../../../../../docs/api-v1-routes.md");
    for reason in reasons {
        assert!(
            routes.contains(&format!("`{reason}`")),
            "{reason} is undocumented"
        );
    }
}

/// The classification table must equal what the checked-in manifests admit, per source family
/// and emitter role, so a new role or a changed ABI declaration fails the build instead of being
/// silently treated as eligible.
///
/// For each manifest and each role it can select (no role, every `[[contracts]].role`, every
/// `resolver_implementations` role, and every role named in `emitter_roles`), a role admits an ABI
/// observation when some `ABIChanged` or `ABIUpdated` entry that is not `unsupported` is admitted
/// for it (empty `emitter_roles`, or `emitter_roles` naming it) and is keyed like the role's own
/// record events. A role with role-scoped `RecordChanged` events decodes records by their first
/// parameter (PublicResolverV2's `bytes32 node`), so a role-independent record-ID event does not
/// describe its storage. The ENSv1 mirror role stores no records of its own and admits nothing.
#[test]
fn observation_paths_agree_with_the_checked_in_manifests() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests");
    let mut loaded_manifests = Vec::new();
    for profile in std::fs::read_dir(&root).expect("manifest profiles") {
        let profile = profile.expect("manifest profile").path();
        if profile.is_dir() {
            let repository =
                bigname_manifests::load_repository(&profile).expect("manifest repository");
            loaded_manifests.extend(repository.manifests().iter().cloned());
        }
    }
    assert!(!loaded_manifests.is_empty(), "no manifests loaded");

    let first_input = |event: &bigname_manifests::ManifestAbiEvent| {
        event
            .parsed_event()
            .expect("manifest event fragment")
            .inputs
            .first()
            .map(|input| input.ty.clone())
    };
    let mut derived = BTreeMap::<(String, Option<String>), (bool, String)>::new();
    for loaded in &loaded_manifests {
        let manifest = &loaded.manifest;
        let events = &manifest.abi.events;
        let mut roles = BTreeSet::<Option<String>>::from([None]);
        roles.extend(manifest.contracts.iter().map(|c| Some(c.role.clone())));
        roles.extend(
            manifest
                .resolver_implementations
                .iter()
                .map(|implementation| Some(implementation.role.clone())),
        );
        roles.extend(
            events
                .iter()
                .flat_map(|event| event.emitter_roles.iter().cloned().map(Some)),
        );
        for role in roles {
            let admitted_for = |event: &bigname_manifests::ManifestAbiEvent| {
                event.emitter_roles.is_empty()
                    || role
                        .as_deref()
                        .is_some_and(|role| event.emitter_roles.iter().any(|r| r == role))
            };
            let role_record_keys = events
                .iter()
                .filter(|event| {
                    role.as_deref()
                        .is_some_and(|role| event.emitter_roles.iter().any(|r| r == role))
                        && event.normalized_events.iter().any(|n| n == "RecordChanged")
                })
                .filter_map(first_input)
                .collect::<BTreeSet<_>>();
            let admits = role.as_deref() != Some(bigname_manifests::ENSV1_MIRROR_RESOLVER_ROLE)
                && events.iter().any(|event| {
                    matches!(event.name.as_str(), "ABIChanged" | "ABIUpdated")
                        && event.status
                            != Some(bigname_manifests::CapabilitySupportStatus::Unsupported)
                        && admitted_for(event)
                        && (role_record_keys.is_empty()
                            || first_input(event)
                                .is_some_and(|key| role_record_keys.contains(&key)))
                });
            let key = (manifest.source_family.clone(), role);
            let source = loaded.path.display().to_string();
            if let Some((previous, previous_source)) = derived.get(&key) {
                assert_eq!(
                    *previous, admits,
                    "{key:?}: {previous_source} and {source} disagree on ABI admission"
                );
            } else {
                derived.insert(key, (admits, source));
            }
        }
    }

    for ((family, role), (admits, source)) in &derived {
        assert_eq!(
            admits_abi_observations(family, role.as_deref()),
            *admits,
            "admits_abi_observations({family}, {role:?}) disagrees with {source}"
        );
    }
    // The derivation must reach the cases the table distinguishes, or the comparison is vacuous.
    for (family, role, expected) in [
        ("ens_v1_resolver_l1", None, true),
        ("ens_v1_resolver_l1", Some("public_resolver"), true),
        ("ens_v2_resolver_l1", None, true),
        ("ens_v2_resolver_l1", Some("permissioned_resolver"), true),
        ("ens_v2_resolver_l1", Some("public_resolver_v2"), false),
        ("ens_v2_resolver_l1", Some("ensv1_mirror_resolver"), false),
        ("basenames_base_resolver", None, false),
        ("basenames_base_resolver", Some("resolver"), false),
    ] {
        let key = (family.to_owned(), role.map(str::to_owned));
        assert_eq!(
            derived.get(&key).map(|(admits, _)| *admits),
            Some(expected),
            "{key:?}"
        );
    }
}
