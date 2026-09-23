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
    for accepted in [
        "1",
        "2",
        "8",
        "9223372036854775808",
        "18446744073709551616",
        &wide,
    ] {
        assert_eq!(
            single_bit_content_type(&json!(accepted)).map(|value| value.to_string()),
            Some(accepted.to_owned()),
            "{accepted}"
        );
    }
    let overflow = format!("1{}", "0".repeat(80));
    for rejected in [
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
        provenance: &provenance,
        chain_positions: &json!({"target_block_number": 100}),
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
    let routes = include_str!("../../../../../docs/api-v2-routes.md");
    for reason in reasons {
        assert!(
            routes.contains(&format!("`{reason}`")),
            "{reason} is undocumented"
        );
    }
}

/// The classification rule mirrors manifest admission: every ENSv1 resolver-family manifest
/// declares `ABIChanged`, every ENSv2 resolver-family manifest declares `ABIUpdated`, and no
/// Basenames resolver manifest declares either.
#[test]
fn observation_paths_agree_with_the_checked_in_manifests() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests");
    let mut seen = BTreeMap::<String, usize>::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("manifest directory") {
            let path = entry.expect("manifest entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
                continue;
            }
            let family = path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_owned();
            let text = std::fs::read_to_string(&path).expect("manifest text");
            let declares = |event: &str| text.contains(&format!("event {event}("));
            match family.as_str() {
                "ens_v1_resolver_l1" => assert!(declares("ABIChanged"), "{}", path.display()),
                "ens_v2_resolver_l1" => assert!(declares("ABIUpdated"), "{}", path.display()),
                "basenames_base_resolver" => assert!(
                    !declares("ABIChanged") && !declares("ABIUpdated"),
                    "{} now declares an ABI event; revisit admits_abi_observations",
                    path.display()
                ),
                _ => continue,
            }
            *seen.entry(family).or_default() += 1;
        }
    }
    for family in [
        "ens_v1_resolver_l1",
        "ens_v2_resolver_l1",
        "basenames_base_resolver",
    ] {
        assert!(
            seen.get(family).copied().unwrap_or_default() > 0,
            "{family}"
        );
    }
}
