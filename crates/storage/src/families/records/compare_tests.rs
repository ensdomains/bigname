use super::*;

#[test]
fn keyed_lists_name_the_element_and_field_that_differ() {
    let mut differences = Vec::new();
    diff(
        &mut differences,
        "",
        &json!({"entries": [{"record_key": "a", "value": 1}, {"record_key": "b", "value": 2}],
                "provenance": {"ids": [1, 2]}}),
        &json!({"entries": [{"record_key": "b", "value": 3}],
                "provenance": {"ids": [1, 2], "extra": null}}),
    );
    assert_eq!(
        differences,
        [
            Difference {
                field: "entries[a]".into(),
                today: Some(json!({"record_key": "a", "value": 1})),
                family: None,
            },
            Difference {
                field: "entries[b].value".into(),
                today: Some(json!(2)),
                family: Some(json!(3)),
            },
            Difference {
                field: "provenance.extra".into(),
                today: None,
                family: Some(Value::Null),
            },
        ]
    );
}

#[test]
fn a_stored_absent_marker_is_not_a_missing_field() {
    let mut differences = Vec::new();
    diff(
        &mut differences,
        "",
        &json!({"chain_positions": {"block_number": 1}}),
        &json!({"chain_positions": {"block_number": 1, "mutated": "<absent>"}}),
    );
    assert_eq!(
        differences,
        [Difference {
            field: "chain_positions.mutated".into(),
            today: None,
            family: Some(json!("<absent>")),
        }]
    );
}

fn address(name: &str, value: &str) -> AddressRecordCurrentEntry {
    AddressRecordCurrentEntry {
        address: "0x01".into(),
        logical_name_id: name.into(),
        namespace: "ens".into(),
        canonical_display_name: name.into(),
        normalized_name: name.into(),
        namehash: name.into(),
        surface_binding_id: None,
        resource_id: None,
        record_resource_id: uuid::Uuid::nil(),
        binding_kind: None,
        coin_type: "60".into(),
        record_key: "addr:60".into(),
        provenance: json!({"value": value}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn a_page_difference_shows_beside_an_entry_difference() {
    let differences = compare_address_results(
        &[address("a", "1")],
        &["cursor-a".into()],
        &[address("a", "2")],
        &["cursor-b".into()],
    );
    let fields: Vec<&str> = differences.iter().map(|d| d.field.as_str()).collect();
    let key = format!("a|{}|-", uuid::Uuid::nil());
    assert_eq!(
        fields,
        [format!("entries[{key}].provenance.value").as_str(), "pages"]
    );
}

#[test]
fn a_repeated_address_key_is_compared_per_occurrence() {
    let differences = compare_address_records(
        &[address("a", "1"), address("a", "2")],
        &[address("a", "1"), address("a", "3")],
    );
    let key = format!("a|{}|-", uuid::Uuid::nil());
    assert_eq!(
        differences,
        [Difference {
            field: format!("entries[{key}#2].provenance.value"),
            today: Some(json!("2")),
            family: Some(json!("3")),
        }]
    );
}

#[test]
fn a_repeated_address_key_differing_only_first_is_reported() {
    let key = format!("a|{}|-", uuid::Uuid::nil());
    let differences = compare_address_records(
        &[address("a", "1"), address("a", "2")],
        &[address("a", "3"), address("a", "2")],
    );
    assert_eq!(
        differences,
        [Difference {
            field: format!("entries[{key}].provenance.value"),
            today: Some(json!("1")),
            family: Some(json!("3")),
        }]
    );
}

#[test]
fn swapped_repeated_address_keys_differ_per_occurrence() {
    let key = format!("a|{}|-", uuid::Uuid::nil());
    let differences = compare_address_records(
        &[address("a", "1"), address("a", "2")],
        &[address("a", "2"), address("a", "1")],
    );
    let fields: Vec<String> = differences.into_iter().map(|d| d.field).collect();
    assert_eq!(
        fields,
        [
            format!("entries[{key}].provenance.value"),
            format!("entries[{key}#2].provenance.value"),
        ]
    );
}

#[test]
fn a_changed_binding_is_one_entry_removed_and_one_added() {
    let bound = |binding: u128| AddressRecordCurrentEntry {
        surface_binding_id: Some(uuid::Uuid::from_u128(binding)),
        ..address("a", "1")
    };
    let differences = compare_address_records(&[bound(1)], &[bound(2)]);
    let key = |binding: u128| {
        format!(
            "entries[a|{}|{}]",
            uuid::Uuid::nil(),
            uuid::Uuid::from_u128(binding)
        )
    };
    let sides: Vec<(String, bool, bool)> = differences
        .into_iter()
        .map(|d| (d.field, d.today.is_some(), d.family.is_some()))
        .collect();
    assert_eq!(sides, [(key(1), true, false), (key(2), false, true)]);
}

/// Robustness for supplied sequences: two bindings of one name in swapped order stay apart. The
/// normal Surface query lists one row per name, so this is not evidence that it returns both.
#[test]
fn entries_of_one_name_under_two_bindings_are_not_paired_by_position() {
    let bound = |binding: u128, value: &str| AddressRecordCurrentEntry {
        surface_binding_id: Some(uuid::Uuid::from_u128(binding)),
        ..address("a", value)
    };
    let differences = compare_address_records(
        &[bound(1, "1"), bound(2, "2")],
        &[bound(2, "2"), bound(1, "1")],
    );
    let fields: Vec<&str> = differences.iter().map(|d| d.field.as_str()).collect();
    assert_eq!(fields, ["entries.order"]);
}

fn pair(value: i64) -> CompatibilityPair {
    let position = |log: i64| FamilyPosition {
        block_number: 1,
        transaction_index: Some(0),
        log_index: Some(log),
        event_identity: format!("event-{log}"),
    };
    CompatibilityPair {
        record_key: "addr:60".into(),
        value_event_id: Some(value),
        value_position: position(value),
        sibling_event_id: Some(value + 1),
        sibling_position: position(value + 1),
    }
}

#[test]
fn a_repeated_pair_is_a_difference_on_either_side_and_on_both() {
    let duplicates = |expected: &[CompatibilityPair], family: &[CompatibilityPair]| {
        compare_pairs(expected, family)
            .into_iter()
            .find(|difference| difference.field == "compatibility_pairs.duplicates")
            .map(|difference| (difference.today, difference.family))
    };
    let (one, twice) = ([pair(1)], [pair(1), pair(1)]);
    assert_eq!(
        duplicates(&twice, &one),
        Some((Some(json!(["addr:60"])), Some(json!([]))))
    );
    assert_eq!(
        duplicates(&one, &twice),
        Some((Some(json!([])), Some(json!(["addr:60"]))))
    );
    assert_eq!(
        duplicates(&twice, &twice),
        Some((Some(json!(["addr:60"])), Some(json!(["addr:60"]))))
    );
    assert_eq!(duplicates(&one, &one), None);
}

#[test]
fn a_reordered_list_reports_the_order() {
    let mut differences = Vec::new();
    diff(
        &mut differences,
        "selectors",
        &json!([{"record_key": "a"}, {"record_key": "b"}]),
        &json!([{"record_key": "b"}, {"record_key": "a"}]),
    );
    assert_eq!(
        differences,
        [Difference {
            field: "selectors.order".into(),
            today: Some(json!(["a", "b"])),
            family: Some(json!(["b", "a"])),
        }]
    );
}
