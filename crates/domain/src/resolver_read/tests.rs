use serde_json::json;

use super::*;

fn projected() -> Value {
    json!({"status": "projected", "exhaustiveness": "not_asserted"})
}

fn rule() -> Value {
    json!({"read_rules": [{
        "kind": "ensip19_default_address",
        "source_record_key": ENSIP19_DEFAULT_RECORD_KEY
    }]})
}

#[test]
fn exact_nonempty_absence_marker_only_blocks_matching_derivation() {
    use IndexedRecordStatus::{NotFound, Success, Unsupported};
    for (exact, marker, expected, derived) in [
        (None, json!([]), Success, true),
        (Some("not_found"), json!([]), Success, true),
        (Some("not_found"), json!(["addr:60"]), NotFound, false),
        (None, json!(["addr:60"]), Success, true),
        (Some("success"), json!(["addr:60"]), Success, false),
        (Some("not_found"), json!("addr:60"), Success, true),
    ] {
        let mut entries = json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,
            "record_family":"addr", "selector_key":"2147483648", "status":"success",
            "value":"0x1111111111111111111111111111111111111111"}]);
        if let Some(status) = exact {
            entries
                .as_array_mut()
                .unwrap()
                .push(json!({"record_key":"addr:60",
                "record_family":"addr", "selector_key":"60", "status":status,
                "value":"0x2222222222222222222222222222222222222222"}));
        }
        let mut provenance = rule();
        provenance["exact_nonempty_not_found_record_keys"] = marker;
        let answer = evaluate_indexed_record(
            &entries,
            &provenance,
            &projected(),
            "addr:60",
            "addr",
            Some("60"),
        );
        assert_eq!(answer.status, expected, "{entries}; {provenance}");
        assert_eq!(answer.derivation.is_some(), derived);
        let incomplete = evaluate_indexed_record(
            &entries,
            &provenance,
            &json!({"status":"unsupported"}),
            "addr:60",
            "addr",
            Some("60"),
        );
        // Unsupported coverage refuses every read, an exact success entry included; a row
        // naming no reason reports the generic non-authoritative reason.
        assert_eq!(incomplete.status, Unsupported, "{entries}; {provenance}");
        assert_eq!(
            incomplete.unsupported_reason.as_deref(),
            Some(INDEXED_INVENTORY_NOT_AUTHORITATIVE_REASON)
        );
        assert_eq!(incomplete.value, None);
    }
}

#[test]
fn unsupported_coverage_refuses_retained_values_with_the_row_reason() {
    let entries = json!([
        {"record_key":"addr:60","record_family":"addr","selector_key":"60","status":"success",
         "value":"0xFA75ED860000000000000000000000000000ABCD"},
        {"record_key":"text:url","record_family":"text","selector_key":"url","status":"success",
         "value":{"value":"https://value.example"}},
        {"record_key":"contenthash","record_family":"contenthash","selector_key":null,
         "status":"success","value":{"encoding":"hex","bytes":"0xe3010170"}}
    ]);
    let unsupported = json!({
        "status":"unsupported",
        "exhaustiveness":"not_asserted",
        "unsupported_reason":"resolver_implementation_unknown"
    });
    for (record_key, family, selector) in [
        ("addr:60", "addr", Some("60")),
        ("text:url", "text", Some("url")),
        ("contenthash", "contenthash", None),
        ("text:missing", "text", Some("missing")),
    ] {
        let answer = evaluate_indexed_record(
            &entries,
            &rule(),
            &unsupported,
            record_key,
            family,
            selector,
        );
        assert_eq!(
            answer.status,
            IndexedRecordStatus::Unsupported,
            "{record_key}"
        );
        assert_eq!(answer.value, None, "{record_key}");
        assert_eq!(
            answer.unsupported_reason.as_deref(),
            Some("resolver_implementation_unknown"),
            "{record_key}"
        );
        assert_eq!(answer.derivation, None, "{record_key}");
    }

    // The same entries answer once the row is supported, so the refusal is the coverage's.
    let served = evaluate_indexed_record(
        &entries,
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(served.status, IndexedRecordStatus::Success);
    assert_eq!(
        served.value,
        Some(json!("0xfa75ed860000000000000000000000000000abcd"))
    );

    // A null or blank reason falls back to the generic non-authoritative reason.
    for coverage in [
        json!({"status":"unsupported","unsupported_reason":null}),
        json!({"status":"unsupported","unsupported_reason":""}),
        json!({"status":"partial"}),
    ] {
        let answer =
            evaluate_indexed_record(&entries, &rule(), &coverage, "addr:60", "addr", Some("60"));
        assert_eq!(
            answer.status,
            IndexedRecordStatus::Unsupported,
            "{coverage}"
        );
        assert_eq!(
            answer.unsupported_reason.as_deref(),
            Some(INDEXED_INVENTORY_NOT_AUTHORITATIVE_REASON),
            "{coverage}"
        );
    }
}

#[test]
fn ensip19_xor_boundaries_match_chain_from_coin_type() {
    for (coin_type, expected_chain, eligible) in [
        (59, 0, false),
        (60, 1, true),
        (2_147_483_648, 0, false),
        (2_147_483_649, 1, true),
        (4_294_967_295, 2_147_483_647, true),
        (4_294_967_296, 0, false),
        (u64::MAX, 0, false),
    ] {
        assert_eq!(ensip19_chain_from_coin_type(coin_type), expected_chain);
        assert_eq!(ensip19_default_fallback_target(coin_type), eligible);
    }
}

#[test]
fn exact_success_wins_over_default() {
    let answer = evaluate_indexed_record(
        &json!([
            {"record_key":"addr:2147483649","record_family":"addr","selector_key":"2147483649","status":"success","value":"0xEXACT"},
            {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
        ]),
        &rule(),
        &projected(),
        "addr:2147483649",
        "addr",
        Some("2147483649"),
    );
    assert_eq!(answer.status, IndexedRecordStatus::Success);
    assert_eq!(answer.value, Some(json!("0xexact")));
    assert_eq!(answer.derivation, None);
}

#[test]
fn missing_or_not_found_exact_uses_default_with_metadata() {
    for entries in [
        json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}]),
        json!([
            {"record_key":"addr:2147483649","record_family":"addr","selector_key":"2147483649","status":"not_found"},
            {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
        ]),
    ] {
        let answer = evaluate_indexed_record(
            &entries,
            &rule(),
            &projected(),
            "addr:2147483649",
            "addr",
            Some("2147483649"),
        );
        assert_eq!(answer.status, IndexedRecordStatus::Success);
        assert_eq!(answer.value, Some(json!("0xdefault")));
        assert_eq!(
            answer.derivation,
            Some(IndexedRecordDerivation {
                rule: ResolverReadFeature::Ensip19DefaultAddress,
                source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
            })
        );
    }
}

#[test]
fn derived_zero20_matches_the_requested_verified_decode() {
    let zero20 = json!([{
        "record_key":ENSIP19_DEFAULT_RECORD_KEY,
        "record_family":"addr",
        "selector_key":"2147483648",
        "status":"success",
        "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000000"}
    }]);
    let expected_derivation = Some(IndexedRecordDerivation {
        rule: ResolverReadFeature::Ensip19DefaultAddress,
        source_record_key: ENSIP19_DEFAULT_RECORD_KEY.to_owned(),
    });

    let legacy = evaluate_indexed_record(
        &zero20,
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(legacy.status, IndexedRecordStatus::NotFound);
    assert_eq!(legacy.value, None);
    assert_eq!(legacy.derivation, expected_derivation);

    let multicoin = evaluate_indexed_record(
        &zero20,
        &rule(),
        &projected(),
        "addr:2147483649",
        "addr",
        Some("2147483649"),
    );
    assert_eq!(multicoin.status, IndexedRecordStatus::Success);
    assert_eq!(
        multicoin.value,
        Some(json!("0x0000000000000000000000000000000000000000"))
    );
    assert_eq!(multicoin.derivation, expected_derivation);

    let nonzero = evaluate_indexed_record(
        &json!([{
            "record_key":ENSIP19_DEFAULT_RECORD_KEY,
            "record_family":"addr",
            "selector_key":"2147483648",
            "status":"success",
            "value":{"encoding":"hex","bytes":"0x0000000000000000000000000000000000000def"}
        }]),
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(nonzero.status, IndexedRecordStatus::Success);
    assert_eq!(
        nonzero.value,
        Some(json!("0x0000000000000000000000000000000000000def"))
    );
    assert_eq!(nonzero.derivation, expected_derivation);
}

#[test]
fn authoritative_default_absence_is_a_derived_miss() {
    let answer = evaluate_indexed_record(
        &json!([]),
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(answer.status, IndexedRecordStatus::NotFound);
    assert!(answer.derivation.is_some());
}

#[test]
fn incomplete_or_unsupported_default_source_is_nonterminal() {
    let incomplete = evaluate_indexed_record(
        &json!([]),
        &rule(),
        &json!({"status":"unsupported","unsupported_reason":"coverage_incomplete"}),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(incomplete.status, IndexedRecordStatus::Unsupported);

    let unsupported = evaluate_indexed_record(
        &json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"unsupported","unsupported_reason":"value_not_retained"}]),
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(unsupported.status, IndexedRecordStatus::Unsupported);
}

#[test]
fn exact_unsupported_does_not_assume_empty_storage() {
    let answer = evaluate_indexed_record(
        &json!([
            {"record_key":"addr:60","record_family":"addr","selector_key":"60","status":"unsupported","unsupported_reason":"value_not_retained"},
            {"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}
        ]),
        &rule(),
        &projected(),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(answer.status, IndexedRecordStatus::Unsupported);
    assert_eq!(answer.derivation, None);
}

#[test]
fn default_key_ineligible_and_non_addr_requests_never_derive() {
    for (record_key, family, selector, expected) in [
        (
            ENSIP19_DEFAULT_RECORD_KEY,
            "addr",
            Some("2147483648"),
            IndexedRecordStatus::Success,
        ),
        ("addr:59", "addr", Some("59"), IndexedRecordStatus::NotFound),
        (
            "text:url",
            "text",
            Some("url"),
            IndexedRecordStatus::NotFound,
        ),
    ] {
        let answer = evaluate_indexed_record(
            &json!([{"record_key":ENSIP19_DEFAULT_RECORD_KEY,"record_family":"addr","selector_key":"2147483648","status":"success","value":"0xDEFAULT"}]),
            &rule(),
            &projected(),
            record_key,
            family,
            selector,
        );
        assert_eq!(answer.status, expected);
        assert_eq!(answer.derivation, None);
    }
}
