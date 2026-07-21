use super::*;

#[test]
fn normalize_address_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
    assert_eq!(
        normalize_address("0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(normalize_address("NOT-A-HEX-ADDRESS"), "not-a-hex-address");
    assert_eq!(normalize_address("0xABC"), "0xabc");
    assert_eq!(
        normalize_address("00000000000000000000000000000000000000AA"),
        "00000000000000000000000000000000000000aa"
    );
}

#[test]
fn parse_primary_name_address_keeps_existing_validation_boundary() {
    let parsed =
        match parse_primary_name_address(" 0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E ") {
            Ok(parsed) => parsed,
            Err(_) => panic!("standard address should parse"),
        };
    assert_eq!(
        parsed,
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert!(parse_primary_name_address("0xABC").is_err());
    assert!(parse_primary_name_address("00000000000000000000000000000000000000AA").is_err());
}

#[test]
fn primary_name_coin_type_is_canonicalized_at_parse_boundary() {
    assert_eq!(
        must_parse(parse_primary_name_coin_type(Some("060"))),
        "60"
    );
}

#[test]
fn empty_optional_enum_values_use_route_defaults() {
    assert_eq!(must_parse(parse_history_scope(Some(""))), HistoryScope::Both);
    assert_eq!(
        must_parse(parse_resolution_mode(Some(""))),
        ResolutionMode::Declared
    );
    assert_eq!(
        must_parse(parse_response_view(Some(""), ResponseView::Full)),
        ResponseView::Full
    );
    assert_eq!(
        must_parse(parse_meta_mode(Some(""), MetaMode::Full)),
        MetaMode::Full
    );
    assert_eq!(
        must_parse(parse_address_names_dedupe_by(Some(""))),
        AddressNamesCurrentDedupe::Surface
    );
}

#[test]
fn exact_name_path_names_reject_non_normalized_or_unnormalizable_input() {
    assert!(parse_exact_name_path_name("ens", "Alice.eth").is_err());
    assert_eq!(
        must_parse(parse_exact_name_path_name("ens", "alice.eth")),
        "alice.eth"
    );
    assert!(parse_exact_name_path_name("ens", "bad name.eth").is_err());
}

#[test]
fn resolution_record_keys_reject_overflowing_addr_coin_type() {
    let error = parse_resolution_record_keys(
        Some("addr:18446744073709551616"),
        ResolutionMode::Verified,
    )
    .expect_err("overflowing coin_type selectors must be invalid input");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_input");
}

#[test]
fn resolution_record_keys_canonicalize_addr_coin_type_before_dedupe() {
    let error = parse_resolution_record_keys(Some("addr:060,addr:60"), ResolutionMode::Verified)
        .expect_err("canonical duplicate addr selectors must be rejected");

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_input");
    assert_eq!(error.message, "records must not contain duplicate selectors");
}

#[test]
fn verified_resolution_record_keys_cap_at_two_hundred() {
    let at_limit = (0..MAX_VERIFIED_RECORD_KEYS)
        .map(|index| format!("text:key{index}"))
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        must_parse(parse_resolution_record_keys(
            Some(&at_limit),
            ResolutionMode::Verified,
        ))
        .len(),
        MAX_VERIFIED_RECORD_KEYS
    );

    let over_limit = format!("{at_limit},text:overflow");
    let error = parse_resolution_record_keys(Some(&over_limit), ResolutionMode::Verified)
        .expect_err("verified selector lists over 200 must be rejected");
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "invalid_input");
    assert_eq!(error.message, "records must contain at most 200 selectors");
}

fn must_parse<T>(result: ApiResult<T>) -> T {
    match result {
        Ok(value) => value,
        Err(_) => panic!("parser should accept empty optional enum value"),
    }
}
