use super::*;

#[test]
fn evm_addresses_are_normalized_at_the_parse_boundary() {
    let parsed = match parse_evm_address(" 0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E ", "address")
    {
        Ok(parsed) => parsed,
        Err(_) => panic!("standard address should parse"),
    };
    assert_eq!(parsed, "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e");
    assert!(parse_evm_address("0xABC", "address").is_err());
    assert!(parse_evm_address("00000000000000000000000000000000000000AA", "address").is_err());
}

#[test]
fn primary_name_coin_type_is_canonicalized_at_parse_boundary() {
    let parsed = match parse_primary_name_coin_type(Some("060")) {
        Ok(parsed) => parsed,
        Err(_) => panic!("coin type should parse"),
    };
    assert_eq!(parsed, "60");
}
