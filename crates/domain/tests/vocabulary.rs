use std::str::FromStr;

use bigname_domain::vocabulary::{
    ChainId, EvmAddress, Namespace, SourceFamily,
    canonicalize_prefixed_evm_address_or_ascii_lowercase, parse_alloy_evm_address,
};

#[test]
fn closed_vocabulary_round_trips_wire_values() {
    assert_eq!(ChainId::from_str("base-mainnet"), Ok(ChainId::BaseMainnet));
    assert_eq!(
        ChainId::from_str("base-e2e-composed-reorg"),
        Ok(ChainId::BaseE2eComposedReorg)
    );
    assert_eq!(
        ChainId::from_str("ethereum-e2e-rpc"),
        Ok(ChainId::EthereumE2eRpc)
    );
    assert_eq!(
        ChainId::from_str("ethereum-e2e-reorg"),
        Ok(ChainId::EthereumE2eReorg)
    );
    assert_eq!(
        ChainId::from_str("ethereum-e2e-composed-reorg"),
        Ok(ChainId::EthereumE2eComposedReorg)
    );
    assert_eq!(
        ChainId::from_str("project-fixture"),
        Ok(ChainId::ProjectFixture)
    );
    assert_eq!(Namespace::from_str("ens"), Ok(Namespace::Ens));
    assert_eq!(
        SourceFamily::from_str("basenames_execution"),
        Ok(SourceFamily::BasenamesExecution)
    );
    assert!(ChainId::from_str("base-future").is_err());
    assert!(Namespace::from_str("unknown").is_err());
    assert!(SourceFamily::from_str("unknown_family").is_err());
}

#[test]
fn address_policy_names_make_strict_and_legacy_behavior_explicit() {
    let checksummed = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    assert_eq!(
        EvmAddress::from_str(checksummed)
            .expect("strict address must parse")
            .to_string(),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(
        parse_alloy_evm_address("00000000000C2E074eC69A0dFb2997BA6C7d2E1E")
            .expect("manifest-compatible Alloy grammar must parse")
            .to_string(),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert!(
        "00000000000C2E074eC69A0dFb2997BA6C7d2E1E"
            .parse::<EvmAddress>()
            .is_err()
    );
    assert!(parse_alloy_evm_address("0xABC").is_err());
    assert_eq!(
        canonicalize_prefixed_evm_address_or_ascii_lowercase("NOT-A-HEX-ADDRESS"),
        "not-a-hex-address"
    );
}
