use super::*;
use phase_runner::config::{ChainConfig, SeedBasis, SourceConfig};

#[test]
fn runtime_manifest_startup_admits_hackathon_floor_and_preserves_source_identity() {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia-hackathon");
    let source = SourceConfig::new(
        "ethereum-sepolia",
        "sepolia-staging-reth",
        "drpc",
        SeedBasis::EthereumHead,
        11_626_442,
        "https://rpc.invalid",
    )
    .unwrap();
    let mut chains = vec![ChainConfig::new("ethereum-sepolia", vec![source], false).unwrap()];
    let (_, profile) = prepare_runtime_manifests(&root, &mut chains).unwrap();
    assert_eq!(profile, "sepolia-hackathon");
    let source = &chains[0].sources[0];
    assert_eq!(source.source_key, "sepolia-staging-reth");
    assert_eq!(source.start_block_number, 11_626_442);
    assert_eq!(source.endpoint(), "https://rpc.invalid");
    assert_eq!(chains[0].intake_sources()[0].source_key, source.source_key);
    assert_eq!(
        chains[0].intake_sources()[0].start_block_number,
        source.start_block_number
    );
}

#[test]
fn runtime_manifest_startup_rejects_nonzero_floor_for_other_approved_profile() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia");
    let source = SourceConfig::new(
        "ethereum-sepolia",
        "sepolia-staging-reth",
        "drpc",
        SeedBasis::EthereumHead,
        11_626_442,
        "https://rpc.invalid",
    )
    .unwrap();
    let mut chains = vec![ChainConfig::new("ethereum-sepolia", vec![source], false).unwrap()];
    let error = prepare_runtime_manifests(&root, &mut chains).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("binary-approved sepolia-hackathon corpus")
    );
    assert_eq!(chains[0].sources[0].start_block_number, 11_626_442);
}
