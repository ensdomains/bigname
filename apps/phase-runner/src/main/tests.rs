use phase_runner::{
    config::{ChainConfig, SeedBasis, SourceConfig},
    error::RunnerError,
};

use super::*;

#[test]
fn init_schema_cli_is_available() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "init-schema",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
    ])
    .expect("init-schema command must parse")
    .resolve()
    .expect("init-schema command must resolve");
    assert!(matches!(command, ResolvedCommand::InitSchema { .. }));
}

#[test]
fn rewind_cli_requires_an_exact_ancestor() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "rewind",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "base-mainnet",
        "--ancestor-block",
        "42",
        "--ancestor-hash",
        "0x42",
    ])
    .expect("rewind command must parse")
    .resolve()
    .expect("rewind command must resolve");

    match command {
        ResolvedCommand::Rewind {
            chain_id, ancestor, ..
        } => {
            assert_eq!(chain_id, "base-mainnet");
            assert_eq!(ancestor.number, 42);
            assert_eq!(ancestor.hash, "0x42");
        }
        _ => panic!("expected rewind command"),
    }
}

#[test]
fn terminal_chain_report_makes_run_command_fail() {
    let report = SupervisorReport {
        stopped_chains: vec![(
            "broken-chain".to_owned(),
            RunnerError::data_integrity("bad lineage"),
        )],
    };

    let error = require_clean_supervisor_exit(report)
        .expect_err("a terminal chain failure must produce a nonzero main result");
    assert!(error.to_string().contains("broken-chain"));
    assert!(error.to_string().contains("DataIntegrity"));
}

#[test]
fn checked_in_manifest_profile_is_bound_to_the_binary() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let (repository, profile) =
        load_hashed_manifest_repository(&root).expect("mainnet manifest profile must be covered");

    assert_eq!(profile, "mainnet");
    assert!(!repository.manifests().is_empty());
}

#[test]
fn checked_in_profiles_identify_two_configured_ens_chains() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    load_hashed_manifest_repository(&root).expect("mainnet manifest profile must be covered");
    let chains = [
        configured_chain("ethereum-mainnet"),
        configured_chain("ethereum-sepolia"),
    ];

    validate_deployment_table_set(&chains, COMPILED_CHAIN_NAMESPACES.iter().copied())
        .expect_err("the checked-in profiles must identify both configured ENS chains");
}

#[test]
fn partial_runtime_manifest_tree_is_rejected_by_the_hash_gate() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet/base");
    let error = load_hashed_manifest_repository(&root)
        .expect_err("an arbitrary runtime manifest subset must be rejected");

    assert!(error.to_string().contains("not covered"));
    assert!(error.to_string().contains("interpreter content hash"));
}

fn configured_chain(chain_id: &str) -> ChainConfig {
    ChainConfig::new(
        chain_id,
        vec![
            SourceConfig::new(
                chain_id,
                "rpc",
                "rpc",
                SeedBasis::BaseSeam,
                0,
                "http://rpc.invalid",
            )
            .unwrap(),
        ],
        false,
    )
    .unwrap()
}
